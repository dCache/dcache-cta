package org.dcache.nearline.cta.xrootd;

import static org.dcache.xrootd.protocol.XrootdProtocol.DATA_SERVER;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import cta.eos.CtaEos.Transport;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import org.dcache.nearline.cta.CtaTransportProvider;
import org.dcache.nearline.cta.PendingRequest;
import org.dcache.xrootd.core.XrootdAuthenticationHandlerProvider;
import org.dcache.xrootd.core.XrootdAuthorizationHandlerProvider;
import org.dcache.xrootd.core.XrootdDecoder;
import org.dcache.xrootd.core.XrootdEncoder;
import org.dcache.xrootd.core.XrootdHandshakeHandler;
import org.dcache.xrootd.plugins.ChannelHandlerFactory;
import org.dcache.xrootd.plugins.ChannelHandlerProvider;
import org.dcache.xrootd.stream.ChunkedResponseWriteHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataMover extends AbstractIdleService implements CtaTransportProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataMover.class);

    private final ServerBootstrap server;
    private final CompletableFuture<Void> cf = new CompletableFuture<>();

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;

    private final ConcurrentMap<String, PendingRequest> pendingRequests;

    /**
     * Driver configured hsm name.
     */
    private final String hsmName;

    /**
     * Driver configured hsm type;
     */
    private final String hsmType;

    private volatile String url;

    public DataMover(String type, String name, InetSocketAddress sa,
          ConcurrentMap<String, PendingRequest> pendingRequests) {

        hsmType = type;
        hsmName = name;

        try {
            this.pendingRequests = pendingRequests;
            bossGroup = new NioEventLoopGroup(0, new ThreadFactoryBuilder().setNameFormat("cta-datamover-accept-%d").build());
            workerGroup = new NioEventLoopGroup(0, new ThreadFactoryBuilder().setNameFormat("cta-datamover-worker-%d").build());

            server = new ServerBootstrap()
                  .group(bossGroup, workerGroup)
                  .channel(NioServerSocketChannel.class)
                  .localAddress(sa)
                  .childOption(ChannelOption.TCP_NODELAY, true)
                  .childOption(ChannelOption.SO_KEEPALIVE, true)
                  .childHandler(new XrootChannelInitializer());
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void startUp() throws Exception {

        server.bind()
              .addListener(new ChannelFutureListener() {
                  @Override
                  public void operationComplete(ChannelFuture channelFuture) throws Exception {
                      if (!channelFuture.isSuccess()) {
                          LOGGER.error("Failed to start DataMover: {}", channelFuture.cause().getMessage());
                          cf.completeExceptionally(channelFuture.cause());
                      } else {
                          InetSocketAddress sa = (InetSocketAddress) channelFuture.channel()
                                .localAddress();
                          url = InetAddresses.toUriString(sa.getAddress()) + ":" + sa.getPort();
                          LOGGER.info("Xroot IO mover started on: {}", url);
                          cf.complete(null);
                      }
                  }
              });
        cf.get();
    }

    @Override
    protected void shutDown() throws Exception {

        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    @Override
    protected String serviceName() {
        return "Xrootd CTA data mover";
    }

    private class XrootChannelInitializer extends ChannelInitializer<Channel> {

        public final List<ChannelHandlerFactory> channelHandlerFactories;
        private final ServiceLoader<ChannelHandlerProvider> channelHandlerProviders;

        XrootChannelInitializer() throws Exception {

            var pluginLoader = this.getClass().getClassLoader();
            XrootdAuthenticationHandlerProvider.setPluginClassLoader(pluginLoader);
            XrootdAuthorizationHandlerProvider.setPluginClassLoader(pluginLoader);

            channelHandlerProviders =
                  ServiceLoader.load(ChannelHandlerProvider.class, pluginLoader);

            channelHandlerFactories = new ArrayList<>();
            for (String plugin : List.of("authn:none")) {
                channelHandlerFactories.add(createHandlerFactory(plugin));
            }
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast("handshaker", new XrootdHandshakeHandler(DATA_SERVER));
            pipeline.addLast("encoder", new XrootdEncoder());
            pipeline.addLast("decoder", new XrootdDecoder());

            if (LOGGER.isDebugEnabled()) {
                pipeline.addLast("logger", new LoggingHandler(XrootChannelInitializer.class));
            }

            for (ChannelHandlerFactory factory : channelHandlerFactories) {
                pipeline.addLast("plugin:" + factory.getName(), factory.createHandler());
            }

            pipeline.addLast("chunk-writer", new ChunkedResponseWriteHandler());
            pipeline.addLast("data-server",
                  new DataServerHandler(hsmType, hsmName, pendingRequests));
        }

        public final ChannelHandlerFactory createHandlerFactory(String plugin)
              throws Exception {
            for (ChannelHandlerProvider provider : channelHandlerProviders) {
                ChannelHandlerFactory factory =
                      provider.createFactory(plugin, new Properties());
                if (factory != null) {
                    LOGGER.debug("ChannelHandler plugin {} is provided by {}", plugin,
                          provider.getClass());
                    return factory;
                } else {
                    LOGGER.debug("ChannelHandler plugin {} could not be provided by {}", plugin,
                          provider.getClass());
                }
            }
            throw new NoSuchElementException("Channel handler plugin not found: " + plugin);
        }
    }

    @Override
    public Transport getTransport(String id, long ctaArchiveId) {

        Preconditions.checkState(cf != null, "Service is not started");
        Preconditions.checkState(cf.isDone(), "Service is not bound yet.");
        Preconditions.checkState(url != null, "service url is null");

        // REVISIT:
        String reporterUrl = "eosQuery://" + url + "/success/" + id + "?archiveid=" + ctaArchiveId;
        String errorReporter = "eosQuery://" + url + "/error/" + id + "?error=";

        return Transport.newBuilder()
              .setDstUrl("root://" + url + "/" + id)
              .setErrorReportUrl(errorReporter)
              .setReportUrl(reporterUrl)
              .build();
    }
}
