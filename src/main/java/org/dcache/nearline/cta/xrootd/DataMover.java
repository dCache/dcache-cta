package org.dcache.nearline.cta.xrootd;

import static org.dcache.xrootd.protocol.XrootdProtocol.DATA_SERVER;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import java.net.InetSocketAddress;
import org.dcache.xrootd.core.XrootdDecoder;
import org.dcache.xrootd.core.XrootdEncoder;
import org.dcache.xrootd.core.XrootdHandshakeHandler;
import org.dcache.xrootd.stream.ChunkedResponseWriteHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataMover extends AbstractIdleService {

    private static final Logger logger = LoggerFactory.getLogger(DataMover.class);

    private ServerBootstrap server;
    private ChannelFuture cf;

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;

    public DataMover(InetSocketAddress sa) {

        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();

        server = new ServerBootstrap()
              .group(bossGroup, workerGroup)
              .channel(NioServerSocketChannel.class)
              .localAddress(sa)
              .childOption(ChannelOption.TCP_NODELAY, true)
              .childOption(ChannelOption.SO_KEEPALIVE, true)
              .childHandler(new XrootChallenInitializer());
    }

    @Override
    protected void startUp() throws Exception {
        cf = server.bind().sync();
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

    public InetSocketAddress getLocalSocketAddress() {
        Preconditions.checkState(cf != null, "Service is not started");
        Preconditions.checkState(cf.isDone(), "Service is not bound yet.");
        return (InetSocketAddress) cf.channel().localAddress();
    }

    private static class XrootChallenInitializer extends ChannelInitializer<Channel> {

        @Override
        protected void initChannel(Channel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast("handshaker", new XrootdHandshakeHandler(DATA_SERVER));
            pipeline.addLast("encoder", new XrootdEncoder());
            pipeline.addLast("decoder", new XrootdDecoder());
            if (logger.isDebugEnabled()) {
                pipeline.addLast("logger", new LoggingHandler(XrootChallenInitializer.class));
            }

            pipeline.addLast("chunk-writer", new ChunkedResponseWriteHandler());
            pipeline.addLast("data-server", new DataServerHandler());
        }

    }
}
