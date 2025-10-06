package org.dcache.nearline.cta;

import static org.dcache.nearline.cta.Utils.calculateAdler32Checksum;
import static org.dcache.nearline.cta.CtaNearlineStorage.CTA_ENDPOINT;
import static org.dcache.nearline.cta.CtaNearlineStorage.CTA_GROUP;
import static org.dcache.nearline.cta.CtaNearlineStorage.CTA_INSTANCE;
import static org.dcache.nearline.cta.CtaNearlineStorage.CTA_REQUEST_TIMEOUT;
import static org.dcache.nearline.cta.CtaNearlineStorage.CTA_TLS;
import static org.dcache.nearline.cta.CtaNearlineStorage.CTA_USER;
import static org.dcache.nearline.cta.CtaNearlineStorage.IO_ENDPOINT;
import static org.dcache.nearline.cta.CtaNearlineStorage.IO_PORT;
import ch.cern.cta.rpc.CtaRpcGrpc;
import ch.cern.cta.rpc.Request;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import cta.admin.CtaAdmin;
import io.grpc.InsecureChannelCredentials;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import picocli.CommandLine;

@CommandLine.Command(name = "cta-cli", mixinStandardHelpOptions = true, version = "0.0.1",
        description = "Command line utility for CTA-gRPC interface",
        subcommands = {CtaCli.Ping.class, CtaCli.ArchiveCmd.class}

)
public class CtaCli implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(CtaCli.class);

    @CommandLine.Command(name = "ping")
    static class Ping implements Runnable {

        @CommandLine.Parameters(index = "0", description = "cta frontend host")
        String host;

        @CommandLine.Parameters(index = "1", description = "cta frontend port")
        int port;

        @Override
        public void run() {

            var credentials = InsecureChannelCredentials.create();
            var channel = NettyChannelBuilder.forAddress(host, port,
                            credentials)
                    .disableServiceConfigLookUp() // no lookup in DNS for service record
                    .channelType(NioSocketChannel.class) // use Nio event loop instead of epoll
                    .eventLoopGroup(new NioEventLoopGroup(1))
                    .directExecutor() // use netty threads
                    .build();

            try {
                var cta = CtaRpcGrpc.newBlockingStub(channel);
                try {
                    var versionRequest = Request.newBuilder()
                            .setAdmincmd(CtaAdmin.AdminCmd.newBuilder()
                                    .setClientVersion(CtaNearlineStorageProvider.VERSION)
                                    .build()
                            ).build();
                    var res = cta.admin(versionRequest);
                    System.out.println("Remote CTA version: " + res.getMessageTxt() + " " + res.getXattr());
                    System.exit(0);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.flush();

                    System.err.println("Ping failed: " + e);
                    System.exit(1);
                }

            } finally {
                channel.shutdown();
            }

        }
    }


    @CommandLine.Command(name = "archive")
    static class ArchiveCmd implements Callable<Integer> {

        private final CtaNearlineStorage driver = new CtaNearlineStorage("aType", "aName");
        private final CountDownLatch waitToCpmplete = new CountDownLatch(1);

        @CommandLine.Option(names = {"-l", "--local-endpoint"}, paramLabel = "local endpoint", description = "Local endpoint to bind")
        String localEndpoint;

        @CommandLine.Parameters(index = "0", description = "cta frontend host")
        String host;

        @CommandLine.Parameters(index = "1", description = "cta frontend port")
        int port;

        @CommandLine.Parameters(index = "2", description = "user to issue cta request")
        String user;

        @CommandLine.Parameters(index = "3", description = "group to issue cta request")
        String group;

        @CommandLine.Parameters(index = "4", description = "disk instance name")
        String instance;

        @CommandLine.Parameters(index = "5", description = "file's storage class")
        String storageClass;

        @CommandLine.Parameters(index = "6", description = "file's path")
        String file;

        @Override
        public Integer call() throws IOException, InterruptedException {

            if (localEndpoint == null || localEndpoint.isEmpty()) {
                localEndpoint = InetAddress.getLocalHost().getCanonicalHostName();
            }

            var drvConfig = Map.of(CTA_USER,
                    user, CTA_GROUP,
                    group, CTA_INSTANCE,
                    instance, CTA_ENDPOINT,
                    host + ":" + port,
                    IO_PORT, "0",
                    CTA_TLS, "false",
                    IO_ENDPOINT, localEndpoint,
                    CTA_REQUEST_TIMEOUT, "3");

            driver.configure(drvConfig);
            driver.start();

            AtomicBoolean success = new AtomicBoolean();

            try {
                var request = mockedFlushRequest(file, storageClass);
                driver.flush(Set.of(new ForwardingFlushRequest() {
                    @Override
                    protected FlushRequest delegate() {
                        return request;
                    }

                    @Override
                    public void completed(Set<URI> uris) {
                        success.set(true);
                        super.completed(uris);
                    }
                }));
                waitToCpmplete.await();
            } finally {
                driver.shutdown();
            }

            if (!success.get()) {
                return 3;
            }
            return 0;
        }

        private FlushRequest mockedFlushRequest(String s, String storageClass) throws IOException {


            var path = Path.of(s);
            var id = UUID.randomUUID();
            var posixAttr = Files.getFileAttributeView(path, PosixFileAttributeView.class).readAttributes();


            var attrs = FileAttributes.of()
                    .size(posixAttr.size())
                    .storageClass(storageClass)
                    .hsm("cta").creationTime(posixAttr.creationTime().toMillis())
                    .pnfsId(InodeId.toPnfsid(id)) // just reuse
                    .checksum(calculateAdler32Checksum(path.toFile())).build();

            return new FlushRequest() {
                @Override
                public File getFile() {
                    return path.toFile();
                }

                @Override
                public URI getReplicaUri() {
                    return path.toUri();
                }

                @Override
                public FileAttributes getFileAttributes() {
                    return attrs;
                }

                @Override
                public long getReplicaCreationTime() {
                    return posixAttr.creationTime().toMillis();
                }

                @Override
                public ListenableFuture<String> activateWithPath() {
                    return Futures.immediateFuture("s");
                }

                @Override
                public UUID getId() {
                    return id;
                }

                @Override
                public long getDeadline() {
                    return 10_000;
                }

                @Override
                public ListenableFuture<Void> activate() {
                    return Futures.immediateFuture(null);
                }

                @Override
                public void failed(Exception e) {
                    LOGGER.error("Failed to submit archive request: {}", e.getMessage());
                    waitToCpmplete.countDown();
                }

                @Override
                public void failed(int i, String s) {
                    LOGGER.error("Failed to submit archive request: {} ({})", s, i);
                    waitToCpmplete.countDown();
                }

                @Override
                public void completed(Set<URI> uris) {
                    LOGGER.info("Complete: {}", uris);
                    waitToCpmplete.countDown();
                }
            };
        }
    }


    @Override
    public void run() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        var cmd = new CommandLine(new CtaCli());
        cmd.setExecutionStrategy(new CommandLine.RunAll()); // default is RunLast
        int exitCode = cmd.execute(args);

        if (args.length == 0) {
            cmd.usage(System.out);
        }

        System.exit(exitCode);
    }

    public static class InodeId {

        /**
         * no instance allowed
         */
        private InodeId() { /**/ }

        public static String toPnfsid(UUID uuid) {


            String idString = digits((long) 0 >> 32, 4) + digits(uuid.getMostSignificantBits() >> 32, 8) + digits(uuid.getMostSignificantBits() >> 16, 4) + digits(uuid.getMostSignificantBits(), 4) + digits(uuid.getLeastSignificantBits() >> 48, 4) + digits(uuid.getLeastSignificantBits(), 12);

            return idString.toUpperCase();
        }

        /**
         * Returns val represented by the specified number of hex digits.
         */
        private static String digits(long val, int digits) {
            long hi = 1L << (digits * 4);
            return Long.toHexString(hi | (val & (hi - 1))).substring(1);
        }

        private static int toDigit(char ch) throws NumberFormatException {
            if (ch >= '0' && ch <= '9') {
                return ch - '0';
            }
            if (ch >= 'A' && ch <= 'F') {
                return ch - 'A' + 10;
            }
            if (ch >= 'a' && ch <= 'f') {
                return ch - 'a' + 10;
            }
            throw new NumberFormatException("illegal character '" + ch + '\'');
        }
    }

}
