package org.dcache.nearline.cta;

import static org.dcache.nearline.cta.CtaNearlineStorage.CTA_ENDPOINT;
import static org.dcache.nearline.cta.CtaNearlineStorage.CTA_GROUP;
import static org.dcache.nearline.cta.CtaNearlineStorage.CTA_INSTANCE;
import static org.dcache.nearline.cta.CtaNearlineStorage.CTA_REQUEST_TIMEOUT;
import static org.dcache.nearline.cta.CtaNearlineStorage.CTA_TLS;
import static org.dcache.nearline.cta.CtaNearlineStorage.CTA_USER;
import static org.dcache.nearline.cta.CtaNearlineStorage.IO_PORT;
import ch.cern.cta.rpc.CtaRpcGrpc;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import diskCacheV111.util.Adler32;
import io.grpc.InsecureChannelCredentials;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class CtaCli {

    private static final Logger LOGGER = LoggerFactory.getLogger(CtaCli.class);

    private static void usage() {
        System.err.println("Usage: ctacli <host> <port> <cmd> <args>...");
        System.err.println("\n");
        System.err.println("  archive <path> <storage class>");
        System.exit(2);
    }


    private static void runPing(String host, int port) {

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
                var res = cta.version(Empty.newBuilder().build());
                System.out.println("Remote CTA version: " + res.getCtaVersion());
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


    private static class ArchiveCmd {

        private final CtaNearlineStorage driver = new CtaNearlineStorage("aType", "aName");
        private final CountDownLatch waitToCpmplete = new CountDownLatch(1);

        ArchiveCmd(String host, int port, String user, String group, String instance) {

            var drvConfig = Map.of(CTA_USER,
                    user, CTA_GROUP,
                    group, CTA_INSTANCE,
                    instance, CTA_ENDPOINT,
                    host + ":" + port,
                    IO_PORT, "0",
                    CTA_TLS, "false",
                    CTA_REQUEST_TIMEOUT, "3");

            driver.configure(drvConfig);
            driver.start();
        }

        public void run(String file, String storageClass) throws IOException, InterruptedException {

            try {
                var request = mockedFlushRequest(file, storageClass);
                driver.flush(Set.of(request));

                waitToCpmplete.await();

            } finally {
                driver.shutdown();
            }
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
                    .checksum(calculateChecksum(path.toFile())).build();

            var request = new FlushRequest() {
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

            return request;
        }


        private static Checksum calculateChecksum(File file) throws IOException {

            ByteBuffer bb = ByteBuffer.allocate(8192);
            var adler = new Adler32();
            try (FileChannel fc = FileChannel.open(file.toPath())) {
                while (true) {
                    bb.clear();
                    int n = fc.read(bb);
                    if (n < 0) {
                        break;
                    }
                    bb.flip();
                    adler.update(bb);
                }
                return new Checksum(ChecksumType.ADLER32, adler.digest());
            }
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException {

        if (args.length < 3) {
            usage();
        }

        var cmd = args[2];
        switch (cmd) {
            case "ping":
                runPing(args[0], Integer.parseInt(args[1]));
                break;
            case "archive":
                if (args.length != 8) {
                    usage();
                }
                new ArchiveCmd(args[0], Integer.parseInt(args[1]), args[3], args[4], args[5])
                        .run(args[6], args[7]);
                break;
            default:
                usage();
        }

        System.exit(0);
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

        public static byte[] hexStringToByteArray(String id) {

            if (id.length() % 2 != 0) {
                throw new IllegalArgumentException("The string needs to be even-length: " + id);
            }

            int len = id.length() / 2;
            byte[] bytes = new byte[len];

            for (int i = 0; i < len; i++) {
                final int charIndex = i * 2;
                final int d0 = toDigit(id.charAt(charIndex));
                final int d1 = toDigit(id.charAt(charIndex + 1));
                bytes[i] = (byte) ((d0 << 4) + d1);
            }
            return bytes;
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
