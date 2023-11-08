package org.dcache.nearline.cta;

import ch.cern.cta.rpc.CtaRpcGrpc;
import com.google.protobuf.Empty;
import io.grpc.InsecureChannelCredentials;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel;

public class CtaCli {

    private static void usage() {
        System.err.println("Usage: ctacli <host> <port> <cmd> <args>...");
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
                System.out.flush();;

                System.err.println("Ping failed: " + e);
                System.exit(1);
            }

        } finally {
            channel.shutdown();
        }

    }

    public static void main(String[] args) {

        if (args.length < 3) {
            usage();
        }

        var cmd = args[2];
        switch (cmd) {
            case "ping":
                runPing(args[0], Integer.parseInt(args[1]));
                break;
            default:
                usage();
        }

    }
}
