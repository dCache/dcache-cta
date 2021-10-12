package org.dcache.nearline.cta;

import com.google.protobuf.Empty;
import cta.admin.CtaAdmin.Version;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.ThreadLocalRandom;
import org.dcache.cta.rpc.ArchiveRequest;
import org.dcache.cta.rpc.ArchiveResponse;
import org.dcache.cta.rpc.CtaRpcGrpc;
import org.dcache.cta.rpc.DeleteRequest;
import org.dcache.cta.rpc.RetrieveRequest;
import org.dcache.cta.rpc.RetrieveResponse;

public class DummyCta {

    private final Server server;

    public DummyCta() {
        server = NettyServerBuilder.forPort(0).addService(new CtaSvc()).build();

    }

    public void start() throws IOException {
        server.start();
    }

    public void shutdown() {
        server.shutdown();
    }

    public String getConnectString() {
        return "localhost:" + server.getPort();
    }

    private static class CtaSvc extends CtaRpcGrpc.CtaRpcImplBase {

        @Override
        public void version(Empty request, StreamObserver<Version> responseObserver) {
            super.version(request, responseObserver);
        }

        @Override
        public void archive(ArchiveRequest request,
              StreamObserver<ArchiveResponse> responseObserver) {


            var response = ArchiveResponse.newBuilder()
                  .setFid(ThreadLocalRandom.current().nextLong())
                  .setReqId("ArchiveRequest-" + ThreadLocalRandom.current().nextInt())
                  .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void retrieve(RetrieveRequest request,
              StreamObserver<RetrieveResponse> responseObserver) {
            super.retrieve(request, responseObserver);
        }

        @Override
        public void delete(DeleteRequest request, StreamObserver<Empty> responseObserver) {
            super.delete(request, responseObserver);
        }
    }
}
