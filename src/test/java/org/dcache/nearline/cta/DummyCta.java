package org.dcache.nearline.cta;

import com.google.protobuf.Empty;
import cta.admin.CtaAdmin.Version;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.dcache.cta.rpc.ArchiveRequest;
import org.dcache.cta.rpc.ArchiveResponse;
import org.dcache.cta.rpc.CancelRetrieveRequest;
import org.dcache.cta.rpc.CtaRpcGrpc;
import org.dcache.cta.rpc.DeleteRequest;
import org.dcache.cta.rpc.RetrieveRequest;
import org.dcache.cta.rpc.RetrieveResponse;

public class DummyCta {

    private final Server server;
    private volatile boolean fail;

    public DummyCta() {
        server = NettyServerBuilder.forPort(0).addService(new CtaSvc()).build();
        fail = false;
    }

    public void start() throws IOException {
        server.start();
    }

    public void shutdown() throws InterruptedException {
        server.shutdown();
        server.awaitTermination();
    }

    public String getConnectString() {
        return "localhost:" + server.getPort();
    }

    public void fail() {
        fail = true;
    }

    private class CtaSvc extends CtaRpcGrpc.CtaRpcImplBase {

        @Override
        public void version(Empty request, StreamObserver<Version> responseObserver) {
            if (!fail) {
                responseObserver.onNext(
                      Version.newBuilder()
                            .setCtaVersion("embedded dummy x-x-x")
                            .setXrootdSsiProtobufInterfaceVersion("testing")
                            .build()
                );
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(new StatusException(Status.INTERNAL));
            }
        }

        @Override
        public void archive(ArchiveRequest request,
              StreamObserver<ArchiveResponse> responseObserver) {

            if (!fail) {
                var response = ArchiveResponse.newBuilder()
                      .setFid(ThreadLocalRandom.current().nextLong())
                      .setReqId("ArchiveRequest-" + ThreadLocalRandom.current().nextInt())
                      .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(new StatusException(Status.INTERNAL));
            }
        }

        @Override
        public void retrieve(RetrieveRequest request,
              StreamObserver<RetrieveResponse> responseObserver) {

            if (!fail) {
                var response = RetrieveResponse.newBuilder()
                      .setReqId("RetrieveRequest-" + ThreadLocalRandom.current().nextInt())
                      .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(new StatusException(Status.INTERNAL));
            }
        }

        @Override
        public void cancelRetrieve(CancelRetrieveRequest request,
              StreamObserver<Empty> responseObserver) {
            if (!fail) {
                var response = Empty.newBuilder()
                      .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(new StatusException(Status.INTERNAL));
            }
        }

        @Override
        public void delete(DeleteRequest request, StreamObserver<Empty> responseObserver) {
            if (!fail) {
                var response = Empty.newBuilder()
                      .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(new StatusException(Status.INTERNAL));
            }
        }
    }

    public void waitToReply() throws AssertionError {
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            throw new AssertionError("Should neve happen", e);
        }
    }
}
