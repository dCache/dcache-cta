package org.dcache.nearline.cta;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Empty;
import cta.admin.CtaAdmin.Version;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import ch.cern.cta.rpc.ArchiveRequest;
import ch.cern.cta.rpc.ArchiveResponse;
import ch.cern.cta.rpc.CancelRetrieveRequest;
import ch.cern.cta.rpc.CtaRpcGrpc;
import ch.cern.cta.rpc.DeleteRequest;
import ch.cern.cta.rpc.RetrieveRequest;
import ch.cern.cta.rpc.RetrieveResponse;

public class DummyCta {

    private final Server server;
    private volatile boolean fail;

    public DummyCta(File cert, File key) throws Exception {
        server = NettyServerBuilder.forPort(0)
              .sslContext(GrpcSslContexts.forServer(cert, key)
                    .clientAuth(ClientAuth.NONE)
                    .protocols("TLSv1.3", "TLSv1.2")
                    .build()
              )
              .bossEventLoopGroup( new NioEventLoopGroup(2, new ThreadFactoryBuilder().setNameFormat("dummy-cta-server-accept-%d").build()))
              .workerEventLoopGroup(new NioEventLoopGroup(2, new ThreadFactoryBuilder().setNameFormat("dummy-cta-server-worker-%d").build()))
              .channelType(NioServerSocketChannel.class)
              .addService(new CtaSvc())
              .directExecutor()
              .build();
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

            if (request.getInstance().getName().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getCli().getUser().getUsername().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getCli().getUser().getGroupname().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getFile().getUid() == 0) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getFile().getGid() == 0) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getFile().getPath().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

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

            if (request.getInstance().getName().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getCli().getUser().getUsername().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getCli().getUser().getGroupname().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getFile().getUid() == 0) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getFile().getGid() == 0) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getFile().getPath().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getArchiveId() == 0L) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

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

            if (request.getInstance().getName().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getCli().getUser().getUsername().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getCli().getUser().getGroupname().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getArchiveId() == 0L) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

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

            if (request.getInstance().getName().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getCli().getUser().getUsername().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getCli().getUser().getGroupname().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getFile().getUid() == 0) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getFile().getGid() == 0) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getFile().getPath().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getArchiveId() == 0L) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

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
