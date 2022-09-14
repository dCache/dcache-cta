package org.dcache.nearline.cta;

import ch.cern.cta.rpc.ArchiveResponse;
import ch.cern.cta.rpc.CreateResponse;
import ch.cern.cta.rpc.CtaRpcGrpc;
import ch.cern.cta.rpc.RetrieveResponse;
import ch.cern.cta.rpc.SchedulerRequest;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class DummyCta {

    private final Server server;
    private volatile boolean fail;

    private volatile boolean drop;

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
    }

    public void start() throws IOException {
        server.start();
    }

    public void shutdown() throws InterruptedException {
        server.shutdown();
        if(!server.awaitTermination(1, TimeUnit.SECONDS)) {
            server.shutdownNow();
        }
    }

    public String getConnectString() {
        return "localhost:" + server.getPort();
    }

    public void fail() {
        fail = true;
    }

    public void drop() {
        drop = true;
    }
    private class CtaSvc extends CtaRpcGrpc.CtaRpcImplBase {

        @Override
        public void version(Empty request, StreamObserver<Version> responseObserver) {

            if (drop) {
                return;
            }

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
        public void create(SchedulerRequest request,
              StreamObserver<CreateResponse> responseObserver) {

            if (drop) {
                return;
            }

            if (request.getMd().getWf().getInstance().getName().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getCli().getUser().getUsername().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getCli().getUser().getGroupname().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getFile().getOwner().getUid() == 0) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getFile().getOwner().getGid() == 0) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getFile().getLpath().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (!fail) {
                var response = CreateResponse.newBuilder()
                      .setArchiveFileId(ThreadLocalRandom.current().nextLong())
                      .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(new StatusException(Status.INTERNAL));
            }

        }

        @Override
        public void archive(SchedulerRequest request,
              StreamObserver<ArchiveResponse> responseObserver) {

            if (drop) {
                return;
            }

            if (request.getMd().getWf().getInstance().getName().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getCli().getUser().getUsername().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getCli().getUser().getGroupname().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getFile().getOwner().getUid() == 0) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getFile().getOwner().getGid() == 0) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getFile().getLpath().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getFile().getArchiveFileId() == 0L) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (!fail) {
                var response = ArchiveResponse.newBuilder()
                      .setObjectstoreId("ArchiveRequest-" + ThreadLocalRandom.current().nextInt())
                      .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(new StatusException(Status.INTERNAL));
            }
        }

        @Override
        public void retrieve(SchedulerRequest request,
              StreamObserver<RetrieveResponse> responseObserver) {

            if (drop) {
                return;
            }

            if (request.getMd().getWf().getInstance().getName().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getCli().getUser().getUsername().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getCli().getUser().getGroupname().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getFile().getOwner().getUid() == 0) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getFile().getOwner().getGid() == 0) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getFile().getLpath().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getFile().getArchiveFileId() == 0L) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (!fail) {
                var response = RetrieveResponse.newBuilder()
                      .setObjectstoreId("RetrieveRequest-" + ThreadLocalRandom.current().nextInt())
                      .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(new StatusException(Status.INTERNAL));
            }
        }

        @Override
        public void cancelRetrieve(SchedulerRequest request,
              StreamObserver<Empty> responseObserver) {

            if (drop) {
                return;
            }

            if (request.getMd().getWf().getInstance().getName().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getCli().getUser().getUsername().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getCli().getUser().getGroupname().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getFile().getArchiveFileId() == 0L) {
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
        public void delete(SchedulerRequest request, StreamObserver<Empty> responseObserver) {

            if (drop) {
                return;
            }

            if (request.getMd().getWf().getInstance().getName().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getCli().getUser().getUsername().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getCli().getUser().getGroupname().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getFile().getOwner().getUid() == 0) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getFile().getOwner().getGid() == 0) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getFile().getLpath().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getMd().getFile().getArchiveFileId() == 0L) {
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
        waitToReply(1);
    }

    public void waitToReply(int n) throws AssertionError {
        try {
            TimeUnit.SECONDS.sleep(n);
        } catch (InterruptedException e) {
            throw new AssertionError("Should neve happen", e);
        }
    }
}
