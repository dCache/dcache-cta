package org.dcache.nearline.cta;

import static org.mockito.Mockito.spy;
import ch.cern.cta.rpc.CtaRpcGrpc;
import ch.cern.cta.rpc.Request;
import ch.cern.cta.rpc.Response;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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

    private final CtaRpcGrpc.CtaRpcImplBase ctaSvc;

    public DummyCta(File cert, File key) throws Exception {

        ctaSvc = spy(new CtaSvc());

        server = NettyServerBuilder.forPort(0)
              .sslContext(GrpcSslContexts.forServer(cert, key)
                    .clientAuth(ClientAuth.NONE)
                    .protocols("TLSv1.3", "TLSv1.2")
                    .build()
              )
              .bossEventLoopGroup( new NioEventLoopGroup(2, new ThreadFactoryBuilder().setNameFormat("dummy-cta-server-accept-%d").build()))
              .workerEventLoopGroup(new NioEventLoopGroup(2, new ThreadFactoryBuilder().setNameFormat("dummy-cta-server-worker-%d").build()))
              .channelType(NioServerSocketChannel.class)
              .addService(ctaSvc)
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

    public CtaRpcGrpc.CtaRpcImplBase ctaSvc() {
        return ctaSvc;
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
        public void create(Request request,
              StreamObserver<Response> responseObserver) {

            if (drop) {
                return;
            }

            if (request.getNotification().getWf().getInstance().getName().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getCli().getUser().getUsername().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getCli().getUser().getGroupname().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getFile().getOwner().getUid() == 0) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getFile().getOwner().getGid() == 0) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getFile().getLpath().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (!fail) {
                var response = Response.newBuilder()
                      .setArchiveFileId(Long.toString(ThreadLocalRandom.current().nextLong()))
                      .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(new StatusException(Status.INTERNAL));
            }

        }

        @Override
        public void archive(Request request,
              StreamObserver<Response> responseObserver) {

            if (drop) {
                return;
            }

            if (request.getNotification().getWf().getInstance().getName().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getCli().getUser().getUsername().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getCli().getUser().getGroupname().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getFile().getOwner().getUid() == 0) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getFile().getOwner().getGid() == 0) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getFile().getLpath().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getFile().getArchiveFileId() == 0L) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (!fail) {
                var response = Response.newBuilder()
                      .setRequestObjectstoreId("ArchiveRequest-" + ThreadLocalRandom.current().nextInt())
                      .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(new StatusException(Status.INTERNAL));
            }
        }

        @Override
        public void retrieve(Request request,
              StreamObserver<Response> responseObserver) {

            if (drop) {
                return;
            }

            if (request.getNotification().getWf().getInstance().getName().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getCli().getUser().getUsername().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getCli().getUser().getGroupname().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getFile().getOwner().getUid() == 0) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getFile().getOwner().getGid() == 0) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getFile().getLpath().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getFile().getArchiveFileId() == 0L) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (!fail) {
                var response = Response.newBuilder()
                      .setRequestObjectstoreId("RetrieveRequest-" + ThreadLocalRandom.current().nextInt())
                      .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(new StatusException(Status.INTERNAL));
            }
        }

        @Override
        public void cancelRetrieve(Request request,
              StreamObserver<Response> responseObserver) {

            if (drop) {
                return;
            }

            if (request.getNotification().getWf().getInstance().getName().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getCli().getUser().getUsername().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getCli().getUser().getGroupname().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getFile().getArchiveFileId() == 0L) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (!fail) {
                var response = Response.newBuilder()
                      .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(new StatusException(Status.INTERNAL));
            }
        }

        @Override
        public void delete(Request request, StreamObserver<Response> responseObserver) {

            if (drop) {
                return;
            }

            if (request.getNotification().getWf().getInstance().getName().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getCli().getUser().getUsername().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getCli().getUser().getGroupname().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getFile().getOwner().getUid() == 0) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getFile().getOwner().getGid() == 0) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getFile().getLpath().isEmpty()) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (request.getNotification().getFile().getArchiveFileId() == 0L) {
                responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
                return;
            }

            if (!fail) {
                var response = Response.newBuilder()
                      .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(new StatusException(Status.INTERNAL));
            }
        }
    }
}
