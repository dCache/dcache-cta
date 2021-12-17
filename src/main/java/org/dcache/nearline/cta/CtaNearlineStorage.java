package org.dcache.nearline.cta;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.net.HostAndPort;
import com.google.protobuf.Empty;
import cta.admin.CtaAdmin.Version;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import org.dcache.cta.rpc.ArchiveResponse;
import org.dcache.cta.rpc.CtaRpcGrpc;
import org.dcache.cta.rpc.CtaRpcGrpc.CtaRpcStub;
import org.dcache.cta.rpc.RetrieveResponse;
import org.dcache.nearline.cta.xrootd.DataMover;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.util.Checksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CtaNearlineStorage implements NearlineStorage {


    private static final Logger LOGGER = LoggerFactory.getLogger(CtaNearlineStorage.class);

    public static final String CTA_INSTANCE = "cta-instance-name";
    public static final String CTA_ENDPOINT = "cta-frontend-addr";
    public static final String CTA_USER = "cta-user";
    public static final String CTA_GROUP = "cta-group";
    public static final String IO_ENDPOINT = "io-endpoint";
    public static final String IO_PORT = "io-port";

    protected final String type;
    protected final String name;


    /**
     * User name associated with the requests on the CTA side.
     */
    private String ctaUser;

    /**
     * Group name associated with the request on the CTA side.
     */
    private String ctaGroup;

    /**
     * dCache instance identifier within CTA
     */
    private String instanceName;

    /**
     * Foatory, that converts dCache requests to corresponding gRPC messages.
     */
    private RequestsFactory ctaRequestFactory;

    /**
     * CTA gRPC frontend service.
     */
    private CtaRpcStub cta;

    /**
     * Data mover used to read or write files by CTA.
     */
    private DataMover dataMover;

    /**
     * gRPC communication channel.
     */
    private ManagedChannel channel;

    /**
     * Socket address to for IO by CTA.
     */
    private InetSocketAddress ioSocketAddress;

    /**
     * CTA frontend.
     */
    private HostAndPort ctaEndpoint;

    /**
     * Requests submitted to CTA.
     */
    private final ConcurrentMap<String, PendingRequest> pendingRequests = new ConcurrentHashMap<>();

    public CtaNearlineStorage(String type, String name) {

        Objects.requireNonNull(type, "HSM type is not provided");
        Objects.requireNonNull(name, "HSM name is not provided");

        this.type = type;
        this.name = name;
    }

    /**
     * Flush all files in {@code requests} to nearline storage.
     *
     * @param requests
     */
    @Override
    public void flush(Iterable<FlushRequest> requests) {
        for (var fr : requests) {

            var id = fr.getFileAttributes().getPnfsId().toString();

            var r = new ForwardingFlushRequest() {
                @Override
                protected FlushRequest delegate() {
                    return fr;
                }

                @Override
                public void failed(Exception e) {
                    pendingRequests.remove(id);
                    super.failed(e);
                }

                @Override
                public void failed(int i, String s) {
                    pendingRequests.remove(id);
                    super.failed(i, s);
                }

                @Override
                public void completed(Set<URI> uris) {
                    pendingRequests.remove(id);
                    super.completed(uris);
                }
            };

            try {
                r.activate().get();
            } catch (ExecutionException | InterruptedException e) {
                Throwable t = Throwables.getRootCause(e);
                LOGGER.error("Failed to activate flush request: {}", t.getMessage());
                r.failed(e);
            }

            var ar = ctaRequestFactory.valueOf(r);
            cta.archive(ar, new StreamObserver<>() {

                @Override
                public void onNext(ArchiveResponse response) {
                    LOGGER.info("{} : {} : archive id {}, request: {}",
                          r.getId(),
                          r.getFileAttributes().getPnfsId(),
                          response.getFid(),
                          response.getReqId()
                    );
                    pendingRequests.put(id, new PendingRequest(Instant.now(), r));
                }

                @Override
                public void onError(Throwable t) {
                    LOGGER.error("Failed to submit archive request {}", t.getMessage());
                    Exception e =
                          t instanceof Exception ? Exception.class.cast(t) : new Exception(t);
                    r.failed(e);
                }

                @Override
                public void onCompleted() {
                }
            });
        }
    }

    /**
     * Stage all files in {@code requests} from nearline storage.
     *
     * @param requests
     */
    @Override
    public void stage(Iterable<StageRequest> requests) {
        for (var sr : requests) {

            var id = sr.getFileAttributes().getPnfsId().toString();
            var r = new ForwardingStageRequest() {
                @Override
                protected StageRequest delegate() {
                    return sr;
                }

                @Override
                public void failed(Exception e) {
                    pendingRequests.remove(id);
                    super.failed(e);
                }

                @Override
                public void failed(int i, String s) {
                    pendingRequests.remove(id);
                    super.failed(i, s);
                }

                @Override
                public void completed(Set<Checksum> checksums) {
                    pendingRequests.remove(id);
                    super.completed(checksums);
                }
            };

            try {
                r.activate().get();
                r.allocate().get();
            } catch (ExecutionException | InterruptedException e) {
                Throwable t = Throwables.getRootCause(e);
                LOGGER.error("Failed to activate/allocate space for retrieve request: {}",
                      t.getMessage());
                r.failed(e);
            }

            var rr = ctaRequestFactory.valueOf(r);

            cta.retrieve(rr, new StreamObserver<>() {
                @Override
                public void onNext(RetrieveResponse response) {
                    LOGGER.info("{} : {} : request: {}",
                          r.getId(),
                          r.getFileAttributes().getPnfsId(),
                          response.getReqId()
                    );
                    pendingRequests.put(id, new PendingRequest(Instant.now(), r));
                }

                @Override
                public void onError(Throwable t) {
                    LOGGER.error("Failed to submit stage request {}", t.getMessage());
                    Exception e =
                          t instanceof Exception ? Exception.class.cast(t) : new Exception(t);
                    r.failed(e);
                }

                @Override
                public void onCompleted() {
                }
            });
        }
    }

    /**
     * Delete all files in {@code requests} from nearline storage.
     *
     * @param requests
     */
    @Override
    public void remove(Iterable<RemoveRequest> requests) {

        for (var r : requests) {
            var deleteRequest = ctaRequestFactory.valueOf(r);
            cta.delete(deleteRequest, new StreamObserver<>() {
                @Override
                public void onNext(Empty value) {
                    LOGGER.info("Delete request {} submitted {}",
                          r.getId(),
                          r.getUri()
                    );
                    r.completed(null);
                }

                @Override
                public void onError(Throwable t) {
                    LOGGER.error("Failed to submit stage request {}", t.getMessage());
                    Exception e =
                          t instanceof Exception ? Exception.class.cast(t) : new Exception(t);
                    r.failed(e);
                }

                @Override
                public void onCompleted() {

                }
            });
        }
    }

    /**
     * Cancel any flush, stage or remove request with the given id.
     *
     * <p>The failed method of any cancelled request should be called with a CancellationException.
     * If the request completes before it can be cancelled, then the cancellation should be ignored
     * and the completed or failed method should be called as appropriate.
     *
     * <p>A call to cancel must be non-blocking.
     *
     * @param uuid id of the request to cancel
     */
    @Override
    public void cancel(UUID uuid) {
        // FIXME: we need to cancel the requests in CTA.
        var requestIterator =  pendingRequests.entrySet().iterator();
        while (requestIterator.hasNext()) {
            var r = requestIterator.next().getValue().getRequest();
            if (r.getId().equals(uuid)) {
                r.failed(new CancellationException("Canceled by dCache"));
                // no other matches expected.
                break;
            }
        }
    }

    /**
     * Applies a new configuration.
     *
     * @param properties
     * @throws IllegalArgumentException if the configuration is invalid
     */
    @Override
    public void configure(Map<String, String> properties) throws IllegalArgumentException {

        // mandatory options
        String instance = properties.get(CTA_INSTANCE);
        String endpoint = properties.get(CTA_ENDPOINT);
        String user = properties.get(CTA_USER);
        String group = properties.get(CTA_GROUP);

        checkArgument(instance != null, "dCache instance name is not set.");
        checkArgument(endpoint != null, "CTA frontend is not set.");
        checkArgument(user != null, "CTA user is not set.");
        checkArgument(group != null, "CTA group is not set.");

        ctaEndpoint = HostAndPort.fromString(endpoint);
        checkArgument(ctaEndpoint.hasPort(), "Port is not provided for CTA frontend");

        ctaUser = user;
        ctaGroup = group;
        instanceName = instance;

        // Optional options
        String localEndpoint = properties.get(IO_ENDPOINT);
        String localPort = properties.getOrDefault(IO_PORT, "0");
        if (localEndpoint == null) {
            try {
                localEndpoint = InetAddress.getLocalHost().getCanonicalHostName();
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException("Can't detect local host name", e);
            }
        }

        ioSocketAddress = new InetSocketAddress(localEndpoint, Integer.parseInt(localPort));
    }

    @Override
    public void start() {

        dataMover = new DataMover(type, name, ioSocketAddress, pendingRequests);
        dataMover.startAsync().awaitRunning();

        channel = ManagedChannelBuilder
              .forAddress(ctaEndpoint.getHost(), ctaEndpoint.getPort())
              .usePlaintext()
              .build();

        cta = CtaRpcGrpc.newStub(channel);
        channel.notifyWhenStateChanged(ConnectivityState.CONNECTING, () ->
              cta.version(Empty.newBuilder().build(), new StreamObserver<>() {
                  @Override
                  public void onNext(Version version) {
                      LOGGER.info("Connected to CTA version {} : {}", version.getCtaVersion(),
                            version.getXrootdSsiProtobufInterfaceVersion());
                  }

                  @Override
                  public void onError(Throwable t) {
                      LOGGER.error("Failed to get CTA version {}", t.getMessage());
                  }

                  @Override
                  public void onCompleted() {

                  }
              })
        );

        ctaRequestFactory = new RequestsFactory(instanceName, ctaUser, ctaGroup, dataMover);
    }


    /**
     * Cancels all requests and initiates a shutdown of the nearline storage interface.
     *
     * <p>This method does not wait for actively executing requests to terminate.
     */
    @Override
    public void shutdown() {
        if (dataMover != null) {
            dataMover.stopAsync().awaitTerminated();
        }

        if (channel != null) {
            channel.shutdown();
        }
    }

    @VisibleForTesting
    int getPendingRequestsCount() {
        return pendingRequests.size();
    }

    @VisibleForTesting
    PendingRequest getRequest(String id) {
        return pendingRequests.get(id);
    }
}
