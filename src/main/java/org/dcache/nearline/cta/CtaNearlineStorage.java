package org.dcache.nearline.cta;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Throwables;
import com.google.common.net.HostAndPort;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import org.dcache.cta.rpc.CtaRpcGrpc;
import org.dcache.cta.rpc.CtaRpcGrpc.CtaRpcBlockingStub;
import org.dcache.nearline.cta.xrootd.DataMover;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.NearlineRequest;
import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;
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
    private CtaRpcBlockingStub cta;

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
    private final ConcurrentMap<String, NearlineRequest> pendingRequests = new ConcurrentHashMap<>();

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
        for (var r : requests) {
            try {
                r.activate().get();
                var ar = ctaRequestFactory.valueOf(r);
                var id = r.getFileAttributes().getPnfsId().toString();
                pendingRequests.put(id, r);
                var response = cta.archive(ar);

                LOGGER.info("{} : {} : archive id {}, request: {}",
                      r.getId(),
                      r.getFileAttributes().getPnfsId(),
                      response.getFid(),
                      response.getReqId()
                );

            } catch (ExecutionException | InterruptedException e) {
                Throwable t = Throwables.getRootCause(e);
                LOGGER.error("Failed to submit flush request: {}", t.getMessage());
                pendingRequests.remove(r.getId().toString());
                r.failed(e);
            }

        }
    }

    /**
     * Stage all files in {@code requests} from nearline storage.
     *
     * @param requests
     */
    @Override
    public void stage(Iterable<StageRequest> requests) {
        for (var r : requests) {
            try {
                r.activate().get();
                r.allocate().get();
                var rr = ctaRequestFactory.valueOf(r);
                var id = r.getFileAttributes().getPnfsId().toString();
                pendingRequests.put(id, r);
                var response = cta.retrieve(rr);

                LOGGER.info("{} : {} : request: {}",
                      r.getId(),
                      r.getFileAttributes().getPnfsId(),
                      response.getReqId()
                );

            } catch (ExecutionException | InterruptedException e) {
                Throwable t = Throwables.getRootCause(e);
                LOGGER.error("Failed to submit retrieve request: {}", t.getMessage());
                pendingRequests.remove(r.getId().toString());
                r.failed(e);
            }
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
            var reply = cta.delete(deleteRequest);
            r.completed(null);
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
        cta = CtaRpcGrpc.newBlockingStub(channel);

        var version = cta.version(Empty.newBuilder().build());
        LOGGER.info("Connected to CTA version {} : {}", version.getCtaVersion(),
              version.getXrootdSsiProtobufInterfaceVersion());

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
}
