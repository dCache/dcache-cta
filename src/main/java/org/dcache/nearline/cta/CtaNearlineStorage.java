package org.dcache.nearline.cta;

import static com.google.common.base.Preconditions.checkArgument;

import java.net.URI;
import java.util.Map;
import java.util.UUID;

import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;

public class CtaNearlineStorage implements NearlineStorage {

    private final String CTA_INSTANCE = "cta-instance-name";
    private final String CTA_ENDPOINT = "cta-frontend-addr";
    private final String CTA_USER = "cta-user";
    private final String CTA_GROUP = "cta-group";
    private final String IO_ENDPOINT = "io-endpoint";
    private final String IO_PORT = "io-port";

    protected final String type;
    protected final String name;

    private RequestsFactory ctaRequestFactory;

    public CtaNearlineStorage(String type, String name) {
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
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Stage all files in {@code requests} from nearline storage.
     *
     * @param requests
     */
    @Override
    public void stage(Iterable<StageRequest> requests) {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Delete all files in {@code requests} from nearline storage.
     *
     * @param requests
     */
    @Override
    public void remove(Iterable<RemoveRequest> requests) {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Cancel any flush, stage or remove request with the given id.
     *
     * <p>The failed method of any cancelled request should be called with a CancellationException.
     * If
     * the request completes before it can be cancelled, then the cancellation should be ignored and
     * the completed or failed method should be called as appropriate.
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

        // Optional options
        String localEndpoint = properties.get(IO_ENDPOINT);
        String localPort = properties.get(IO_PORT);

        URI ioUrl = URI.create("root://" + localEndpoint + ":" + localPort);
        ctaRequestFactory = new RequestsFactory(instance, user, group, ioUrl.toString());
    }

    /**
     * Cancels all requests and initiates a shutdown of the nearline storage interface.
     *
     * <p>This method does not wait for actively executing requests to terminate.
     */
    @Override
    public void shutdown() {
    }
}