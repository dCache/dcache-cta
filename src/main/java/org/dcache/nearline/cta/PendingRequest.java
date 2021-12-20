package org.dcache.nearline.cta;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import org.dcache.pool.nearline.spi.NearlineRequest;

/**
 * Represents Nearline request in pending queue.
 */
public class PendingRequest {

    /**
     * Point on the time-line when request was submitted into pending queue.
     */
    private final Instant submissionTime;

    /**
     * The nearline request.
     */
    private final NearlineRequest request;

    public PendingRequest(NearlineRequest request) {
        this.submissionTime = Instant.now();
        this.request = request;
    }

    /**
     * Returns request submission timestamp.
     *
     * @return request submission timestamp.
     */
    public Instant getSubmissionTime() {
        return submissionTime;
    }

    /**
     * Returns underlying {@link NearlineRequest} requests.
     *
     * @return requests the nearline request.
     */
    public NearlineRequest getRequest() {
        return request;
    }

    /**
     * Returns requests {@link UUID} assigned by dCache's flush queue.
     *
     * @return requests uuid.
     */
    public UUID getRequestId() {
        return request.getId();
    }

    public void cancel() {
        request.failed(new CancellationException("Canceled by dCache"));
    }
}
