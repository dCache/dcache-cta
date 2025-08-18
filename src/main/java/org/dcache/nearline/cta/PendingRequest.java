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
     * The type of the request.
     * <p>
     * STAGE - request to stage file from nearline storage to online storage.
     * FLUSH - request to flush file from online storage to nearline storage.
     * CANCEL - request to cancel staged file.
     * DELETE - request to delete archived file.
     */
    public enum Type {
        STAGE, // request to stage file
        FLUSH, // request to flush file
        CANCEL, // request to cancel staged file
        DELETE // request to delete arcived file
    }

    /**
     * Point on the time-line when request was submitted into pending queue.
     */
    private final Instant submissionTime;

    /**
     * The nearline request.
     */
    private final NearlineRequest request;

    /**
     * CTA scheduler request ID (aka object store ID).
     */
    private final String ctaRequestId;

    /**
     * The type of the request.
     */
    private Type action;

    /**
     * Constructs a new pending request.
     *
     * @param request the nearline request.
     * @param ctaRequestId the CTA request ID (aka object store ID).
     * @param action the type of the request.
     */
    public PendingRequest(NearlineRequest request, String ctaRequestId, Type action) {
        this.submissionTime = Instant.now();
        this.request = request;
        this.ctaRequestId  = ctaRequestId;
        this.action = action;
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

    /**
     * Returns CTA request ID (aka object store ID).
     *
     * @return CTA request ID.
     */
    public String getCtaRequestId() {
        return ctaRequestId;
    }

    /**
     * Returns the request type.
     *
     * @return the request type.
     */
    public Type getAction() {
        return action;
    }

    public void cancel() {
        request.failed(new CancellationException("Canceled by dCache"));
    }
}
