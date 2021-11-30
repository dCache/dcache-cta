package org.dcache.nearline.cta.xrootd;

import java.time.Instant;
import org.dcache.pool.nearline.spi.NearlineRequest;

/**
 * Represents Nearline request in pending queue.
 */
public class PendingRequest {

    /**
     *  Point on the time-line when request was submitted into pending queue.
     */
    private final Instant submitionTime;

    /**
     * The nearline request.
     */
    private final NearlineRequest request;

    public PendingRequest(Instant submitionTime, NearlineRequest request) {
        this.submitionTime = submitionTime;
        this.request = request;
    }

    public Instant getSubmitionTime() {
        return submitionTime;
    }

    public NearlineRequest getRequest() {
        return request;
    }
}
