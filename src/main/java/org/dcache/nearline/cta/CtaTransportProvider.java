package org.dcache.nearline.cta;

import cta.eos.CtaEos.Transport;

/**
 * Interface to data mover specific url for IO, error and success reporting.
 */
public interface CtaTransportProvider {

    /**
     * Get transport used by CTA for IO, error and success reporting.
     *
     * @param diskFileId String identifier for Disk File ID (a-ka pnfsid)
     * @param ctaArchiveId CTA Archive File ID.
     * @return transport used by CTA for IO, error and success reporting.
     */
    Transport getTransport(String diskFileId, long ctaArchiveId);

}
