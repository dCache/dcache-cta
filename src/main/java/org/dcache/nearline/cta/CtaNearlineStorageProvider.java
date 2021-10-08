package org.dcache.nearline.cta;

import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.NearlineStorageProvider;

public class CtaNearlineStorageProvider implements NearlineStorageProvider {

    @Override
    public String getName() {
        return "org.dcache.nearline.cta.dcache-cta";
    }

    @Override
    public String getDescription() {
        return "dCache Nearline Storage Driver for CTA";
    }

    @Override
    public NearlineStorage createNearlineStorage(String type, String name) {
        return new CtaNearlineStorage(type, name);
    }
}
