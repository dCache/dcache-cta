package org.dcache.nearline.cta;

import static org.junit.Assert.*;

import org.dcache.pool.nearline.spi.NearlineStorage;
import org.junit.Test;

public class CtaNearlineStorageProviderTest {

    @Test
    public void testProviderName() {
        CtaNearlineStorageProvider provider = new CtaNearlineStorageProvider();
        assertEquals("the provider name is incorrect", "dcache-cta", provider.getName());
    }

    @Test
    public void testDescriptionNotNull() {
        CtaNearlineStorageProvider provider = new CtaNearlineStorageProvider();
        assertNotNull(provider.getDescription(), "Description can be null");
    }

    @Test
    public void testDriverType() {
        CtaNearlineStorageProvider provider = new CtaNearlineStorageProvider();

        NearlineStorage nearlineStorage = provider.createNearlineStorage("foo", "bar");
        try {
            assertEquals("incorrect driver type", CtaNearlineStorage.class,
                  nearlineStorage.getClass());
        } finally {
            nearlineStorage.shutdown();
        }
    }

}