package org.dcache.nearline.cta;

import static org.dcache.nearline.cta.CtaNearlineStorage.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.vehicles.FileAttributes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CtaNearlineStorageTest {

    private DummyCta cta;
    private Map<String, String> drvConfig;
    private CtaNearlineStorage driver;

    @Before
    public void setUp() throws IOException {
        cta = new DummyCta();
        cta.start();

        // make mutable config
        drvConfig = new HashMap<>(
              Map.of(
                    CTA_USER, "foo",
                    CTA_GROUP, "bar",
                    CTA_INSTANCE, "foobar",
                    CTA_ENDPOINT, cta.getConnectString(),
                    IO_PORT, "9991"
              )
        );
    }

    @After
    public void tierDown() {
        cta.shutdown();
        if (driver != null) {
            driver.shutdown();
        }
    }

    @Test(expected = NullPointerException.class)
    public void testMissingHsmType() {
        new CtaNearlineStorage(null, "aName");
    }

    @Test(expected = NullPointerException.class)
    public void testMissingHsmName() {
        new CtaNearlineStorage("aType", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCfgMissingInstance() {

        driver = new CtaNearlineStorage("aType", "aName");

        drvConfig.remove(CTA_INSTANCE);
        driver.configure(drvConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCfgMissingUser() {

        driver = new CtaNearlineStorage("aType", "aName");

        drvConfig.remove(CTA_USER);
        driver.configure(drvConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCfgMissingGroup() {

        driver = new CtaNearlineStorage("aType", "aName");
        drvConfig.remove(CTA_GROUP);
        driver.configure(drvConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCfgMissingEndpoint() {

        driver = new CtaNearlineStorage("aType", "aName");

        drvConfig.remove(CTA_ENDPOINT);
        driver.configure(drvConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCfgInvalidEndpoint() {

        driver = new CtaNearlineStorage("aType", "aName");

        drvConfig.put(CTA_ENDPOINT, "localhost");
        driver.configure(drvConfig);
    }

    @Test
    public void testCfgAcceptValid() {

        driver = new CtaNearlineStorage("aType", "aName");
        driver.configure(drvConfig);
    }

    @Test
    public void testRequestActivationOnSubmit() {

        var request = mockedRequest();
        var driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);

        driver.flush(Set.of(request));

        verify(request).activate();
    }

    @Test
    public void testStartAfterShutdown() {

        driver = new CtaNearlineStorage("aType", "aName");
        driver.configure(drvConfig);
        driver.shutdown();

        driver = new CtaNearlineStorage("aType", "aName");
        driver.configure(drvConfig);
        driver.shutdown();
    }


    private FlushRequest mockedRequest() {

        var attrs = FileAttributes.of()
              .size(9876543210L)
              .storageClass("a:b")
              .hsm("z")
              .pnfsId("0000C9B4E3768770452E8B1B8E0232584872")
              .build();

        var request = mock(FlushRequest.class);

        when(request.activate()).thenReturn(Futures.immediateFuture(null));
        when(request.getFileAttributes()).thenReturn(attrs);
        when(request.getId()).thenReturn(UUID.randomUUID());

        return request;
    }
}