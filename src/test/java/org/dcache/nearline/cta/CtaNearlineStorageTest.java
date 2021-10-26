package org.dcache.nearline.cta;

import static org.dcache.nearline.cta.CtaNearlineStorage.*;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import diskCacheV111.vehicles.GenericStorageInfo;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.vehicles.FileAttributes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CtaNearlineStorageTest {

    private DummyCta cta;
    private Map<String, String> drvConfig;
    private CtaNearlineStorage driver;

    private CompletableFuture<Void> waitForComplete;

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
        try {
            cta.shutdown();
        } catch (InterruptedException e) {
        }
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
        driver.start();
    }

    @Test
    public void testFlushRequestActivationOnSubmit() {

        var request = mockedFlushRequest();
        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.flush(Set.of(request));

        verify(request).activate();
    }

    @Test
    public void testStageRequestActivationOnSubmit() {

        var request = mockedStageRequest();
        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.stage(Set.of(request));

        verify(request).activate();
    }

    @Test
    public void testSpaceAllocationOnSubmit() {

        var request = mockedStageRequest();
        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.stage(Set.of(request));

        verify(request).allocate();
    }

    @Test
    public void testFlushRequestFailActivation() {

        var request = mockedFlushRequest();
        when(request.activate()).thenReturn(
              Futures.immediateFailedFuture(new IOException("injected error")));

        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.flush(Set.of(request));

        verify(request).failed(any());
    }

    @Test
    public void testStageRequestFailActivation() {

        var request = mockedStageRequest();
        when(request.activate()).thenReturn(
              Futures.immediateFailedFuture(new IOException("injected error")));

        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.stage(Set.of(request));

        verify(request).failed(any());
    }

    @Test
    public void testStageRequestFailAllocation() {

        var request = mockedStageRequest();
        when(request.allocate()).thenReturn(
              Futures.immediateFailedFuture(new IOException("injected error")));

        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.stage(Set.of(request));

        verify(request).failed(any());
    }

    @Test
    public void testStartAfterShutdown() {

        driver = new CtaNearlineStorage("aType", "aName");
        driver.configure(drvConfig);
        driver.start();
        driver.shutdown();

        driver = new CtaNearlineStorage("aType", "aName");
        driver.configure(drvConfig);
        driver.start();
        driver.shutdown();
    }

    @Test
    public void testStageRequestFailOnRpcError() {

        var request = mockedStageRequest();

        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        cta.fail();
        driver.stage(Set.of(request));
        waitToComplete();

        verify(request).failed(any());
    }

    @Test
    public void testFlushRequestFailOnRpcError() {

        var request = mockedFlushRequest();

        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        cta.fail();
        driver.flush(Set.of(request));
        waitToComplete();

        verify(request).failed(any());
    }

    @Test
    public void testSuccessOnRemove() {

        var request = mockedRemoveRequest();

        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.remove(Set.of(request));
        waitToComplete();

        verify(request).completed(any());
    }

    @Test
    public void testRemoveRequestOnRpcError() {

        var request = mockedRemoveRequest();

        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();
        cta.fail();

        driver.remove(Set.of(request));
        waitToComplete();

        verify(request).failed(any());
    }

    void waitToComplete() {
        try {
            waitForComplete.get(1, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            fail("Request not complete");
        }
    }

    private FlushRequest mockedFlushRequest() {

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

        waitForComplete = new CompletableFuture<>();

        doAnswer(i -> {
            waitForComplete.complete(null);
            return null;
        }).when(request).failed(any());

        doAnswer(i -> {
            waitForComplete.complete(null);
            return null;
        }).when(request).failed(anyInt(), any());

        doAnswer(i -> {
            waitForComplete.complete(null);
            return null;
        }).when(request).completed(any());

        return request;
    }

    private StageRequest mockedStageRequest() {

        var storageInfo = GenericStorageInfo.valueOf("a:b@z", "*");
        storageInfo.addLocation(URI.create("cta://cta?archiveid=9876543210"));

        var attrs = FileAttributes.of()
              .size(9876543210L)
              .storageClass(storageInfo.getStorageClass())
              .hsm(storageInfo.getHsm())
              .storageInfo(storageInfo)
              .pnfsId("0000C9B4E3768770452E8B1B8E0232584872")
              .build();

        var request = mock(StageRequest.class);

        when(request.activate()).thenReturn(Futures.immediateFuture(null));
        when(request.allocate()).thenReturn(Futures.immediateFuture(null));
        when(request.getFileAttributes()).thenReturn(attrs);
        when(request.getId()).thenReturn(UUID.randomUUID());

        waitForComplete = new CompletableFuture<>();

        doAnswer(i -> {
            waitForComplete.complete(null);
            return null;
        }).when(request).failed(any());

        doAnswer(i -> {
            waitForComplete.complete(null);
            return null;
        }).when(request).failed(anyInt(), any());

        doAnswer(i -> {
            waitForComplete.complete(null);
            return null;
        }).when(request).completed(any());

        return request;
    }

    private RemoveRequest mockedRemoveRequest() {

        var request = mock(RemoveRequest.class);

        when(request.getUri()).thenReturn(
              URI.create("cta://cta/0000C9B4E3768770452E8B1B8E0232584872?archiveid=1234"));

        waitForComplete = new CompletableFuture<>();

        doAnswer(i -> {
            waitForComplete.complete(null);
            return null;
        }).when(request).failed(any());

        doAnswer(i -> {
            waitForComplete.complete(null);
            return null;
        }).when(request).failed(anyInt(), any());

        doAnswer(i -> {
            waitForComplete.complete(null);
            return null;
        }).when(request).completed(any());
        return request;
    }

}