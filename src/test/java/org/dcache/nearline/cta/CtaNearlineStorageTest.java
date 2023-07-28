package org.dcache.nearline.cta;

import static java.io.File.createTempFile;
import static org.dcache.nearline.cta.CtaNearlineStorage.CTA_ENDPOINT;
import static org.dcache.nearline.cta.CtaNearlineStorage.CTA_GROUP;
import static org.dcache.nearline.cta.CtaNearlineStorage.CTA_INSTANCE;
import static org.dcache.nearline.cta.CtaNearlineStorage.CTA_REQUEST_TIMEOUT;
import static org.dcache.nearline.cta.CtaNearlineStorage.CTA_USER;
import static org.dcache.nearline.cta.CtaNearlineStorage.IO_ENDPOINT;
import static org.dcache.nearline.cta.CtaNearlineStorage.IO_PORT;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.google.common.util.concurrent.Futures;
import diskCacheV111.vehicles.GenericStorageInfo;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.vehicles.FileAttributes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
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

public class CtaNearlineStorageTest {

    private Map<String, String> drvConfig;
    private CtaNearlineStorage driver;

    private CompletableFuture<Void> waitForComplete;

    @Before
    public void setUp() throws Exception {


        // make mutable config
        drvConfig = new HashMap<>(
              Map.of(
                    CTA_USER, "root",
                    CTA_GROUP, "bar",
                    CTA_INSTANCE, "ctadev",
                    CTA_ENDPOINT, "dcache-enstore01.desy.de:17017",
                    IO_PORT, "9991",
                    IO_ENDPOINT, "131.169.253.133",
                    CTA_REQUEST_TIMEOUT, "30"
              )
        );
    }

    @After
    public void tierDown() {
        if (driver != null) {
            driver.shutdown();
        }
    }


    @Test
    public void testCfgAcceptValid() throws InterruptedException, IOException {

        driver = new CtaNearlineStorage("aType", "aName");
        driver.configure(drvConfig);
        driver.start();
        TimeUnit.SECONDS.sleep(5);

        var r = mockedStageRequest("0000E896BF4CD65C45B6AAC571FB1DCE6526", 3714246, 31677440L);

        driver.stage(Set.of(r));
        TimeUnit.SECONDS.sleep(900);
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
              .creationTime(System.currentTimeMillis())
              .pnfsId("0000C9B4E3768770452E8B1B8E0232584872")
              .checksum(new Checksum(ChecksumType.ADLER32.createMessageDigest()))
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

    private StageRequest mockedStageRequest(String id, long aid, long size) throws IOException {

        var storageInfo = GenericStorageInfo.valueOf("a:b@z", "*");
        storageInfo.addLocation(URI.create("cta://cta?archiveid=" + aid));

        var attrs = FileAttributes.of()
                .size(size)
                .storageClass(storageInfo.getStorageClass())
                .hsm(storageInfo.getHsm())
                .storageInfo(storageInfo)
                .creationTime(System.currentTimeMillis())
                .pnfsId(id)
                .build();

        var request = mock(StageRequest.class);

        when(request.activate()).thenReturn(Futures.immediateFuture(null));
        when(request.allocate()).thenReturn(Futures.immediateFuture(null));
        when(request.getFileAttributes()).thenReturn(attrs);
        when(request.getId()).thenReturn(UUID.randomUUID());

        File f = createTempFile(id, "-stage");
        f.delete();
        f.deleteOnExit();

        when(request.getReplicaUri()).thenReturn(f.toURI());


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
              .creationTime(System.currentTimeMillis())
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