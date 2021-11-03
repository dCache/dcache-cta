package org.dcache.nearline.cta.xrootd;

import static java.io.File.createTempFile;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import diskCacheV111.vehicles.GenericStorageInfo;
import io.netty.channel.ChannelHandlerContext;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Base64;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.NearlineRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.vehicles.FileAttributes;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.protocol.messages.CloseRequest;
import org.dcache.xrootd.protocol.messages.OpenRequest;
import org.dcache.xrootd.protocol.messages.QueryRequest;
import org.dcache.xrootd.protocol.messages.StatRequest;
import org.junit.Before;
import org.junit.Test;

public class DataServerHandlerTest {


    private DataServerHandler handler;
    private ConcurrentMap<String, NearlineRequest> requests;
    private ChannelHandlerContext ctx;
    private CompletableFuture<Void> waitForComplete;

    @Before
    public void setUp() throws Exception {
        ctx = mock(ChannelHandlerContext.class);
        requests = new ConcurrentHashMap<>();
        handler = new DataServerHandler("cta", "test", requests);
    }

    @Test(expected = XrootdException.class)
    public void testOpenWithoutRequests() throws XrootdException {

        var buf = new ByteBufBuilder()
              .withShort(1)    // stream id
              .withShort(XrootdProtocol.kXR_open)
              .withShort(0)
              .withShort(XrootdProtocol.kXR_open_read | XrootdProtocol.kXR_retstat)
              .withZeros(12) // padding
              .withString("0000C9B4E3768770452E8B1B8E0232584872", UTF_8)
              .build();

        OpenRequest msg = new OpenRequest(buf);

        handler.doOnOpen(ctx, msg);
    }

    @Test
    public void testOpenForStage() throws XrootdException, IOException {

        var stageRequest = mockedStageRequest();

        var buf = new ByteBufBuilder()
              .withShort(1)    // stream id
              .withShort(XrootdProtocol.kXR_open)
              .withShort(0)
              .withShort(XrootdProtocol.kXR_delete | XrootdProtocol.kXR_new)
              .withZeros(12) // padding
              .withString("0000C9B4E3768770452E8B1B8E0232584872", UTF_8)
              .build();

        OpenRequest msg = new OpenRequest(buf);

        handler.doOnOpen(ctx, msg);
        assertTrue(new File(stageRequest.getReplicaUri()).exists());
    }


    @Test
    public void testOpenForFlush() throws XrootdException, IOException {

        var stageRequest = mockedFlushRequest();

        var buf = new ByteBufBuilder()
              .withShort(1)    // stream id
              .withShort(XrootdProtocol.kXR_open)
              .withZeros(16) // padding
              .withString("0000C9B4E3768770452E8B1B8E0232584872", UTF_8)
              .withShort(0)
              .withShort(XrootdProtocol.kXR_open_read)
              .build();

        OpenRequest msg = new OpenRequest(buf);

        handler.doOnOpen(ctx, msg);
    }

    @Test(expected = XrootdException.class)
    public void testOpenForFlushDontExists() throws XrootdException, IOException {

        var stageRequest = mockedFlushRequest();

        var buf = new ByteBufBuilder()
              .withShort(1)    // stream id
              .withShort(XrootdProtocol.kXR_open)
              .withZeros(16) // padding
              .withString("0000C9B4E3768770452E8B1B8E0232584872", UTF_8)
              .withShort(0)
              .withShort(XrootdProtocol.kXR_open_read)
              .build();

        new File(stageRequest.getReplicaUri()).delete();
        OpenRequest msg = new OpenRequest(buf);

        handler.doOnOpen(ctx, msg);
    }

    @Test(expected = XrootdException.class)
    public void testCloseNoOpen() throws XrootdException, IOException {

        var buf = new ByteBufBuilder()
              .withShort(1)    // stream id
              .withShort(XrootdProtocol.kXR_close)
              .withInt(1) // fh
              .build();

        var msg = new CloseRequest(buf);

        handler.doOnClose(ctx, msg);
    }

    @Test
    public void testCloseAfterStage() throws XrootdException, IOException, InterruptedException {

        var stageRequest = mockedStageRequest();

        var buf = new ByteBufBuilder()
              .withShort(1)    // stream id
              .withShort(XrootdProtocol.kXR_open)
              .withShort(0)
              .withShort(XrootdProtocol.kXR_delete | XrootdProtocol.kXR_new)
              .withZeros(12) // padding
              .withString("0000C9B4E3768770452E8B1B8E0232584872", UTF_8)
              .build();

        var openMsg = new OpenRequest(buf);

        var openResponse = handler.doOnOpen(ctx, openMsg);

        buf = new ByteBufBuilder()
              .withShort(1)    // stream id
              .withShort(XrootdProtocol.kXR_close)
              .withInt(openResponse.getFileHandle()) // fh
              .build();

        var closeMgs = new CloseRequest(buf);

        handler.doOnClose(ctx, closeMgs);
        waitToComplete();

        verify(stageRequest).completed(any());
    }

    @Test
    public void testStatByOpenFileStage() throws XrootdException, IOException, InterruptedException {

        var flushRequest = mockedFlushRequest();

        var buf = new ByteBufBuilder()
              .withShort(1)    // stream id
              .withShort(XrootdProtocol.kXR_open)
              .withShort(0)
              .withShort(XrootdProtocol.kXR_open_read | XrootdProtocol.kXR_retstat)
              .withZeros(12) // padding
              .withString("0000C9B4E3768770452E8B1B8E0232584872", UTF_8)
              .build();

        var openMsg = new OpenRequest(buf);

        var openResponse = handler.doOnOpen(ctx, openMsg);

        assertNotNull("file status is not returned", openResponse.getFileStatus());

        buf = new ByteBufBuilder()
              .withShort(1)    // stream id
              .withShort(XrootdProtocol.kXR_stat)
              .withByte(0)     // opts
              .withZeros(11)
              .withInt(openResponse.getFileHandle()) // fh
              .withString("", UTF_8) // path
              .build();

        var statMgs = new StatRequest(buf);
        handler.doOnStat(ctx, statMgs);
    }

    @Test(expected = XrootdException.class)
    public void testStatWithoutOpen() throws XrootdException, IOException, InterruptedException {

        var flushRequest = mockedFlushRequest();

        var buf = new ByteBufBuilder()
              .withShort(1)    // stream id
              .withShort(XrootdProtocol.kXR_stat)
              .withByte(0)     // opts
              .withZeros(11)
              .withInt(0) // fh
              .withString("", UTF_8) // path
              .build();

        var statMgs = new StatRequest(buf);
        handler.doOnStat(ctx, statMgs);
    }

    @Test
    public void testStatByPath() throws XrootdException, IOException, InterruptedException {

        var flushRequest = mockedFlushRequest();

        var buf = new ByteBufBuilder()
              .withShort(1)    // stream id
              .withShort(XrootdProtocol.kXR_open)
              .withShort(0)
              .withShort(XrootdProtocol.kXR_open_read)
              .withZeros(12) // padding
              .withString("0000C9B4E3768770452E8B1B8E0232584872", UTF_8)
              .build();

        var openMsg = new OpenRequest(buf);

        var openResponse = handler.doOnOpen(ctx, openMsg);

        buf = new ByteBufBuilder()
              .withShort(1)    // stream id
              .withShort(XrootdProtocol.kXR_stat)
              .withByte(0)     // opts
              .withZeros(11)
              .withInt(-1) // fh
              .withString("0000C9B4E3768770452E8B1B8E0232584872", UTF_8) // path
              .build();

        var statMgs = new StatRequest(buf);
        handler.doOnStat(ctx, statMgs);
    }

    @Test(expected = XrootdException.class)
    public void testStatByInvalidPath() throws XrootdException, IOException, InterruptedException {

        var flushRequest = mockedFlushRequest();

        var buf = new ByteBufBuilder()
              .withShort(1)    // stream id
              .withShort(XrootdProtocol.kXR_open)
              .withShort(0)
              .withShort(XrootdProtocol.kXR_open_read)
              .withZeros(12) // padding
              .withString("0000C9B4E3768770452E8B1B8E0232584872", UTF_8)
              .build();

        var openMsg = new OpenRequest(buf);

        var openResponse = handler.doOnOpen(ctx, openMsg);

        buf = new ByteBufBuilder()
              .withShort(1)    // stream id
              .withShort(XrootdProtocol.kXR_stat)
              .withByte(0)     // opts
              .withZeros(11)
              .withInt(-1) // fh
              .withString("xxx", UTF_8) // path
              .build();

        var statMgs = new StatRequest(buf);
        handler.doOnStat(ctx, statMgs);
    }

    @Test(expected = XrootdException.class)
    public void testFailErrorPropagationOnWrongRequest() throws XrootdException, IOException {

        var stageRequest = mockedFlushRequest();

        var error = "/error/xxxx?error="
              + Base64.getEncoder().encodeToString("some error".getBytes(UTF_8));

        var buf = new ByteBufBuilder()
              .withShort(1)    // stream id
              .withShort(XrootdProtocol.kXR_query)
              .withShort(XrootdProtocol.kXR_Qopaquf)
              .withInt(-1) // fh, not used
              .withInt(error.length())
              .withZeros(6)
              .withString(error, UTF_8)
              .build();

        var msg = new QueryRequest(buf);

        handler.doOnQuery(ctx, msg);
    }

    @Test(expected = XrootdException.class)
    public void testBogusQueryRequest() throws XrootdException, IOException {

        var stageRequest = mockedFlushRequest();

        var error = "/foo/0000C9B4E3768770452E8B1B8E0232584872?error="
              + Base64.getEncoder().encodeToString("some error".getBytes(UTF_8));

        var buf = new ByteBufBuilder()
              .withShort(1)    // stream id
              .withShort(XrootdProtocol.kXR_query)
              .withShort(XrootdProtocol.kXR_Qopaquf)
              .withInt(-1) // fh, not used
              .withInt(error.length())
              .withZeros(6)
              .withString(error, UTF_8)
              .build();

        var msg = new QueryRequest(buf);

        handler.doOnQuery(ctx, msg);
    }

    @Test
    public void testErrorPropagation() throws XrootdException, IOException {

        var stageRequest = mockedFlushRequest();

        var error = "/error/0000C9B4E3768770452E8B1B8E0232584872?error="
              + Base64.getEncoder().encodeToString("some error".getBytes(UTF_8));

        var buf = new ByteBufBuilder()
              .withShort(1)    // stream id
              .withShort(XrootdProtocol.kXR_query)
              .withShort(XrootdProtocol.kXR_Qopaquf)
              .withInt(-1) // fh, not used
              .withInt(error.length())
              .withZeros(6)
              .withString(error, UTF_8)
              .build();

        var msg = new QueryRequest(buf);

        handler.doOnQuery(ctx, msg);
        verify(stageRequest).failed(anyInt(), any());
    }

    @Test
    public void testCompletePropagationOnSuccess() throws XrootdException, IOException {

        var stageRequest = mockedFlushRequest();

        var success = "/success/0000C9B4E3768770452E8B1B8E0232584872?archiveid=31415926";

        var buf = new ByteBufBuilder()
              .withShort(1)    // stream id
              .withShort(XrootdProtocol.kXR_query)
              .withShort(XrootdProtocol.kXR_Qopaquf)
              .withInt(-1) // fh, not used
              .withInt(success.length())
              .withZeros(6)
              .withString(success, UTF_8)
              .build();

        var msg = new QueryRequest(buf);

        handler.doOnQuery(ctx, msg);
        var expectedUri = Set.of(URI.create("cta://test/0000C9B4E3768770452E8B1B8E0232584872?archiveid=31415926"));
        verify(stageRequest).completed(expectedUri);
    }

    @Test(expected = XrootdException.class)
    public void testFailOnMissingId() throws XrootdException, IOException {

        var stageRequest = mockedFlushRequest();

        var success = "/success/0000C9B4E3768770452E8B1B8E0232584872?foo=31415926";

        var buf = new ByteBufBuilder()
              .withShort(1)    // stream id
              .withShort(XrootdProtocol.kXR_query)
              .withShort(XrootdProtocol.kXR_Qopaquf)
              .withInt(-1) // fh, not used
              .withInt(success.length())
              .withZeros(6)
              .withString(success, UTF_8)
              .build();

        var msg = new QueryRequest(buf);

        handler.doOnQuery(ctx, msg);
    }

    void waitToComplete() {
        try {
            waitForComplete.get(1, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            fail("Request not complete");
        }
    }

    private StageRequest mockedStageRequest() throws IOException {

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

        File f = createTempFile("0000C9B4E3768770452E8B1B8E0232584872", "-stage");
        f.delete();
        f.deleteOnExit();

        when(request.getReplicaUri()).thenReturn(f.toURI());

        requests.put("0000C9B4E3768770452E8B1B8E0232584872", request);

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

    private FlushRequest mockedFlushRequest() throws IOException {

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

        File f = createTempFile("0000C9B4E3768770452E8B1B8E0232584872", "-flush");
        f.deleteOnExit();

        when(request.getReplicaUri()).thenReturn(f.toURI());

        requests.put("0000C9B4E3768770452E8B1B8E0232584872", request);

        return request;
    }
}