package org.dcache.nearline.cta.xrootd;

import static java.io.File.createTempFile;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import diskCacheV111.vehicles.GenericStorageInfo;
import io.netty.channel.ChannelHandlerContext;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.NearlineRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.vehicles.FileAttributes;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.protocol.messages.CloseRequest;
import org.dcache.xrootd.protocol.messages.OpenRequest;
import org.junit.Before;
import org.junit.Test;

public class DataServerHandlerTest {


    private DataServerHandler handler;
    private ConcurrentMap<String, NearlineRequest> requests;
    private ChannelHandlerContext ctx;

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
              .withString("0000C9B4E3768770452E8B1B8E0232584872", StandardCharsets.UTF_8)
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
              .withString("0000C9B4E3768770452E8B1B8E0232584872", StandardCharsets.UTF_8)
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
              .withString("0000C9B4E3768770452E8B1B8E0232584872", StandardCharsets.UTF_8)
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
              .withString("0000C9B4E3768770452E8B1B8E0232584872", StandardCharsets.UTF_8)
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