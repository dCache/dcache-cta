package org.dcache.nearline.cta.xrootd;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import io.netty.channel.ChannelHandlerContext;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.dcache.pool.nearline.spi.NearlineRequest;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.protocol.messages.OpenRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

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

    @After
    public void tearDown() throws Exception {
    }


    @Test(expected = XrootdException.class)
    public void testOpenWithoutRequests() throws XrootdException {

        var buf = new ByteBufBuilder()
              .withShort(1)    // stream id
              .withShort(XrootdProtocol.kXR_open)
              .withZeros(16) // padding
              .withString("/foo", StandardCharsets.UTF_8)
              .withShort(0)
              .withShort(XrootdProtocol.kXR_open_read)
              .build();

        OpenRequest msg = new OpenRequest(buf);

        handler.doOnOpen(ctx, msg);
    }
}