package org.dcache.nearline.cta;

import static org.junit.Assert.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import com.google.protobuf.ByteString;
import cta.common.CtaCommon.ChecksumBlob.Checksum.Type;
import diskCacheV111.vehicles.GenericStorageInfo;
import java.net.URI;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.vehicles.FileAttributes;
import org.junit.Test;

public class RequestsFactoryTest {

    @Test
    public void testArchive() {

        var rf = new RequestsFactory("dcache", "foo", "bar", "https://localhost");

        byte[] csum = new byte[]{0x11, 0x22, 0x33, 0x44};

        var fileAttrs = FileAttributes.of()
              .pnfsId("00001234567812345678")
              .checksum(new Checksum(ChecksumType.ADLER32, csum))
              .size(9876543210L)
              .storageClass("a:b")
              .hsm("cta")
              .build();

        var flushRequest = mock(FlushRequest.class);
        given(flushRequest.getFileAttributes()).willReturn(fileAttrs);

        var achriveRequest = rf.valueOf(flushRequest);

        assertEquals(fileAttrs.getStorageClass() + "@" + fileAttrs.getHsm(),
              achriveRequest.getFile().getStorageClass());
        assertEquals(Type.ADLER32, achriveRequest.getFile().getCsb().getCs(0).getType());
        assertEquals(ByteString.copyFrom(csum),
              achriveRequest.getFile().getCsb().getCs(0).getValue());
        assertEquals(fileAttrs.getSize(), achriveRequest.getFile().getSize());
        assertEquals(fileAttrs.getPnfsId().toString(), achriveRequest.getFile().getFid());
    }

    @Test
    public void testDelete() {

        var rf = new RequestsFactory("dcache", "foo", "bar", "https://localhost");

        var pnfsid = "0000C9B4E3768770452E8B1B8E0232584872";
        var archiveId = 12345L;
        var uri = URI.create(String.format("cta://cta/%s/%d", pnfsid, archiveId));

        var removeRequest = mock(RemoveRequest.class);
        given(removeRequest.getUri()).willReturn(uri);

        var deleteRequest = rf.valueOf(removeRequest);

        assertEquals(pnfsid, deleteRequest.getFile().getFid());
        assertEquals(archiveId, deleteRequest.getArchiveId());
    }

    @Test
    public void testRetrieve() {

        var rf = new RequestsFactory("dcache", "foo", "bar", "https://localhost");

        var pnfsid = "0000C9B4E3768770452E8B1B8E0232584872";
        var archiveId = 12345L;
        var uri = URI.create(String.format("cta://cta/%s/%d", pnfsid, archiveId));

        var storageInfo = GenericStorageInfo.valueOf("a:b@z", "*");
        storageInfo.addLocation(uri);

        var fileAttrs = FileAttributes.of()
              .pnfsId(pnfsid)
              .size(9876543210L)
              .storageClass("a:b")
              .hsm("cta")
              .storageInfo(storageInfo)
              .build();

        var flushRequest = mock(StageRequest.class);
        given(flushRequest.getFileAttributes()).willReturn(fileAttrs);

        var retrieveRequest = rf.valueOf(flushRequest);

        assertEquals(fileAttrs.getStorageClass() + "@" + fileAttrs.getHsm(),
              retrieveRequest.getFile().getStorageClass());
        assertEquals(fileAttrs.getSize(), retrieveRequest.getFile().getSize());
        assertEquals(fileAttrs.getPnfsId().toString(), retrieveRequest.getFile().getFid());
        assertEquals(archiveId, retrieveRequest.getArchiveId());
    }

}