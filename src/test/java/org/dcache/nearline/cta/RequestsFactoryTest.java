package org.dcache.nearline.cta;

import static org.junit.Assert.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import com.google.protobuf.ByteString;
import cta.common.CtaCommon.ChecksumBlob.Checksum.Type;
import cta.eos.CtaEos.Transport;
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


    private final CtaTransportProvider transportProvider = new CtaTransportProvider() {
        @Override
        public Transport getTransport(String id) {
            String reporterUrl = "eosQuery://localhost/success/" + id;
            String errorReporter = "eosQuery://localhost/error/" + id + "?error=";

            return Transport.newBuilder()
                  .setDstUrl("root://localhost/" + id)
                  .setErrorReportUrl(errorReporter)
                  .setReportUrl(reporterUrl)
                  .build();
        }
    };

    @Test
    public void testArchive() {

        var rf = new RequestsFactory("dcache", "foo", "bar", transportProvider);

        byte[] csum = new byte[]{0x11, 0x22, 0x33, 0x44};
        byte[] csumCta = new byte[]{0x44, 0x33, 0x22, 0x11};

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
        assertEquals(ByteString.copyFrom(csumCta),
              achriveRequest.getFile().getCsb().getCs(0).getValue());
        assertEquals(fileAttrs.getSize(), achriveRequest.getFile().getSize());
        assertEquals(fileAttrs.getPnfsId().toString(), achriveRequest.getFile().getFid());

        assertFalse(achriveRequest.getInstance().getName().isEmpty());
        assertFalse(achriveRequest.getCli().getUser().getUsername().isEmpty());
        assertFalse(achriveRequest.getCli().getUser().getGroupname().isEmpty());
        assertFalse(achriveRequest.getFile().getUid() == 0);
        assertFalse(achriveRequest.getFile().getGid() == 0);
        assertFalse(achriveRequest.getFile().getPath().isEmpty());
    }

    @Test
    public void testDelete() {

        var rf = new RequestsFactory("dcache", "foo", "bar", transportProvider);

        var pnfsid = "0000C9B4E3768770452E8B1B8E0232584872";
        var archiveId = 12345L;
        var uri = URI.create(String.format("cta://cta/%s?archiveid=%d", pnfsid, archiveId));

        var removeRequest = mock(RemoveRequest.class);
        given(removeRequest.getUri()).willReturn(uri);

        var deleteRequest = rf.valueOf(removeRequest);

        assertEquals(pnfsid, deleteRequest.getFile().getFid());
        assertEquals(archiveId, deleteRequest.getArchiveId());

        assertFalse(deleteRequest.getInstance().getName().isEmpty());
        assertFalse(deleteRequest.getCli().getUser().getUsername().isEmpty());
        assertFalse(deleteRequest.getCli().getUser().getGroupname().isEmpty());
        assertFalse(deleteRequest.getFile().getUid() == 0);
        assertFalse(deleteRequest.getFile().getGid() == 0);
        assertFalse(deleteRequest.getFile().getPath().isEmpty());
        assertFalse(deleteRequest.getArchiveId() == 0L);

    }

    @Test
    public void testRetrieve() {

        var rf = new RequestsFactory("dcache", "foo", "bar", transportProvider);

        var pnfsid = "0000C9B4E3768770452E8B1B8E0232584872";
        var archiveId = 12345L;
        var uri = URI.create(String.format("cta://cta/%s?archiveid=%d", pnfsid, archiveId));

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

        assertFalse(retrieveRequest.getInstance().getName().isEmpty());
        assertFalse(retrieveRequest.getCli().getUser().getUsername().isEmpty());
        assertFalse(retrieveRequest.getCli().getUser().getGroupname().isEmpty());
        assertFalse(retrieveRequest.getFile().getUid() == 0);
        assertFalse(retrieveRequest.getFile().getGid() == 0);
        assertFalse(retrieveRequest.getFile().getPath().isEmpty());
        assertFalse(retrieveRequest.getArchiveId() == 0L);
    }

}