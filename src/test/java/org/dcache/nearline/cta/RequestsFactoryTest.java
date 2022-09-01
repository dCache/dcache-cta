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
              .creationTime(System.currentTimeMillis())
              .hsm("cta")
              .build();

        var flushRequest = mock(FlushRequest.class);
        given(flushRequest.getFileAttributes()).willReturn(fileAttrs);

        var achriveRequest = rf.valueOf(flushRequest, 123);

        assertEquals(fileAttrs.getStorageClass() + "@" + fileAttrs.getHsm(),
              achriveRequest.getMd().getFile().getStorageClass());
        assertEquals(Type.ADLER32, achriveRequest.getMd().getFile().getCsb().getCs(0).getType());
        assertEquals(ByteString.copyFrom(csumCta),
              achriveRequest.getMd().getFile().getCsb().getCs(0).getValue());
        assertEquals(fileAttrs.getSize(), achriveRequest.getMd().getFile().getSize());
        assertEquals(fileAttrs.getPnfsId().toString(), achriveRequest.getMd().getFile().getDiskFileId());

        assertFalse(achriveRequest.getMd().getWf().getInstance().getName().isEmpty());
        assertFalse(achriveRequest.getMd().getCli().getUser().getUsername().isEmpty());
        assertFalse(achriveRequest.getMd().getCli().getUser().getGroupname().isEmpty());
        assertFalse(achriveRequest.getMd().getFile().getOwner().getUid() == 0);
        assertFalse(achriveRequest.getMd().getFile().getOwner().getGid() == 0);
        assertFalse(achriveRequest.getMd().getFile().getLpath().isEmpty());
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

        assertEquals(pnfsid, deleteRequest.getMd().getFile().getDiskFileId());
        assertEquals(archiveId, deleteRequest.getMd().getFile().getArchiveFileId());

        assertFalse(deleteRequest.getMd().getWf().getInstance().getName().isEmpty());
        assertFalse(deleteRequest.getMd().getCli().getUser().getUsername().isEmpty());
        assertFalse(deleteRequest.getMd().getCli().getUser().getGroupname().isEmpty());
        assertFalse(deleteRequest.getMd().getFile().getOwner().getUid() == 0);
        assertFalse(deleteRequest.getMd().getFile().getOwner().getGid() == 0);
        assertFalse(deleteRequest.getMd().getFile().getLpath().isEmpty());
        assertFalse(deleteRequest.getMd().getFile().getArchiveFileId() == 0L);

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
              .creationTime(System.currentTimeMillis())
              .build();

        var flushRequest = mock(StageRequest.class);
        given(flushRequest.getFileAttributes()).willReturn(fileAttrs);

        var retrieveRequest = rf.valueOf(flushRequest);

        assertEquals(fileAttrs.getStorageClass() + "@" + fileAttrs.getHsm(),
              retrieveRequest.getMd().getFile().getStorageClass());
        assertEquals(fileAttrs.getSize(), retrieveRequest.getMd().getFile().getSize());
        assertEquals(fileAttrs.getPnfsId().toString(), retrieveRequest.getMd().getFile().getDiskFileId());
        assertEquals(archiveId, retrieveRequest.getMd().getFile().getArchiveFileId());

        assertFalse(retrieveRequest.getMd().getWf().getInstance().getName().isEmpty());
        assertFalse(retrieveRequest.getMd().getCli().getUser().getUsername().isEmpty());
        assertFalse(retrieveRequest.getMd().getCli().getUser().getGroupname().isEmpty());
        assertFalse(retrieveRequest.getMd().getFile().getOwner().getUid() == 0);
        assertFalse(retrieveRequest.getMd().getFile().getOwner().getGid() == 0);
        assertFalse(retrieveRequest.getMd().getFile().getLpath().isEmpty());
        assertFalse(retrieveRequest.getMd().getFile().getArchiveFileId() == 0L);
    }

}