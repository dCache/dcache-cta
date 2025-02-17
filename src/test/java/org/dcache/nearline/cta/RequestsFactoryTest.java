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
        public Transport getTransport(String id, long archiveId) {
            String reporterUrl = "eosQuery://localhost/success/" + id + "?archiveid=" + archiveId;
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

        var achriveRequest = rf.getStoreRequest(flushRequest, 123);

        assertEquals(fileAttrs.getStorageClass() + "@" + fileAttrs.getHsm(),
              achriveRequest.getNotification().getFile().getStorageClass());
        assertEquals(Type.ADLER32, achriveRequest.getNotification().getFile().getCsb().getCs(0).getType());
        assertEquals(ByteString.copyFrom(csumCta),
              achriveRequest.getNotification().getFile().getCsb().getCs(0).getValue());
        assertEquals(fileAttrs.getSize(), achriveRequest.getNotification().getFile().getSize());
        assertEquals(fileAttrs.getPnfsId().toString(), achriveRequest.getNotification().getFile().getDiskFileId());

        assertFalse(achriveRequest.getNotification().getWf().getInstance().getName().isEmpty());
        assertFalse(achriveRequest.getNotification().getCli().getUser().getUsername().isEmpty());
        assertFalse(achriveRequest.getNotification().getCli().getUser().getGroupname().isEmpty());
        assertFalse(achriveRequest.getNotification().getFile().getOwner().getUid() == 0);
        assertFalse(achriveRequest.getNotification().getFile().getOwner().getGid() == 0);
        assertFalse(achriveRequest.getNotification().getFile().getLpath().isEmpty());
    }

    @Test
    public void testDelete() {

        var rf = new RequestsFactory("dcache", "foo", "bar", transportProvider);

        var pnfsid = "0000C9B4E3768770452E8B1B8E0232584872";
        var archiveId = 12345L;
        var uri = URI.create(String.format("cta://cta/%s?archiveid=%d", pnfsid, archiveId));

        var removeRequest = mock(RemoveRequest.class);
        given(removeRequest.getUri()).willReturn(uri);

        var deleteRequest = rf.getRemoveRequest(removeRequest);

        assertEquals(pnfsid, deleteRequest.getNotification().getFile().getDiskFileId());
        assertEquals(archiveId, deleteRequest.getNotification().getFile().getArchiveFileId());

        assertFalse(deleteRequest.getNotification().getWf().getInstance().getName().isEmpty());
        assertFalse(deleteRequest.getNotification().getCli().getUser().getUsername().isEmpty());
        assertFalse(deleteRequest.getNotification().getCli().getUser().getGroupname().isEmpty());
        assertFalse(deleteRequest.getNotification().getFile().getOwner().getUid() == 0);
        assertFalse(deleteRequest.getNotification().getFile().getOwner().getGid() == 0);
        assertFalse(deleteRequest.getNotification().getFile().getLpath().isEmpty());
        assertFalse(deleteRequest.getNotification().getFile().getArchiveFileId() == 0L);

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

        var retrieveRequest = rf.getStageRequest(flushRequest);

        assertEquals(fileAttrs.getStorageClass() + "@" + fileAttrs.getHsm(),
              retrieveRequest.getNotification().getFile().getStorageClass());
        assertEquals(fileAttrs.getSize(), retrieveRequest.getNotification().getFile().getSize());
        assertEquals(fileAttrs.getPnfsId().toString(), retrieveRequest.getNotification().getFile().getDiskFileId());
        assertEquals(archiveId, retrieveRequest.getNotification().getFile().getArchiveFileId());

        assertFalse(retrieveRequest.getNotification().getWf().getInstance().getName().isEmpty());
        assertFalse(retrieveRequest.getNotification().getCli().getUser().getUsername().isEmpty());
        assertFalse(retrieveRequest.getNotification().getCli().getUser().getGroupname().isEmpty());
        assertFalse(retrieveRequest.getNotification().getFile().getOwner().getUid() == 0);
        assertFalse(retrieveRequest.getNotification().getFile().getOwner().getGid() == 0);
        assertFalse(retrieveRequest.getNotification().getFile().getLpath().isEmpty());
        assertFalse(retrieveRequest.getNotification().getFile().getArchiveFileId() == 0L);
    }

}