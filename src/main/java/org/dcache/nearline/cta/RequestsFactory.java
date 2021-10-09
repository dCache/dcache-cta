package org.dcache.nearline.cta;

import com.google.protobuf.ByteString;
import cta.common.CtaCommon;
import cta.eos.CtaEos;
import java.nio.charset.StandardCharsets;
import org.dcache.cta.rpc.FileInfo;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.cta.rpc.ArchiveRequest;
import org.dcache.util.ChecksumType;
import org.dcache.vehicles.FileAttributes;

public class RequestsFactory {

    /**
     * Client instance id used by CTA. Typically, represents dCache instance.
     */
    private final CtaCommon.Service instance;

    /**
     * User associated with CTA request. Typically, used by CTA by tape allocation.
     */
    private final CtaEos.Client client;

    /**
     * URI used for IO transport. The final URI sent to CTA constructed as &lt;uri&gt;/&lt;pnfsid&gt;
     */
    private final String url;

    /**
     *
     * @param service
     * @param user user name associated with the requests on the CTA side.
     * @param group group name associated with the request on the CTA side.
     * @param url  URI used for IO transport.
     */
    public RequestsFactory(String service, String user, String group, String url) {

        instance = CtaCommon.Service.newBuilder()
              .setName(service)
              .build();

        client = CtaEos.Client.newBuilder()
              .setUser(CtaCommon.RequesterId.newBuilder()
                    .setUsername(user)
                    .setGroupname(group)
                    .build())
              .build();

        this.url = url;
    }

    public ArchiveRequest valueOf(FlushRequest flushRequest) {

        FileAttributes dcacheFileAttrs = flushRequest.getFileAttributes();

        // REVISIT:
        String reporter = String.format("eosQuery://%s//%s",
               url + "/" + dcacheFileAttrs.getPnfsId());

        var transport = CtaEos.Transport.newBuilder()
              .setDstUrl(url + "/" + dcacheFileAttrs.getPnfsId())
              .setErrorReportUrl(reporter)
              .setReportUrl(reporter)
              .build();

        var checksumBuilder = CtaCommon.ChecksumBlob.newBuilder();
        dcacheFileAttrs.getChecksums().forEach(cs -> {

                  // TODO: add other types as well.
                  var type = cs.getType();
                  if (type == ChecksumType.ADLER32) {
                      checksumBuilder.addCs(
                            CtaCommon.ChecksumBlob.Checksum.newBuilder()
                                  .setType(CtaCommon.ChecksumBlob.Checksum.Type.ADLER32)
                                  .setValue(ByteString.copyFrom(cs.getValue(), StandardCharsets.US_ASCII))
                                  .build()
                      );
                  }
              }
        );

        var ctaFileInfo = FileInfo.newBuilder()
              .setSize(dcacheFileAttrs.getSize())
              .setFid(dcacheFileAttrs.getPnfsId().toString())
              .setStorageClass(dcacheFileAttrs.getStorageClass())
              .setCsb(checksumBuilder.build())
              .build();

        var archiveArgs = ArchiveRequest.newBuilder()
              .setInstance(instance)
              .setCli(client)
              .setTransport(transport)
              .setFile(ctaFileInfo)
              .build();

        return archiveArgs;
    }

}
