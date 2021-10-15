package org.dcache.nearline.cta;

import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import cta.common.CtaCommon;
import cta.eos.CtaEos;
import cta.eos.CtaEos.Transport;
import java.io.File;
import java.net.URI;
import java.util.Objects;
import org.dcache.cta.rpc.DeleteRequest;
import org.dcache.cta.rpc.FileInfo;
import org.dcache.cta.rpc.RetrieveRequest;
import org.dcache.namespace.FileAttribute;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.cta.rpc.ArchiveRequest;
import org.dcache.pool.nearline.spi.NearlineRequest;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;
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
     * URI used for IO transport. The final URI sent to CTA constructed as
     * &lt;uri&gt;/&lt;pnfsid&gt;
     */
    private final String url;

    /**
     * @param service
     * @param user    user name associated with the requests on the CTA side.
     * @param group   group name associated with the request on the CTA side.
     * @param url     URI used for IO transport.
     */
    public RequestsFactory(String service, String user, String group, String url) {

        Objects.requireNonNull(service, "Service name is Null");
        Objects.requireNonNull(user, "User name is Null");
        Objects.requireNonNull(group, "Group name is Null");
        Objects.requireNonNull(url, "IO url is Null");

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

    public ArchiveRequest valueOf(FlushRequest request) {

        FileAttributes dcacheFileAttrs = request.getFileAttributes();

        Transport transport = getTransport(request);

        var checksumBuilder = CtaCommon.ChecksumBlob.newBuilder();
        if (dcacheFileAttrs.isDefined(FileAttribute.CHECKSUM)) {
            dcacheFileAttrs.getChecksums().forEach(cs -> {

                      // TODO: add other types as well.
                      var type = cs.getType();
                      if (type == ChecksumType.ADLER32) {
                          checksumBuilder.addCs(
                                CtaCommon.ChecksumBlob.Checksum.newBuilder()
                                      .setType(CtaCommon.ChecksumBlob.Checksum.Type.ADLER32)
                                      .setValue(ByteString.copyFrom(
                                            BaseEncoding.base16().lowerCase().decode(cs.getValue())
                                      ))
                                      .build()
                          );
                      }
                  }
            );
        }

        var ctaFileInfo = FileInfo.newBuilder()
              .setSize(dcacheFileAttrs.getSize())
              .setFid(dcacheFileAttrs.getPnfsId().toString())
              .setStorageClass(dcacheFileAttrs.getStorageClass() + "@" + dcacheFileAttrs.getHsm())
              .setCsb(checksumBuilder.build())
              .build();

        return ArchiveRequest.newBuilder()
              .setInstance(instance)
              .setCli(client)
              .setTransport(transport)
              .setFile(ctaFileInfo)
              .build();
    }

    public DeleteRequest valueOf(RemoveRequest request) {

        // we expect uri in form: cta://cta/<pnfsid>/archiveid
        var uri = request.getUri();
        File asPath = new File(uri.getPath());

        String pnfsid = asPath.getParentFile().getName();
        long archiveId = Long.parseLong(asPath.getName());

        var transport = getTransport(request);

        var ctaFileInfo = FileInfo.newBuilder()
              .setFid(pnfsid)
              .build();

        return DeleteRequest.newBuilder()
              .setInstance(instance)
              .setCli(client)
              .setTransport(transport)
              .setFile(ctaFileInfo)
              .setArchiveId(archiveId)
              .build();
    }

    public RetrieveRequest valueOf(StageRequest request) {

        FileAttributes dcacheFileAttrs = request.getFileAttributes();

        // we expect uri in form: cta://cta/<pnfsid>/archiveid

        var uri = dcacheFileAttrs.getStorageInfo().locations().get(0);
        File asPath = new File(uri.getPath());

        long archiveId = Long.parseLong(asPath.getName());

        var transport = getTransport(request);

        var ctaFileInfo = FileInfo.newBuilder()
              .setSize(dcacheFileAttrs.getSize())
              .setFid(dcacheFileAttrs.getPnfsId().toString())
              .setStorageClass(dcacheFileAttrs.getStorageClass() + "@" + dcacheFileAttrs.getHsm())
              .build();

        return RetrieveRequest.newBuilder()
              .setInstance(instance)
              .setCli(client)
              .setTransport(transport)
              .setFile(ctaFileInfo)
              .setArchiveId(archiveId)
              .build();
    }

    private Transport getTransport(NearlineRequest request) {
        // REVISIT:
        String reporterUrl = "eosQuery://" + url + "/status/" + request.getId() + "/";
        String errorReporter = "eosQuery://" + url + "/error/" + request.getId() + "/";

        return Transport.newBuilder()
              .setDstUrl("root://" + url + "/" + request.getId())
              .setErrorReportUrl(errorReporter)
              .setReportUrl(reporterUrl)
              .build();
    }
}
