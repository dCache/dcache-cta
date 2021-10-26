package org.dcache.nearline.cta;

import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import cta.common.CtaCommon;
import cta.eos.CtaEos;
import cta.eos.CtaEos.Transport;
import java.io.File;
import java.util.Objects;
import org.dcache.cta.rpc.DeleteRequest;
import org.dcache.cta.rpc.FileInfo;
import org.dcache.cta.rpc.RetrieveRequest;
import org.dcache.namespace.FileAttribute;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.cta.rpc.ArchiveRequest;
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
     * {@link CtaTransportProvider} used to generate IO, error and success report urls.
     */
    private final CtaTransportProvider transportProvider;

    /**
     * @param service
     * @param user              user name associated with the requests on the CTA side.
     * @param group             group name associated with the request on the CTA side.
     * @param transportProvider transportprovider to generate IO, error and success report urls.
     */
    public RequestsFactory(String service, String user, String group,
          CtaTransportProvider transportProvider) {

        Objects.requireNonNull(service, "Service name is Null");
        Objects.requireNonNull(user, "User name is Null");
        Objects.requireNonNull(group, "Group name is Null");
        Objects.requireNonNull(transportProvider, "Transport provider  url is Null");

        instance = CtaCommon.Service.newBuilder()
              .setName(service)
              .build();

        client = CtaEos.Client.newBuilder()
              .setUser(CtaCommon.RequesterId.newBuilder()
                    .setUsername(user)
                    .setGroupname(group)
                    .build())
              .build();

        this.transportProvider = transportProvider;
    }

    public ArchiveRequest valueOf(FlushRequest request) {

        FileAttributes dcacheFileAttrs = request.getFileAttributes();

        var id = dcacheFileAttrs.getPnfsId().toString();
        Transport transport = transportProvider.getTransport(id);

        var checksumBuilder = CtaCommon.ChecksumBlob.newBuilder();
        if (dcacheFileAttrs.isDefined(FileAttribute.CHECKSUM)) {
            dcacheFileAttrs.getChecksums().forEach(cs -> {

                      // TODO: add other types as well.
                      var type = cs.getType();
                      if (type == ChecksumType.ADLER32) {

                          // CTA expects the sum as short encoded in little-endian format
                          var dcacheSum = BaseEncoding.base16().lowerCase().decode(cs.getValue());
                          var ctaSum = new byte[4];
                          ctaSum[0] = dcacheSum[3];
                          ctaSum[1] = dcacheSum[2];
                          ctaSum[2] = dcacheSum[1];
                          ctaSum[3] = dcacheSum[0];

                          checksumBuilder.addCs(
                                CtaCommon.ChecksumBlob.Checksum.newBuilder()
                                      .setType(CtaCommon.ChecksumBlob.Checksum.Type.ADLER32)
                                      .setValue(ByteString.copyFrom(ctaSum))
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

        var id = asPath.getParentFile().getName();
        long archiveId = Long.parseLong(asPath.getName());

        var ctaFileInfo = FileInfo.newBuilder()
              .setFid(id)
              .build();

        return DeleteRequest.newBuilder()
              .setInstance(instance)
              .setCli(client)
              .setFile(ctaFileInfo)
              .setArchiveId(archiveId)
              .build();
    }

    public RetrieveRequest valueOf(StageRequest request) {

        FileAttributes dcacheFileAttrs = request.getFileAttributes();

        // we expect uri in form: cta://cta/pnfsid/?archiveid=xxxx

        var uri = dcacheFileAttrs.getStorageInfo().locations().get(0);
        var id = dcacheFileAttrs.getPnfsId().toString();
        long archiveId = Long.parseLong(uri.getQuery().substring("archiveid=".length()));

        var transport = transportProvider.getTransport(id);

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

}
