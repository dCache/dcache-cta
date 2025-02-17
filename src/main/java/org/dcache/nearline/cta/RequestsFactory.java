package org.dcache.nearline.cta;

import ch.cern.cta.rpc.Request;
import ch.cern.cta.rpc.Response;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import cta.common.CtaCommon;
import cta.common.CtaCommon.Clock;
import cta.common.CtaCommon.OwnerId;
import cta.eos.CtaEos;
import cta.eos.CtaEos.Metadata;
import cta.eos.CtaEos.Notification;
import cta.eos.CtaEos.Transport;
import cta.eos.CtaEos.Workflow;
import cta.eos.CtaEos.Workflow.EventType;
import java.io.File;
import java.net.InetAddress;
import java.time.Instant;
import java.util.Objects;
import org.dcache.namespace.FileAttribute;
import org.dcache.pool.nearline.spi.FlushRequest;
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
            .setSec(CtaCommon.Security.newBuilder()
                    .setName(service)
                    .setProt("none")
                    .setApp("dCache-CTA-" + CtaNearlineStorageProvider.VERSION)
                    .setHost(InetAddress.getLoopbackAddress().getCanonicalHostName())
                    .build()
            )
            .build();

        this.transportProvider = transportProvider;
    }

    public Request getCreateRequest(FileAttributes dcacheFileAttrs) {

        var id = dcacheFileAttrs.getPnfsId().toString();
        var md = Metadata.newBuilder()
              .setSize(dcacheFileAttrs.getSize())
              .setDiskFileId(id)
              .setStorageClass(dcacheFileAttrs.getStorageClass() + "@" + dcacheFileAttrs.getHsm())
              .setOwner(
                    OwnerId.newBuilder()
                          .setUid(1)
                          .setGid(1)
                          .build()
              )
              .setBtime(Clock.newBuilder()
                    .setSec(dcacheFileAttrs.isDefined(FileAttribute.CREATION_TIME) ? dcacheFileAttrs.getCreationTime() / 1000 : Instant.now().getEpochSecond())
                    .build())
              .setLpath("/" + id)
              .build();

        var notification = Notification.newBuilder()
              .setCli(client)
              .setFile(md)
              .setWf(Workflow.newBuilder()
                    .setInstance(instance)
                    .setEvent(EventType.CREATE)
                    .build())
              .build();

        return Request.newBuilder()
              .setNotification(notification)
              .build();
    }


    public Request getStoreRequest(FlushRequest request, long archiveId) {

        FileAttributes dcacheFileAttrs = request.getFileAttributes();

        var id = dcacheFileAttrs.getPnfsId().toString();
        Transport transport = transportProvider.getTransport(id, archiveId);

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

        var md = Metadata.newBuilder()
              .setArchiveFileId(archiveId)
              .setSize(dcacheFileAttrs.getSize())
              .setDiskFileId(id)
              .setStorageClass(dcacheFileAttrs.getStorageClass() + "@" + dcacheFileAttrs.getHsm())
              .setOwner(
                    OwnerId.newBuilder()
                          .setUid(1)
                          .setGid(1)
                          .build()
              )
              .setBtime(Clock.newBuilder()
                    .setSec(dcacheFileAttrs.isDefined(FileAttribute.CREATION_TIME) ? dcacheFileAttrs.getCreationTime() / 1000 : Instant.now().getEpochSecond())
                    .build())
              .setCsb(checksumBuilder.build())
              .setLpath("/" + id)
              .build();

        // CTA ignores URL provided in the transport, so we need to create a new instance
        var customInstance = CtaCommon.Service.newBuilder(instance)
                .setUrl(transport.getDstUrl())
                .build();

        var notification = Notification.newBuilder()
              .setCli(client)
              .setTransport(transport)
              .setFile(md)
              .setWf(Workflow.newBuilder()
                    .setInstance(customInstance)
                    .setEvent(EventType.CLOSEW)
                    .build())
              .build();

        return Request.newBuilder()
              .setNotification(notification)
              .build();
    }

    public Request getAbortStoreRequest(Request request, Response response) {

        // we expect uri in form: cta://cta/<pnfsid>?archiveid=xxx
        var notification = Notification.newBuilder()
                .setCli(client)
                .setFile(Metadata.newBuilder(request.getNotification().getFile())
                        .putXattr(CtaConstants.ATTR_OBJECTSTOE_ID, response.getRequestObjectstoreId())
                        .build()
                )
                .setWf(Workflow.newBuilder()
                        .setInstance(instance)
                        .setEvent(EventType.DELETE)
                        .build())
                .build();

        return Request.newBuilder()
                .setNotification(notification)
                .build();
    }

    public Request getAbortStageRequest(Request request, Response response) {

        // we expect uri in form: cta://cta/<pnfsid>?archiveid=xxx
        var notification = Notification.newBuilder()
                .setCli(client)
                .setFile(Metadata.newBuilder(request.getNotification().getFile())
                        .putXattr(CtaConstants.ATTR_OBJECTSTOE_ID, response.getRequestObjectstoreId())
                        .build()
                )
                .setWf(Workflow.newBuilder()
                        .setInstance(instance)
                        .setEvent(EventType.ABORT_PREPARE)
                        .build())
                .build();

        return Request.newBuilder()
                .setNotification(notification)
                .build();
    }


    public Request getRemoveRequest(RemoveRequest request) {

        // we expect uri in form: cta://cta/<pnfsid>?archiveid=xxx

        var uri = request.getUri();
        var id = new File(uri.getPath()).getName();
        long archiveId = Long.parseLong(uri.getQuery().substring("archiveid=".length()));

        var md = Metadata.newBuilder()
              .setArchiveFileId(archiveId)
              .setDiskFileId(id)
              .setOwner(
                    OwnerId.newBuilder()
                          .setUid(1)
                          .setGid(1)
                          .build()
              )
              .setLpath("/" + id)
              .build();

        var notification = Notification.newBuilder()
              .setCli(client)
              .setFile(md)
              .setWf(Workflow.newBuilder()
                    .setInstance(instance)
                    .setEvent(EventType.DELETE)
                    .build())
              .build();

        return Request.newBuilder()
              .setNotification(notification)
              .build();
    }

    public Request getStageRequest(StageRequest request) {

        FileAttributes dcacheFileAttrs = request.getFileAttributes();

        // we expect uri in form: cta://cta/pnfsid/?archiveid=xxxx

        var uri = dcacheFileAttrs.getStorageInfo().locations().get(0);
        var id = dcacheFileAttrs.getPnfsId().toString();
        long archiveId = Long.parseLong(uri.getQuery().substring("archiveid=".length()));

        var transport = transportProvider.getTransport(id, archiveId);

        var md = Metadata.newBuilder()
              .setArchiveFileId(archiveId)
              .setSize(dcacheFileAttrs.getSize())
              .setDiskFileId(id)
              .setStorageClass(dcacheFileAttrs.getStorageClass() + "@" + dcacheFileAttrs.getHsm())
              .setOwner(
                    OwnerId.newBuilder()
                          .setUid(1)
                          .setGid(1)
                          .build()
              )
              .setLpath("/" + id)
              .build();

        var notification = Notification.newBuilder()
              .setCli(client)
              .setTransport(transport)
              .setFile(md)
              .setWf(Workflow.newBuilder()
                    .setInstance(instance)
                    .setEvent(EventType.PREPARE)
                    .build())
              .build();

        return Request.newBuilder()
              .setNotification(notification)
              .build();
    }
}