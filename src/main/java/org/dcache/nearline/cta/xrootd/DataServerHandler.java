/*
 * Copyright (C) 2011-2024 dCache.org <support@dcache.org>
 * <p>
 * This file is part of xrootd4j.
 * <p>
 * xrootd4j is free software: you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 * <p>
 * xrootd4j is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Lesser General Public License along with xrootd4j.  If
 * not, see http://www.gnu.org/licenses/.
 */
package org.dcache.nearline.cta.xrootd;

import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ArgInvalid;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_FileNotOpen;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_IOError;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_NotFound;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_Qopaquf;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_Unsupported;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_isDir;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_other;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_readable;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_writable;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_xset;

import com.google.common.net.InetAddresses;
import diskCacheV111.util.Adler32;
import diskCacheV111.util.CacheException;
import io.netty.channel.ChannelHandlerContext;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
import org.dcache.nearline.cta.PendingRequest;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.NearlineRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.Strings;
import org.dcache.util.TimeUtils;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.core.XrootdProtocolRequestHandler;
import org.dcache.xrootd.core.XrootdSessionIdentifier;
import org.dcache.xrootd.protocol.messages.CloseRequest;
import org.dcache.xrootd.protocol.messages.EndSessionRequest;
import org.dcache.xrootd.protocol.messages.LoginRequest;
import org.dcache.xrootd.protocol.messages.LoginResponse;
import org.dcache.xrootd.protocol.messages.OkResponse;
import org.dcache.xrootd.protocol.messages.OpenRequest;
import org.dcache.xrootd.protocol.messages.OpenResponse;
import org.dcache.xrootd.protocol.messages.PrepareRequest;
import org.dcache.xrootd.protocol.messages.QueryRequest;
import org.dcache.xrootd.protocol.messages.QueryResponse;
import org.dcache.xrootd.protocol.messages.ReadRequest;
import org.dcache.xrootd.protocol.messages.StatRequest;
import org.dcache.xrootd.protocol.messages.StatResponse;
import org.dcache.xrootd.protocol.messages.SyncRequest;
import org.dcache.xrootd.protocol.messages.WriteRequest;
import org.dcache.xrootd.util.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataServerHandler extends XrootdProtocolRequestHandler {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(DataServerHandler.class);

    // expected uri: /success/0000A3D4FAF099E241FC830295D1DAD54DF2?archiveid=89502
    private final static Pattern SUCCESS_URI = Pattern.compile(".*archiveid=(?<archiveid>\\d+).*");

    /**
     * A record that binds open file and request.
     */
    private static class MigrationRequest {

        /**
         * The nearline request associated with this migration.
         */
        private final NearlineRequest request;

        /**
         * File to migrate.
         */
        private final FileChannel fileChannel;

        /**
         * Migration request creation time.
         */
        private final Instant btime = Instant.now();

        private final IoStats ioStat = new IoStats();

        public MigrationRequest(NearlineRequest request, FileChannel fileChannel) {
            this.request = request;
            this.fileChannel = fileChannel;
        }

        public FileChannel fileChannel() {
            return fileChannel;
        }

        public NearlineRequest request() {
            return request;
        }

        public Instant getCreationTime() {
            return btime;
        }
    }

    private static class RemoteAddressHolder {
        private final InetSocketAddress remote;

        public RemoteAddressHolder(ChannelHandlerContext ctx) {
            this.remote = (InetSocketAddress) ctx.channel().remoteAddress();
        }

        @Override
        public String toString() {
            return InetAddresses.toUriString(remote.getAddress()) + ":" + remote.getPort();
        }
    }

    /**
     * Requests associated with the open files.
     */
    private final List<MigrationRequest> openFiles = new ArrayList<>();

    /**
     * Read/write lock to guard access to {@link #openFiles} list.
     */
    private final ReadWriteLock openFileLock = new ReentrantReadWriteLock();

    private final ConcurrentMap<String, PendingRequest> pendingRequests;

    /**
     * Driver configured hsm name.
     */
    private final String hsmName;

    /**
     * Driver configured hsm type;
     */
    private final String hsmType;

    public DataServerHandler(String type, String name,
          ConcurrentMap<String, PendingRequest> pendingRequests) {

        hsmType = type;
        hsmName = name;
        this.pendingRequests = pendingRequests;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable t) {
        if (t instanceof ClosedChannelException) {
            LOGGER.info("Connection closed");
        } else {
            LOGGER.warn("unexpected exception", t);
            Thread me = Thread.currentThread();
            me.getUncaughtExceptionHandler().uncaughtException(me, t);
        }
        ctx.close();
    }

    @Override
    protected Object doOnLogin(ChannelHandlerContext ctx, LoginRequest msg) throws XrootdException {
        XrootdSessionIdentifier sessionId = new XrootdSessionIdentifier();
        return new LoginResponse(msg, sessionId, "");
    }

    @Override
    protected Object doOnEndSession(ChannelHandlerContext ctx, EndSessionRequest request) {
        return withOk(request);
    }

    @Override
    protected StatResponse doOnStat(ChannelHandlerContext ctx,
          StatRequest req)
          throws XrootdException {
        FileStatus fs;
        if (req.getTarget() == StatRequest.Target.FHANDLE) {
            fs = statusByHandle(req.getFhandle());
        } else {
            fs = statusByPath(req.getPath());
        }
        return new StatResponse(req, fs);
    }

    @Override
    protected OkResponse<PrepareRequest> doOnPrepare(ChannelHandlerContext ctx,
          PrepareRequest msg) {
        return withOk(msg);
    }

    /**
     * Obtains the right mover instance using an opaque token in the request and instruct the mover
     * to open the file in the request. Associates the mover with the file-handle that is produced
     * during processing
     */
    @Override
    protected OpenResponse doOnOpen(ChannelHandlerContext ctx,
          OpenRequest msg)
          throws XrootdException {
        try {
            var pr = getIORequest(msg.getPath());
            var r = pr.getRequest();
            var file = getFile(r);

            LOGGER.info("Request {} scheduling time: {}", file,
                  TimeUtils.describe(Duration.between(Instant.now(), pr.getSubmissionTime()).abs())
                        .orElse("-"));

            EnumSet<StandardOpenOption> openOptions = EnumSet.noneOf(StandardOpenOption.class);
            if (msg.isReadWrite() || msg.isNew() || msg.isDelete()) {
                if (!(r instanceof StageRequest)) {
                    throw new XrootdException(kXR_ArgInvalid,
                          "An attempt to open-for-read for stage requests");
                }
                LOGGER.info("Opening {} for writing from {}", file, new RemoteAddressHolder(ctx));
                openOptions.add(StandardOpenOption.CREATE);
                openOptions.add(StandardOpenOption.WRITE);
                if (msg.isDelete()) {
                    openOptions.add(StandardOpenOption.TRUNCATE_EXISTING);
                }
            } else {
                if (!(r instanceof FlushRequest)) {
                    throw new XrootdException(kXR_ArgInvalid,
                          "An attempt to open-for-write for flush requests");
                }
                LOGGER.info("Opening {} for reading from {}.", file, new RemoteAddressHolder(ctx));
                openOptions.add(StandardOpenOption.READ);
            }

            FileChannel fileChannel = FileChannel.open(file.toPath(), openOptions);
            FileStatus stat = null;
            if (msg.isRetStat()) {
                stat = statusByFile(file);
            }

            var migrationRequest = new MigrationRequest(r, fileChannel);
            int fd = addOpenFile(migrationRequest);

            return new OpenResponse(msg,
                  fd,
                  null,
                  null,
                  stat);
        } catch (FileNotFoundException e) {
            throw new XrootdException(kXR_NotFound, e.getMessage());
        } catch (IOException e) {
            throw new XrootdException(kXR_IOError, e.getMessage());
        }
    }

    /**
     * Use the file descriptor retrieved from the mover upon open and let it obtain a reader object
     * on the pool. The reader object will be placed in a queue, from which it can be taken when
     * sending read information to the client.
     *
     * @param ctx Received from the netty pipeline
     * @param msg The actual request
     */
    @Override
    protected Object doOnRead(ChannelHandlerContext ctx, ReadRequest msg)
          throws XrootdException {
        var request = getOpenFile(msg.getFileHandle());
        FileChannel fileChannel = request.fileChannel();
        if (msg.bytesToRead() == 0) {
            return withOk(msg);
        }

        try {
            var ioStat = request.ioStat.newRequest();
            return new ZeroCopyReadResponse(msg, fileChannel, ioStat);
        } catch (IOException e) {
            throw new XrootdException(kXR_IOError, e.getMessage());
        }
    }

    /**
     * Retrieves the file descriptor obtained upon open and invokes its write operation. The file
     * descriptor will propagate necessary function calls to the mover.
     *
     * @param ctx received from the netty pipeline
     * @param msg the actual request
     */
    @Override
    protected OkResponse<WriteRequest> doOnWrite(ChannelHandlerContext ctx, WriteRequest msg)
          throws XrootdException {
        try {
            var request = getOpenFile(msg.getFileHandle());
            FileChannel fileChannel = request.fileChannel();
            fileChannel.position(msg.getWriteOffset());
            var ioRequest = request.ioStat.newRequest();
            msg.getData(fileChannel);
            ioRequest.done(msg.getDataLength());
            return withOk(msg);
        } catch (IOException e) {
            throw new XrootdException(kXR_IOError, e.getMessage());
        }
    }

    /**
     * Retrieves the right mover based on the request's file-handle and invokes its sync-operation.
     *
     * @param ctx received from the netty pipeline
     * @param msg The actual request
     */
    @Override
    protected OkResponse<SyncRequest> doOnSync(ChannelHandlerContext ctx, SyncRequest msg)
          throws XrootdException {
        try {
            getOpenFile(msg.getFileHandle()).fileChannel().force(false);
            return withOk(msg);
        } catch (IOException e) {
            throw new XrootdException(kXR_IOError, e.getMessage());
        }
    }

    /**
     * Retrieves the right descriptor based on the request's file-handle and invokes its close
     * information.
     *
     * @param ctx received from the netty pipeline
     * @param msg The actual request
     */
    @Override
    protected OkResponse<CloseRequest> doOnClose(ChannelHandlerContext ctx, CloseRequest msg)
          throws XrootdException {
        try {
            var migrationRequest = getAndRemoveOpenFile(msg.getFileHandle());
            migrationRequest.fileChannel().close();

            var r = migrationRequest.request();
            var file = getFile(r);
            long size = file.length();
            long duration = Duration.between(migrationRequest.getCreationTime(), Instant.now())
                  .toMillis();

            LOGGER.info("Closing file {} from {}. Transferred {} in {}, disk performance {}", file,
                  new RemoteAddressHolder(ctx),
                  Strings.humanReadableSize(size),
                  TimeUtils.describeDuration(duration, TimeUnit.MILLISECONDS),
                  Strings.describeBandwidth(migrationRequest.ioStat.getMean())
            );

            return withOk(msg);
        } catch (IOException e) {
            throw new XrootdException(kXR_IOError, e.getMessage());
        }
    }

    @Override
    protected QueryResponse doOnQuery(ChannelHandlerContext ctx, QueryRequest msg)
          throws XrootdException {
        switch (msg.getReqcode()) {

            case kXR_Qopaquf:
                var query = msg.getArgs();
                LOGGER.info("XROOTD query: {} from {}", query, new RemoteAddressHolder(ctx));

                final var errorPrefix = "error=";

                if (!query.startsWith("/error/") && !query.startsWith("/success/")) {
                    throw new XrootdException(kXR_ArgInvalid, "Invalid request");
                }

                var url = URI.create(query);
                var uriQuery = url.getQuery();

                var requestId = new File(url.getPath()).getName();
                var pr = pendingRequests.remove(requestId);
                if (pr == null) {
                    throw new XrootdException(kXR_ArgInvalid, "Invalid request id");
                }
                var r = pr.getRequest();

                if (query.startsWith("/error/")) {
                    if (!uriQuery.startsWith(errorPrefix)) {
                        throw new XrootdException(kXR_ArgInvalid, "Invalid success uri");
                    }
                    var error = new String(
                          Base64.getDecoder().decode(uriQuery.substring(errorPrefix.length())),
                          StandardCharsets.UTF_8);
                    LOGGER.error("Error report on flushing: {} from {} : {}", requestId, new RemoteAddressHolder(ctx), error);
                    r.failed(CacheException.SERVICE_UNAVAILABLE, error);
                } else if (query.startsWith("/success/")) {

                    var m = SUCCESS_URI.matcher(query);
                    if (!m.find()) {
                        throw new XrootdException(kXR_ArgInvalid, "Invalid success uri");
                    }
                    var archiveId = m.group("archiveid");
                    var id = getPnfsId(r);

                    // REVISIT: java17: use switch with pattern
                    if (r instanceof StageRequest) {
                        var file = getFile(r);
                        ForkJoinPool.commonPool().execute(() -> {
                            try {
                                Checksum checksum = calculateChecksum(file);
                                LOGGER.info("Files {} checksum after restore: {}", file, checksum);
                                LOGGER.info("Successful restored from {} : {} : archive id: {}", new RemoteAddressHolder(ctx), requestId, archiveId);
                                r.completed(Set.of(checksum));
                            } catch (IOException e) {
                                LOGGER.error("Post-restore checksum calculation of {} failed: {}", file,
                                        e.getMessage());
                                r.failed(e);
                            }
                        });

                    } else if (r instanceof FlushRequest ) {
                        var hsmUrl = URI.create(
                                hsmType + "://" + hsmName + "/" + id + "?archiveid=" + archiveId);
                        r.completed(Set.of(hsmUrl));
                        LOGGER.info("Successful flushing from {} : {} : archive id: {}", new RemoteAddressHolder(ctx), requestId, archiveId);
                    } else {
                        LOGGER.warn("Unexpected request type: {}", r.getClass());
                        throw new XrootdException(kXR_ArgInvalid, "Invalid request type: " + r.getClass());
                    }
                }

                return new QueryResponse(msg, "");

            default:
                LOGGER.error("Unsupported query code from {} : {}", new RemoteAddressHolder(ctx), msg.getReqcode());
                throw new XrootdException(kXR_Unsupported,
                      "Unsupported kXR_query reqcode: " + msg.getReqcode());
        }
    }

    private int addOpenFile(MigrationRequest migrationRequest) {
        var writeLock = openFileLock.writeLock();
        writeLock.lock();
        try {
            for (int i = 0; i < openFiles.size(); i++) {
                if (openFiles.get(i) == null) {
                    openFiles.set(i, migrationRequest);
                    return i;
                }
            }
            openFiles.add(migrationRequest);
            return openFiles.size() - 1;
        } finally {
            writeLock.unlock();
        }
    }

    private MigrationRequest getOpenFile(int fd)
          throws XrootdException {
        var readLock = openFileLock.readLock();
        readLock.lock();
        try {
            if (fd >= 0 && fd < openFiles.size()) {
                var migrationRequest = openFiles.get(fd);
                if (migrationRequest != null) {
                    return migrationRequest;
                }
            }
        } finally {
            readLock.unlock();
        }
        throw new XrootdException(kXR_FileNotOpen, "Invalid file descriptor");
    }

    private MigrationRequest getAndRemoveOpenFile(int fd)
          throws XrootdException {
        var writeLock = openFileLock.writeLock();
        writeLock.lock();
        try {
            if (fd >= 0 && fd < openFiles.size()) {
                var migrationRequest = openFiles.set(fd, null);
                if (migrationRequest != null) {
                    return migrationRequest;
                }
            }
        } finally {
            writeLock.unlock();
        }
        throw new XrootdException(kXR_FileNotOpen, "Invalid file descriptor");
    }

    private PendingRequest getIORequest(String path)
          throws XrootdException {

        var r = pendingRequests.get(path);
        if (r == null) {
            throw new XrootdException(kXR_NotFound, "Request not found: " + path);
        }

        return r;
    }

    private File getFile(NearlineRequest r)
          throws XrootdException {

        String localPath = null;
        if (r instanceof FlushRequest) {
            localPath = ((FlushRequest) r).getReplicaUri().getPath();
        }

        if (r instanceof StageRequest) {
            localPath = ((StageRequest) r).getReplicaUri().getPath();
        }

        if (localPath == null) {
            throw new XrootdException(kXR_ArgInvalid, "Invalid request: " + r.getId());
        }

        return new File(localPath);
    }

    private int getFileStatusFlagsOf(File file) {
        int flags = 0;
        if (file.isDirectory()) {
            flags |= kXR_isDir;
        }
        if (!file.isFile() && !file.isDirectory()) {
            flags |= kXR_other;
        }
        if (file.canExecute()) {
            flags |= kXR_xset;
        }
        if (file.canRead()) {
            flags |= kXR_readable;
        }
        if (file.canWrite()) {
            flags |= kXR_writable;
        }
        return flags;
    }

    private FileStatus statusByFile(File file) throws XrootdException {

        if (!file.exists()) {
            throw new XrootdException(kXR_NotFound, "No such file");
        }

        int flags = getFileStatusFlagsOf(file);
        return new FileStatus(0,
              file.length(),
              flags,
              file.lastModified() / 1000);
    }

    private FileStatus statusByPath(String path) throws XrootdException {
        File file = getFile(getIORequest(path).getRequest());
        return statusByFile(file);
    }

    private FileStatus statusByHandle(int handle) throws XrootdException {

        FileChannel file = getOpenFile(handle).fileChannel();
        try {
            return new FileStatus(0,
                  file.size(),
                  kXR_readable,
                  System.currentTimeMillis() / 1000);
        } catch (IOException e) {
            throw new XrootdException(kXR_IOError, e.getMessage());
        }
    }

    private static Checksum calculateChecksum(File file) throws IOException {

        ByteBuffer bb = ByteBuffer.allocate(8192);
        var adler = new Adler32();

        try (FileChannel fc = FileChannel.open(file.toPath())) {
            while (true) {
                bb.clear();
                int n = fc.read(bb);
                if (n < 0) {
                    break;
                }
                bb.flip();
                adler.update(bb);
            }

            return new Checksum(ChecksumType.ADLER32, adler.digest());
        }
    }

    private String getPnfsId(NearlineRequest<?> request) {

        if (request instanceof StageRequest) {
            return ((StageRequest) request).getFileAttributes().getPnfsId().toString();
        } else if (request instanceof FlushRequest) {
            return ((FlushRequest) request).getFileAttributes().getPnfsId().toString();
        } else {
            throw new IllegalArgumentException("Request must be StageRequest or FlushRequest");
        }

    }
}
