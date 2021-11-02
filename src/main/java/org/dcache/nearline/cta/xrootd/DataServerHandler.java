/*
 * Copyright (C) 2011-2021 dCache.org <support@dcache.org>
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

import static org.dcache.xrootd.protocol.XrootdProtocol.DATA_SERVER;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ArgInvalid;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_FileNotOpen;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_IOError;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_NotFound;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_Qopaquf;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_Unsupported;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_isDir;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_isDirectory;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_other;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_readable;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_writable;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_xset;

import diskCacheV111.util.Adler32;
import diskCacheV111.util.CacheException;
import io.netty.channel.ChannelHandlerContext;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ForkJoinPool;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.NearlineRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.core.XrootdRequestHandler;
import org.dcache.xrootd.protocol.messages.CloseRequest;
import org.dcache.xrootd.protocol.messages.EndSessionRequest;
import org.dcache.xrootd.protocol.messages.OkResponse;
import org.dcache.xrootd.protocol.messages.OpenRequest;
import org.dcache.xrootd.protocol.messages.OpenResponse;
import org.dcache.xrootd.protocol.messages.PrepareRequest;
import org.dcache.xrootd.protocol.messages.ProtocolRequest;
import org.dcache.xrootd.protocol.messages.ProtocolResponse;
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

public class DataServerHandler extends XrootdRequestHandler {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(DataServerHandler.class);

    /**
     * A record that binds open file and request.
     */
    private static class MigrationRequest {

        private final NearlineRequest request;
        private final RandomAccessFile raf;

        public MigrationRequest(NearlineRequest request, RandomAccessFile raf) {
            this.request = request;
            this.raf = raf;
        }

        public RandomAccessFile raf() {
            return raf;
        }

        public NearlineRequest request() {
            return request;
        }
    }

    private final List<MigrationRequest> _openFiles = new ArrayList<>();

    private final ConcurrentMap<String, ? extends NearlineRequest> pendingRequests;

    /**
     * Driver configured hsm name.
     */
    private final String hsmName;

    /**
     * Driver configured hsm type;
     */
    private final String hsmType;

    public DataServerHandler(String type, String name,
          ConcurrentMap<String, ? extends NearlineRequest> pendingRequests) {

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
    protected ProtocolResponse doOnProtocolRequest(
          ChannelHandlerContext ctx, ProtocolRequest msg) {
        return new ProtocolResponse(msg, DATA_SERVER);
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
            NearlineRequest r = getIORequest(msg.getPath());
            var file = getFile(r);
            if (file.isDirectory()) {
                throw new XrootdException(kXR_isDirectory, "Not a file: " + file);
            }

            RandomAccessFile raf;
            if (msg.isReadWrite() || msg.isNew() || msg.isDelete()) {
                LOGGER.info("Opening {} for writing", file);
                raf = new RandomAccessFile(file, "rw");
                if (msg.isDelete()) {
                    raf.setLength(0);
                }
            } else {
                LOGGER.info("Opening {} for reading.", file);
                raf = new RandomAccessFile(file, "r");
            }

            FileStatus stat = null;
            if (msg.isRetStat()) {
                stat = statusByFile(file);
            }

            var migrationRequest = new MigrationRequest(r, raf);
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
        RandomAccessFile raf = getOpenFile(msg.getFileHandle()).raf();
        if (msg.bytesToRead() == 0) {
            return withOk(msg);
        }

        try {
            return new ZeroCopyReadResponse(msg, raf.getChannel());
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
            FileChannel channel =
                  getOpenFile(msg.getFileHandle()).raf().getChannel();
            channel.position(msg.getWriteOffset());
            msg.getData(channel);
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
            getOpenFile(msg.getFileHandle()).raf().getFD().sync();
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
            var r = closeOpenFile(msg.getFileHandle());
            var file = getFile(r);
            if (r instanceof StageRequest) {
                ForkJoinPool.commonPool().execute(() -> {
                    try {
                        Checksum checksum = calculateChecksum(file);
                        r.completed(Set.of(checksum));
                    } catch (IOException e) {
                        LOGGER.error("Post-restore checksum calculation of {} failed: {}", file,
                              e.getMessage());
                        r.failed(e);
                    }
                });
            }

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
                LOGGER.info("XROOD query: {}", query);

                final var idPrefix = "archiveid=";
                final var errorPrefix = "error=";

                if (!query.startsWith("/error/") && !query.startsWith("/success/")) {
                    throw new XrootdException(kXR_ArgInvalid, "Invalid request");
                }

                var url = URI.create(query);
                var uriQuery = url.getQuery();

                var requestId = new File(url.getPath()).getName();
                var r = pendingRequests.remove(requestId);
                if (r == null) {
                    throw new XrootdException(kXR_ArgInvalid, "Invalid request id");
                }

                if (query.startsWith("/error/")) {
                    if (!uriQuery.startsWith(errorPrefix)) {
                        throw new XrootdException(kXR_ArgInvalid, "Invalid success uri");
                    }
                    var error = new String(
                          Base64.getDecoder().decode(uriQuery.substring(errorPrefix.length())),
                          StandardCharsets.UTF_8);
                    LOGGER.error("Error report on flushing: {} : {}", requestId, error);
                    r.failed(CacheException.SERVICE_UNAVAILABLE, error);
                } else if (query.startsWith("/success/")) {
                    if (!uriQuery.startsWith(idPrefix)) {
                        throw new XrootdException(kXR_ArgInvalid, "Invalid success uri");
                    }
                    // validate that id is a long
                    var archiveId = Long.parseLong(uriQuery.substring(idPrefix.length()));
                    var id = getPnfsId(r);
                    var hsmUrl = URI.create(
                          hsmType + "://" + hsmName + "/" + id + "?archiveid=" + archiveId);
                    r.completed(Set.of(hsmUrl));

                    LOGGER.info("Successful flushing: {} : archive id: {}", requestId, archiveId);
                }

                return new QueryResponse(msg, "");

            default:
                LOGGER.error("Unsupported query code: {}", msg.getReqcode());
                throw new XrootdException(kXR_Unsupported,
                      "Unsupported kXR_query reqcode: " + msg.getReqcode());
        }
    }

    private int addOpenFile(MigrationRequest migrationRequest) {
        for (int i = 0; i < _openFiles.size(); i++) {
            if (_openFiles.get(i) == null) {
                _openFiles.set(i, migrationRequest);
                return i;
            }
        }
        _openFiles.add(migrationRequest);
        return _openFiles.size() - 1;
    }

    private MigrationRequest getOpenFile(int fd)
          throws XrootdException {
        if (fd >= 0 && fd < _openFiles.size()) {
            var migrationRequest = _openFiles.get(fd);
            if (migrationRequest != null) {
                return migrationRequest;
            }
        }
        throw new XrootdException(kXR_FileNotOpen, "Invalid file descriptor");
    }

    private NearlineRequest closeOpenFile(int fd)
          throws XrootdException, IOException {

        var migrationRequest = getOpenFile(fd);
        migrationRequest.raf().close();
        _openFiles.set(fd, null);
        return migrationRequest.request();
    }


    private NearlineRequest getIORequest(String path)
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
        File file = getFile(getIORequest(path));
        return statusByFile(file);
    }

    private FileStatus statusByHandle(int handle) throws XrootdException {

        RandomAccessFile file = getOpenFile(handle).raf();
        try {
            return new FileStatus(0,
                  file.length(),
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
