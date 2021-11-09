package org.dcache.nearline.cta;

import com.google.common.util.concurrent.ListenableFuture;
import java.io.File;
import java.net.URI;
import java.util.Set;
import java.util.UUID;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.vehicles.FileAttributes;

// FIXME: this should be part of dCache
/**
 * A FlushRequest that forwards all calls to some other FlushRequest.
 */
public abstract class ForwardingFlushRequest implements FlushRequest {

    abstract protected FlushRequest delegate();

    @Override
    public File getFile() {
        return delegate().getFile();
    }

    @Override
    public URI getReplicaUri() {
        return delegate().getReplicaUri();
    }

    @Override
    public FileAttributes getFileAttributes() {
        return delegate().getFileAttributes();
    }

    @Override
    public long getReplicaCreationTime() {
        return delegate().getReplicaCreationTime();
    }

    @Override
    public ListenableFuture<String> activateWithPath() {
        return delegate().activateWithPath();
    }

    @Override
    public UUID getId() {
        return delegate().getId();
    }

    @Override
    public long getDeadline() {
        return delegate().getDeadline();
    }

    @Override
    public ListenableFuture<Void> activate() {
        return delegate().activate();
    }

    @Override
    public void failed(Exception e) {
        delegate().failed(e);
    }

    @Override
    public void failed(int i, String s) {
        delegate().failed(i, s);
    }

    @Override
    public void completed(Set<URI> uris) {
        delegate().completed(uris);
    }
}
