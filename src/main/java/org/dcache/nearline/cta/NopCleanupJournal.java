package org.dcache.nearline.cta;

import ch.cern.cta.rpc.CtaRpcGrpc;
import ch.cern.cta.rpc.Request;

import java.util.function.BiFunction;

public class NopCleanupJournal implements CleanupJournal {

    @Override
    public void cleanup(CtaRpcGrpc.CtaRpcBlockingStub cta) {
        // NOP
    }

    @Override
    public void cleanup(CtaRpcGrpc.CtaRpcBlockingStub cta, BiFunction<String, Request, Boolean> function) {
        // NOP
    }

    @Override
    public void put(String pnfsid, Request archiveResponse) {
        // NOP
    }

    @Override
    public void remove(String pnfsid) {
        // NOP
    }

    @Override
    public void close() {
        // nop
    }
}
