package org.dcache.nearline.cta.xrootd;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;
import org.dcache.util.ByteUnit;
import org.dcache.util.ByteUnits;
import org.junit.Test;

public class IoStatsTest {


    private IoStats ioStats = new IoStats();


    @Test
    public void testStats() throws InterruptedException {
        var stats = ioStats.newRequest();

        var speedInMB = 300;
        var sleepTime = 5; // 5 sec

        TimeUnit.SECONDS.sleep(sleepTime);
        stats.done(ByteUnit.MB.toBytes(speedInMB) * sleepTime);

        // expected 300 MB/s
        assertEquals(speedInMB, ByteUnit.BYTES.toMB(ioStats.getMean()), 1.0);
    }

}