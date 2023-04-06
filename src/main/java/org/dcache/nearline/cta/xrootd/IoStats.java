package org.dcache.nearline.cta.xrootd;

// rip off RequestExecutionTimeGaugeImpl#Statistics

import java.util.concurrent.TimeUnit;

/**
 * Encapsulates an online algorithm for maintaining various statistics about samples.
 * <p>
 * See https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance for an explanation.
 */
public class IoStats {

    private double mean;
    private double m2;
    private double min = Double.NaN;
    private double max = Double.NaN;
    private long n;

    public synchronized void update(double x) {
        this.min = this.n == 0L ? x : Math.min(x, this.min);
        this.max = this.n == 0L ? x : Math.max(x, this.max);
        ++this.n;
        double nextMean = this.mean + (x - this.mean) / (double) this.n;
        double nextM2 = this.m2 + (x - this.mean) * (x - nextMean);
        this.mean = nextMean;
        this.m2 = nextM2;
    }

    public synchronized double getMean() {
        return this.n > 0L ? this.mean : Double.NaN;
    }

    public synchronized double getSampleVariance() {
        return this.n > 1L ? this.m2 / (double) (this.n - 1L) : Double.NaN;
    }

    public synchronized double getPopulationVariance() {
        return this.n > 0L ? this.m2 / (double) this.n : Double.NaN;
    }

    public synchronized double getSampleStandardDeviation() {
        return Math.sqrt(this.getSampleVariance());
    }

    public synchronized double getPopulationStandardDeviation() {
        return Math.sqrt(this.getPopulationVariance());
    }

    public synchronized double getStandardError() {
        return this.getSampleStandardDeviation() / Math.sqrt((double) this.n);
    }

    public synchronized long getSampleSize() {
        return this.n;
    }

    public synchronized double getMin() {
        return this.min;
    }

    public synchronized double getMax() {
        return this.max;
    }

    public IoRequest newRequest() {
        return new IoRequest();
    }

    public class IoRequest {

        long t0 = System.nanoTime();

        private IoRequest() {
        }

        public void done(long size) {
            long delta = System.nanoTime() - t0;
            double bandwith = ((double) size / delta )  * TimeUnit.SECONDS.toNanos(1);
            update(bandwith);
        }
    }
}