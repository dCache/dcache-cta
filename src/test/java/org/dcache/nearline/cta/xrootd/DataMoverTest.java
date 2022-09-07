package org.dcache.nearline.cta.xrootd;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyOrNullString;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.dcache.nearline.cta.PendingRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DataMoverTest {

    private DataMover dataMover;
    private ConcurrentMap<String, PendingRequest> requests;

    @Before
    public void setUp() throws UnknownHostException {
        requests = new ConcurrentHashMap<>();
        dataMover = new DataMover("cta", "testing",
              new InetSocketAddress(InetAddress.getLocalHost(), 0),
              requests);
        dataMover.startAsync().awaitRunning();
    }

    @After
    public void tearDown() {
        dataMover.stopAsync().awaitTerminated();
    }

    @Test(expected = Exception.class)
    public void testBindError() throws UnknownHostException {
        URI uri = URI.create(dataMover.getTransport("1", 1).getDstUrl());
        new DataMover("foo", "bar",
              new InetSocketAddress(InetAddress.getLocalHost(), uri.getPort()), requests)
              .startAsync()
              .awaitRunning();
    }

    @Test
    public void testGetTransport() {
        var transport = dataMover.getTransport("1", 1);

        assertThat("destination IRL is not set", transport.getDstUrl(), not(emptyOrNullString()));
        assertThat("error report URL is not set", transport.getErrorReportUrl(),
              not(emptyOrNullString()));
        assertThat("status URL is not set", transport.getReportUrl(), not(emptyOrNullString()));
    }
}