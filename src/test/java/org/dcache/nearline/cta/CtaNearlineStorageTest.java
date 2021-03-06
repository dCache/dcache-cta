package org.dcache.nearline.cta;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.dcache.nearline.cta.CtaNearlineStorage.CTA_CA;
import static org.dcache.nearline.cta.CtaNearlineStorage.CTA_ENDPOINT;
import static org.dcache.nearline.cta.CtaNearlineStorage.CTA_GROUP;
import static org.dcache.nearline.cta.CtaNearlineStorage.CTA_INSTANCE;
import static org.dcache.nearline.cta.CtaNearlineStorage.CTA_TLS;
import static org.dcache.nearline.cta.CtaNearlineStorage.CTA_USER;
import static org.dcache.nearline.cta.CtaNearlineStorage.IO_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import diskCacheV111.vehicles.GenericStorageInfo;
import eu.emi.security.authn.x509.impl.CertificateUtils;
import eu.emi.security.authn.x509.impl.CertificateUtils.Encoding;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.URI;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.vehicles.FileAttributes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CtaNearlineStorageTest {

    private DummyCta cta;
    private Map<String, String> drvConfig;
    private CtaNearlineStorage driver;

    private CompletableFuture<Void> waitForComplete;

    private File keyFile;
    private File certFile;

    @Before
    public void setUp() throws Exception {

        keyFile = File.createTempFile("hostkey-", ".pem");
        certFile = File.createTempFile("hostcert-", ".pem");

        generateSelfSignedCert();

        cta = new DummyCta(certFile, keyFile);
        cta.start();

        // make mutable config
        drvConfig = new HashMap<>(
              Map.of(
                    CTA_USER, "foo",
                    CTA_GROUP, "bar",
                    CTA_INSTANCE, "foobar",
                    CTA_ENDPOINT, cta.getConnectString(),
                    IO_PORT, "9991",
                    CTA_TLS, "true",
                    CTA_CA, certFile.getAbsolutePath()
              )
        );
    }

    @After
    public void tierDown() {
        try {
            cta.shutdown();
        } catch (InterruptedException e) {
        }
        if (driver != null) {
            driver.shutdown();
        }

        keyFile.delete();
        certFile.delete();
    }

    @Test(expected = NullPointerException.class)
    public void testMissingHsmType() {
        new CtaNearlineStorage(null, "aName");
    }

    @Test(expected = NullPointerException.class)
    public void testMissingHsmName() {
        new CtaNearlineStorage("aType", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCfgMissingInstance() {

        driver = new CtaNearlineStorage("aType", "aName");

        drvConfig.remove(CTA_INSTANCE);
        driver.configure(drvConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCfgMissingUser() {

        driver = new CtaNearlineStorage("aType", "aName");

        drvConfig.remove(CTA_USER);
        driver.configure(drvConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCfgMissingGroup() {

        driver = new CtaNearlineStorage("aType", "aName");
        drvConfig.remove(CTA_GROUP);
        driver.configure(drvConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCfgMissingEndpoint() {

        driver = new CtaNearlineStorage("aType", "aName");

        drvConfig.remove(CTA_ENDPOINT);
        driver.configure(drvConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCfgInvalidEndpoint() {

        driver = new CtaNearlineStorage("aType", "aName");

        drvConfig.put(CTA_ENDPOINT, "localhost");
        driver.configure(drvConfig);
    }

    @Test
    public void testCfgAcceptValid() {

        driver = new CtaNearlineStorage("aType", "aName");
        driver.configure(drvConfig);
        driver.start();
    }

    @Test
    public void testWithoutTLS() {

        driver = new CtaNearlineStorage("aType", "aName");
        drvConfig.put(CTA_TLS, "false");

        driver.configure(drvConfig);
        driver.start();

        var request = mockedFlushRequest();
        driver.flush(Set.of(request));
        waitToComplete();
        verify(request).failed(any(io.grpc.StatusRuntimeException.class));
    }

    @Test
    public void testFlushRequestActivationOnSubmit() {

        var request = mockedFlushRequest();
        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.flush(Set.of(request));

        verify(request).activate();
    }

    @Test
    public void testStageRequestActivationOnSubmit() {

        var request = mockedStageRequest();
        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.stage(Set.of(request));

        verify(request).activate();
    }

    @Test
    public void testSpaceAllocationOnSubmit() {

        var request = mockedStageRequest();
        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.stage(Set.of(request));

        verify(request).allocate();
    }

    @Test
    public void testFlushRequestFailActivation() {

        var request = mockedFlushRequest();
        when(request.activate()).thenReturn(
              Futures.immediateFailedFuture(new IOException("injected error")));

        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.flush(Set.of(request));

        verify(request).failed(any());
    }

    @Test
    public void testStageRequestFailActivation() {

        var request = mockedStageRequest();
        when(request.activate()).thenReturn(
              Futures.immediateFailedFuture(new IOException("injected error")));

        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.stage(Set.of(request));

        verify(request).failed(any());
    }

    @Test
    public void testStageRequestFailAllocation() {

        var request = mockedStageRequest();
        when(request.allocate()).thenReturn(
              Futures.immediateFailedFuture(new IOException("injected error")));

        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.stage(Set.of(request));

        verify(request).failed(any());
    }

    @Test
    public void testStartAfterShutdown() {

        driver = new CtaNearlineStorage("aType", "aName");
        driver.configure(drvConfig);
        driver.start();
        driver.shutdown();

        driver = new CtaNearlineStorage("aType", "aName");
        driver.configure(drvConfig);
        driver.start();
        driver.shutdown();
    }

    @Test
    public void testStageRequestFailOnRpcError() {

        var request = mockedStageRequest();

        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        cta.fail();
        driver.stage(Set.of(request));
        waitToComplete();

        verify(request).failed(any());
    }

    @Test
    public void testFlushRequestFailOnRpcError() {

        var request = mockedFlushRequest();

        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        cta.fail();
        driver.flush(Set.of(request));
        waitToComplete();

        verify(request).failed(any());
    }

    @Test
    public void testSuccessOnRemove() {

        var request = mockedRemoveRequest();

        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.remove(Set.of(request));
        waitToComplete();

        verify(request).completed(any());
    }

    @Test
    public void testRemoveRequestOnRpcError() {

        var request = mockedRemoveRequest();

        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();
        cta.fail();

        driver.remove(Set.of(request));
        waitToComplete();

        verify(request).failed(any());
    }

    @Test
    public void testPendingRequestIncrementOnStageSubmit() {

        var request = mockedStageRequest();
        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.stage(Set.of(request));

        cta.waitToReply();
        assertEquals("unexpected pending request queue size", 1,
              driver.getPendingRequestsCount());
    }

    @Test
    public void testPendingRequestDecOnStageComplete() {

        var request = mockedStageRequest();
        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.stage(Set.of(request));

        cta.waitToReply();
        driver.getRequest("0000C9B4E3768770452E8B1B8E0232584872").getRequest().completed(Set.of());
        assertEquals("pending request count not zero", 0, driver.getPendingRequestsCount());
    }

    @Test
    public void testPendingRequestDecOnFlushComplete() {

        var request = mockedFlushRequest();
        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.flush(Set.of(request));

        cta.waitToReply();
        driver.getRequest("0000C9B4E3768770452E8B1B8E0232584872").getRequest().completed(Set.of());
        assertEquals("pending request count not zero", 0, driver.getPendingRequestsCount());
    }

    @Test
    public void testPendingRequestDecOnStageFailedV1() {

        var request = mockedStageRequest();
        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.stage(Set.of(request));

        cta.waitToReply();
        driver.getRequest("0000C9B4E3768770452E8B1B8E0232584872").getRequest().failed(new Exception());
        assertEquals("pending request count not zero", 0, driver.getPendingRequestsCount());
    }

    @Test
    public void testPendingRequestDecOnStageFailedV2() {

        var request = mockedStageRequest();
        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.stage(Set.of(request));

        cta.waitToReply();
        driver.getRequest("0000C9B4E3768770452E8B1B8E0232584872").getRequest().failed(1, "foo");
        assertEquals("pending request count not zero", 0, driver.getPendingRequestsCount());
    }

    @Test
    public void testPendingRequestIncrementOnFlushSubmit() {

        var request = mockedFlushRequest();
        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.flush(Set.of(request));

        cta.waitToReply();
        assertEquals("unexpected pending request queue size", 1,
              driver.getPendingRequestsCount());
    }

    @Test
    public void testPendingRequestDecOnFlushFailedV1() {

        var request = mockedFlushRequest();
        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.flush(Set.of(request));

        cta.waitToReply();
        driver.getRequest("0000C9B4E3768770452E8B1B8E0232584872").getRequest().failed(new Exception());
        assertEquals("pending request count not zero", 0, driver.getPendingRequestsCount());
    }

    @Test
    public void testPendingRequestDecOnFlushFailedV2() {

        var request = mockedFlushRequest();
        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.flush(Set.of(request));

        cta.waitToReply();
        driver.getRequest("0000C9B4E3768770452E8B1B8E0232584872").getRequest().failed(1, "foo");
        assertEquals("pending request count not zero", 0, driver.getPendingRequestsCount());
    }

    @Test
    public void testCencelOfPendingRequest() {

        var request = mockedStageRequest();
        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.stage(Set.of(request));
        cta.waitToReply();

        driver.cancel(request.getId());
        cta.waitToReply();
        assertEquals("unexpected pending request queue size", 0, driver.getPendingRequestsCount());

    }

    @Test
    public void testCencelByRandomUUID() {

        var request = mockedStageRequest();
        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.stage(Set.of(request));

        cta.waitToReply();

        driver.cancel(UUID.randomUUID());
        assertEquals("unexpected pending request queue size", 1, driver.getPendingRequestsCount());

    }

    @Test
    public void testCancelOfPendingStageRequest() {

        var request = mockedStageRequest();
        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.stage(Set.of(request));
        cta.waitToReply();

        driver.cancel(request.getId());
        cta.waitToReply();

        verify(request).failed(any(CancellationException.class));

    }

    @Test
    public void testCancelOfPendingStageRequestOnError() {

        var request = mockedStageRequest();
        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.stage(Set.of(request));

        cta.waitToReply();

        cta.fail();
        driver.cancel(request.getId());
        verify(request, times(0)).failed(any(CancellationException.class));
        verify(request, times(0)).failed(anyInt(), any());
        verify(request, times(0)).completed(any());

    }

    @Test
    public void testCancelOfPendingFlushRequest() {

        var request = mockedFlushRequest();
        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.flush(Set.of(request));
        cta.waitToReply();

        driver.cancel(request.getId());
        cta.waitToReply();

        verify(request).failed(any(CancellationException.class));

    }

    @Test
    public void testCancelOfPendingFlushRequestOnError() {

        var request = mockedFlushRequest();
        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.flush(Set.of(request));

        cta.waitToReply();
        cta.fail();

        driver.cancel(request.getId());
        verify(request, times(0)).failed(any(CancellationException.class));
        verify(request, times(0)).failed(anyInt(), any());
        verify(request, times(0)).completed(any());
    }

    @Test
    public void testDontSubmitFailedFlushToCta() {

        var request = mockedFlushRequest();
        when(request.activate()).thenReturn(
              Futures.immediateFailedFuture(new IOException("Failed to active request")));

        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.flush(Set.of(request));
        assertEquals("pending request count not zero", 0, driver.getPendingRequestsCount());
    }

    @Test
    public void testDontSubmitFailedStageToCta() {

        var request = mockedStageRequest();
        when(request.activate()).thenReturn(
              Futures.immediateFailedFuture(new IOException("Failed to active request")));

        driver = new CtaNearlineStorage("foo", "bar");
        driver.configure(drvConfig);
        driver.start();

        driver.stage(Set.of(request));
        assertEquals("pending request count not zero", 0, driver.getPendingRequestsCount());
    }

    void waitToComplete() {
        try {
            waitForComplete.get(1, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            fail("Request not complete");
        }
    }

    private FlushRequest mockedFlushRequest() {

        var attrs = FileAttributes.of()
              .size(9876543210L)
              .storageClass("a:b")
              .hsm("z")
              .pnfsId("0000C9B4E3768770452E8B1B8E0232584872")
              .build();

        var request = mock(FlushRequest.class);

        when(request.activate()).thenReturn(Futures.immediateFuture(null));
        when(request.getFileAttributes()).thenReturn(attrs);
        when(request.getId()).thenReturn(UUID.randomUUID());

        waitForComplete = new CompletableFuture<>();

        doAnswer(i -> {
            waitForComplete.complete(null);
            return null;
        }).when(request).failed(any());

        doAnswer(i -> {
            waitForComplete.complete(null);
            return null;
        }).when(request).failed(anyInt(), any());

        doAnswer(i -> {
            waitForComplete.complete(null);
            return null;
        }).when(request).completed(any());

        return request;
    }

    private StageRequest mockedStageRequest() {

        var storageInfo = GenericStorageInfo.valueOf("a:b@z", "*");
        storageInfo.addLocation(URI.create("cta://cta?archiveid=9876543210"));

        var attrs = FileAttributes.of()
              .size(9876543210L)
              .storageClass(storageInfo.getStorageClass())
              .hsm(storageInfo.getHsm())
              .storageInfo(storageInfo)
              .pnfsId("0000C9B4E3768770452E8B1B8E0232584872")
              .build();

        var request = mock(StageRequest.class);

        when(request.activate()).thenReturn(Futures.immediateFuture(null));
        when(request.allocate()).thenReturn(Futures.immediateFuture(null));
        when(request.getFileAttributes()).thenReturn(attrs);
        when(request.getId()).thenReturn(UUID.randomUUID());

        waitForComplete = new CompletableFuture<>();

        doAnswer(i -> {
            waitForComplete.complete(null);
            return null;
        }).when(request).failed(any());

        doAnswer(i -> {
            waitForComplete.complete(null);
            return null;
        }).when(request).failed(anyInt(), any());

        doAnswer(i -> {
            waitForComplete.complete(null);
            return null;
        }).when(request).completed(any());

        return request;
    }

    private RemoveRequest mockedRemoveRequest() {

        var request = mock(RemoveRequest.class);

        when(request.getUri()).thenReturn(
              URI.create("cta://cta/0000C9B4E3768770452E8B1B8E0232584872?archiveid=1234"));

        waitForComplete = new CompletableFuture<>();

        doAnswer(i -> {
            waitForComplete.complete(null);
            return null;
        }).when(request).failed(any());

        doAnswer(i -> {
            waitForComplete.complete(null);
            return null;
        }).when(request).failed(anyInt(), any());

        doAnswer(i -> {
            waitForComplete.complete(null);
            return null;
        }).when(request).completed(any());
        return request;
    }

    private void generateSelfSignedCert()
          throws GeneralSecurityException, OperatorCreationException, IOException {

        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA", new BouncyCastleProvider());
        keyPairGenerator.initialize(2048, new SecureRandom());
        KeyPair keyPair = keyPairGenerator.generateKeyPair();

        long notBefore = System.currentTimeMillis();
        long notAfter = notBefore + TimeUnit.DAYS.toMillis(1);

        X500Name subjectDN = new X500Name("CN=localhost, O=dCache.org");
        X500Name issuerDN = subjectDN;

        SubjectPublicKeyInfo subjectPublicKeyInfo =
              SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded());

        X509v3CertificateBuilder certificateBuilder = new X509v3CertificateBuilder(issuerDN,
              BigInteger.ONE,
              new Date(notBefore),
              new Date(notAfter), subjectDN,
              subjectPublicKeyInfo)
              .addExtension(Extension.basicConstraints, true, new BasicConstraints(true))
              .addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment))
              .addExtension(Extension.extendedKeyUsage, true, new ExtendedKeyUsage(
                    new KeyPurposeId[] {KeyPurposeId.id_kp_clientAuth, KeyPurposeId.id_kp_serverAuth}
                    )
              );



        String signatureAlgorithm = "SHA256WithRSA";

        // sign with own key
        ContentSigner contentSigner = new JcaContentSignerBuilder(signatureAlgorithm)
              .build(keyPair.getPrivate());

        X509CertificateHolder certificateHolder = certificateBuilder.build(contentSigner);
        var cert = new JcaX509CertificateConverter().getCertificate(certificateHolder);

        try (OutputStream certOut = Files.newOutputStream(
              certFile.toPath(), CREATE, TRUNCATE_EXISTING,
              WRITE); OutputStream keyOut = Files.newOutputStream(keyFile.toPath(), CREATE,
              TRUNCATE_EXISTING, WRITE)) {

            CertificateUtils.saveCertificate(certOut, cert, Encoding.PEM);
            CertificateUtils.savePrivateKey(keyOut, keyPair.getPrivate(), Encoding.PEM, null, null);
        }
    }

}