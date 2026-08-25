package org.dcache.nearline.cta;

import eu.emi.security.authn.x509.impl.CertificateUtils;
import org.bouncycastle.asn1.DEROctetString;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.*;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

public final class TestUtils {

    private TestUtils() {
    }


    /**
     * A simple test CA that can generate server and client certificates signed by itself.
     */
    static class TestCA {

        /**
         * The signature algorithm used to sign the certificates.
         */
        private final String signatureAlgorithm = "SHA256WithRSA";

        /**
         * The serial number of the next certificate to be generated.
         */
        private final AtomicLong serialNumber = new AtomicLong(1);

        /**
         * The key pair of the CA.
         */
        private final KeyPair caKeyPair;

        /**
         * The certificate of the CA.
         */
        private final X509CertificateHolder certificateHolder;

        public TestCA() throws NoSuchAlgorithmException, CertIOException, OperatorCreationException {
            KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA", new BouncyCastleProvider());
            keyPairGenerator.initialize(2048, new SecureRandom());
            caKeyPair = keyPairGenerator.generateKeyPair();

            long notBefore = System.currentTimeMillis();
            long notAfter = notBefore + TimeUnit.DAYS.toMillis(1);

            X500Name caSubjectDN = new X500Name("CN=Embedded Test CA, O=dCache.org");

            SubjectPublicKeyInfo subjectPublicKeyInfo =
                    SubjectPublicKeyInfo.getInstance(caKeyPair.getPublic().getEncoded());

            X509v3CertificateBuilder certificateBuilder = new X509v3CertificateBuilder(caSubjectDN,
                    BigInteger.valueOf(serialNumber.getAndIncrement()),
                    new Date(notBefore),
                    new Date(notAfter), caSubjectDN,
                    subjectPublicKeyInfo)
                    .addExtension(Extension.basicConstraints, true, new BasicConstraints(true))
                    .addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment | KeyUsage.keyCertSign | KeyUsage.cRLSign));

            // sign with own key
            ContentSigner contentSigner = new JcaContentSignerBuilder(signatureAlgorithm)
                    .build(caKeyPair.getPrivate());

            certificateHolder = certificateBuilder.build(contentSigner);
        }

        public void generateCAChain(File chainFile) throws IOException, CertificateException {

            var cert = new JcaX509CertificateConverter().getCertificate(certificateHolder);

            try (OutputStream certOut = Files.newOutputStream(
                    chainFile.toPath(), CREATE, TRUNCATE_EXISTING,
                    WRITE)) {
                CertificateUtils.saveCertificate(certOut, cert, CertificateUtils.Encoding.PEM);
            }
        }

        public void generateServerCert(File certFile, File keyFile) throws NoSuchAlgorithmException, IOException, OperatorCreationException, CertificateException {

            KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA", new BouncyCastleProvider());
            keyPairGenerator.initialize(2048, new SecureRandom());
            KeyPair keyPair = keyPairGenerator.generateKeyPair();

            long notBefore = System.currentTimeMillis();
            long notAfter = notBefore + TimeUnit.DAYS.toMillis(1);

            X500Name subjectDN = new X500Name("CN=localhost, O=dCache.org");

            SubjectPublicKeyInfo subjectPublicKeyInfo =
                    SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded());

            X509v3CertificateBuilder certificateBuilder = new X509v3CertificateBuilder(certificateHolder.getSubject(),
                    BigInteger.valueOf(serialNumber.getAndIncrement()),
                    new Date(notBefore),
                    new Date(notAfter), subjectDN,
                    subjectPublicKeyInfo)
                    .addExtension(Extension.subjectAlternativeName, true, new GeneralNames(new GeneralName[]{
                            new GeneralName(GeneralName.dNSName, "localhost"),
                            new GeneralName(GeneralName.dNSName, "localhost4"),
                            new GeneralName(GeneralName.dNSName, "localhost6"),
                            new GeneralName(GeneralName.iPAddress, new DEROctetString(new byte[]{127, 0, 0, 1})),
                            new GeneralName(GeneralName.iPAddress, new DEROctetString(new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}))}))
                    .addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment))
                    .addExtension(Extension.extendedKeyUsage, true, new ExtendedKeyUsage(
                                    new KeyPurposeId[]{KeyPurposeId.id_kp_serverAuth}
                            )
                    );

            ContentSigner contentSigner = new JcaContentSignerBuilder(signatureAlgorithm)
                    .build(caKeyPair.getPrivate());

            X509CertificateHolder certificateHolder = certificateBuilder.build(contentSigner);
            var cert = new JcaX509CertificateConverter().getCertificate(certificateHolder);

            try (OutputStream certOut = Files.newOutputStream(
                    certFile.toPath(), CREATE, TRUNCATE_EXISTING,
                    WRITE); OutputStream keyOut = Files.newOutputStream(keyFile.toPath(), CREATE,
                    TRUNCATE_EXISTING, WRITE)) {
                CertificateUtils.saveCertificate(certOut, cert, CertificateUtils.Encoding.PEM);
                CertificateUtils.savePrivateKey(keyOut, keyPair.getPrivate(), CertificateUtils.Encoding.PEM, null, null);
            }
        }

        public void generateClientCert(File certFile, File keyFile, String commonName) throws NoSuchAlgorithmException, IOException, OperatorCreationException, CertificateException {

            KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA", new BouncyCastleProvider());
            keyPairGenerator.initialize(2048, new SecureRandom());
            KeyPair keyPair = keyPairGenerator.generateKeyPair();

            long notBefore = System.currentTimeMillis();
            long notAfter = notBefore + TimeUnit.DAYS.toMillis(1);

            X500Name subjectDN = new X500Name("CN=" + commonName + ", O=dCache.org");

            SubjectPublicKeyInfo subjectPublicKeyInfo =
                    SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded());

            X509v3CertificateBuilder certificateBuilder = new X509v3CertificateBuilder(certificateHolder.getSubject(),
                    BigInteger.valueOf(serialNumber.getAndIncrement()),
                    new Date(notBefore),
                    new Date(notAfter), subjectDN,
                    subjectPublicKeyInfo)
                    .addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment))
                    .addExtension(Extension.extendedKeyUsage, true, new ExtendedKeyUsage(
                                    new KeyPurposeId[]{KeyPurposeId.id_kp_clientAuth}
                            )
                    );

            ContentSigner contentSigner = new JcaContentSignerBuilder(signatureAlgorithm)
                    .build(caKeyPair.getPrivate());

            X509CertificateHolder certificateHolder = certificateBuilder.build(contentSigner);
            var cert = new JcaX509CertificateConverter().getCertificate(certificateHolder);

            try (OutputStream certOut = Files.newOutputStream(
                    certFile.toPath(), CREATE, TRUNCATE_EXISTING,
                    WRITE); OutputStream keyOut = Files.newOutputStream(keyFile.toPath(), CREATE,
                    TRUNCATE_EXISTING, WRITE)) {
                CertificateUtils.saveCertificate(certOut, cert, CertificateUtils.Encoding.PEM);
                CertificateUtils.savePrivateKey(keyOut, keyPair.getPrivate(), CertificateUtils.Encoding.PEM, null, null);
            }
        }
    }
}
