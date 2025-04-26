package com.github.lukaszbudnik.roxdb.grpc;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.lukaszbudnik.roxdb.application.RoxDBConfig;
import com.github.lukaszbudnik.roxdb.rocksdb.RoxDB;
import com.github.lukaszbudnik.roxdb.v1.RoxDBGrpc;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.TlsChannelCredentials;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.*;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.concurrent.TimeUnit;

class RoxDBServerTest {

  @TempDir Path tempDir;
  @Mock RoxDB roxDB;

  static {
    Security.addProvider(new BouncyCastleProvider());
  }

  @Test
  void testStartServer_Insecure() throws IOException, InterruptedException {
    // Arrange
    RoxDBConfig config = mock(RoxDBConfig.class);
    when(config.tlsCertificatePath()).thenReturn("");
    when(config.tlsPrivateKeyPath()).thenReturn("");
    when(config.port()).thenReturn(50052);

    RoxDB roxDB = mock(RoxDB.class);

    // Act
    RoxDBServer server = new RoxDBServer(config, new RoxDBGrpcService(roxDB));
    server.start();

    // create grpc health request
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress("localhost", config.port()).usePlaintext().build();

    // Create health check stub
    HealthGrpc.HealthBlockingStub healthStub = HealthGrpc.newBlockingStub(channel);

    // Create health check request
    HealthCheckRequest healthRequest =
        HealthCheckRequest.newBuilder()
            .setService(RoxDBGrpc.getServiceDescriptor().getName())
            .build();

    try {
      // Send health check request
      HealthCheckResponse response = healthStub.check(healthRequest);

      // Assert the response
      assertEquals(HealthCheckResponse.ServingStatus.SERVING, response.getStatus());
    } finally {
      // Clean up
      channel.shutdownNow();
      server.stop();

      HealthCheckResponse.ServingStatus serviceStatus = server.getServiceStatus();
      // NOT_SERVING after stop
      assertEquals(HealthCheckResponse.ServingStatus.NOT_SERVING, serviceStatus);
    }
  }

  @Test
  void testStartServer_WithTLS()
      throws IOException,
          InterruptedException,
          NoSuchAlgorithmException,
          CertificateException,
          OperatorCreationException {
    // Arrange
    // Create temporary certificate and key files
    Path certFile = createTempFile("cert.pem");
    Path keyFile = createTempFile("key.pem");

    createTestCertKey(certFile, keyFile);

    RoxDBConfig config = mock(RoxDBConfig.class);
    when(config.tlsCertificatePath()).thenReturn(certFile.toString());
    when(config.tlsPrivateKeyPath()).thenReturn(keyFile.toString());
    when(config.tlsCertificateChainPath()).thenReturn("");
    when(config.port()).thenReturn(50053);

    RoxDB roxDB = mock(RoxDB.class);

    // Act
    RoxDBServer server = new RoxDBServer(config, new RoxDBGrpcService(roxDB));
    server.start();

    TlsChannelCredentials.Builder tlsBuilder =
        TlsChannelCredentials.newBuilder().trustManager(certFile.toFile());

    // create grpc health request
    ManagedChannel channel =
        Grpc.newChannelBuilderForAddress("localhost", config.port(), tlsBuilder.build())
            .overrideAuthority("roxdb.localhost") // test cert is generated for CN=roxdb.localhost
            .build();

    // Create health check stub
    HealthGrpc.HealthBlockingStub healthStub = HealthGrpc.newBlockingStub(channel);

    // Create health check request
    HealthCheckRequest healthRequest =
        HealthCheckRequest.newBuilder()
            .setService(RoxDBGrpc.getServiceDescriptor().getName())
            .build();

    try {
      // Send health check request
      HealthCheckResponse response = healthStub.check(healthRequest);

      // Assert the response
      assertEquals(HealthCheckResponse.ServingStatus.SERVING, response.getStatus());
    } finally {
      // Clean up
      channel.shutdownNow();
      server.stop();
    }
  }

  @Test
  void testStartServer_WithMutualTLS()
      throws IOException,
          InterruptedException,
          NoSuchAlgorithmException,
          CertificateException,
          OperatorCreationException {
    // Arrange
    // Create temporary certificate and key files
    Path rootCACertPath = createTempFile("root_ca.crt");
    KeyPair rootCaKeyPair = generateKeyPair();
    X509Certificate rootCaCert = createRootCaCertificate(rootCaKeyPair);
    writePemFile(rootCaCert, rootCACertPath);
    // We don't strictly need the root CA key file for the gRPC config,
    // but keep it if you want to inspect it.

    KeyPair serverKeyPair = generateKeyPair();
    // Use "localhost" and "127.0.0.1" as SANs for the server certificate
    String[] serverSans = {
      "localhost", "127.0.0.1", "roxdb.localhost", "::1"
    }; // Include common loopback names
    X509Certificate serverCert =
        createSignedCertificate(
            "CN=roxdb.localhost", // Server Subject DN (CN still good practice)
            serverKeyPair.getPublic(),
            rootCaKeyPair.getPrivate(), // Signed by Root CA
            rootCaCert,
            true, // Is server cert
            serverSans // Server SANs
            );
    Path serverCertPath = createTempFile("server.crt");
    Path serverKeyPath = createTempFile("server.key");
    writePemFile(serverCert, serverCertPath);
    writePemFile(serverKeyPair.getPrivate(), serverKeyPath);

    KeyPair clientKeyPair = generateKeyPair();
    X509Certificate clientCert =
        createSignedCertificate(
            "CN=grpc-client-test", // Client Subject DN
            clientKeyPair.getPublic(),
            rootCaKeyPair.getPrivate(), // Signed by Root CA
            rootCaCert,
            false, // Is client cert
            null // No SANs for client in this case
            );
    Path clientCertPath = createTempFile("client.crt");
    Path clientKeyPath = createTempFile("client.key");
    writePemFile(clientCert, clientCertPath);
    writePemFile(clientKeyPair.getPrivate(), clientKeyPath);

    RoxDBConfig config = mock(RoxDBConfig.class);
    when(config.tlsCertificatePath()).thenReturn(serverCertPath.toString());
    when(config.tlsPrivateKeyPath()).thenReturn(serverKeyPath.toString());
    when(config.tlsCertificateChainPath()).thenReturn(rootCACertPath.toString());
    when(config.port()).thenReturn(50054);

    RoxDB roxDB = mock(RoxDB.class);

    // Act
    RoxDBServer server = new RoxDBServer(config, new RoxDBGrpcService(roxDB));
    server.start();

    TlsChannelCredentials.Builder tlsBuilder =
        TlsChannelCredentials.newBuilder()
            .trustManager(rootCACertPath.toFile())
            .keyManager(clientCertPath.toFile(), clientKeyPath.toFile());

    // create grpc health request
    ManagedChannel channel =
        Grpc.newChannelBuilderForAddress("localhost", config.port(), tlsBuilder.build())
            .overrideAuthority("roxdb.localhost") // test cert is generated for CN=roxdb.localhost
            .build();

    // Create health check stub
    HealthGrpc.HealthBlockingStub healthStub = HealthGrpc.newBlockingStub(channel);

    // Create health check request
    HealthCheckRequest healthRequest =
        HealthCheckRequest.newBuilder()
            .setService(RoxDBGrpc.getServiceDescriptor().getName())
            .build();

    try {
      // Send health check request
      HealthCheckResponse response = healthStub.check(healthRequest);

      // Assert the response
      assertEquals(HealthCheckResponse.ServingStatus.SERVING, response.getStatus());
    } finally {
      // Clean up
      channel.shutdownNow();
      server.stop();
    }
  }

  private Path createTempFile(String fileName) throws IOException {
    Path filePath = tempDir.resolve(fileName);
    Files.createFile(filePath);
    return filePath;
  }

  private void createTestCertKey(Path certFile, Path keyFile)
      throws NoSuchAlgorithmException,
          OperatorCreationException,
          CertificateException,
          IOException {
    // Generate key pair
    KeyPair keyPair = generateKeyPair();

    // Certificate validity period
    Instant now = Instant.now();
    Date startDate = Date.from(now);
    Date endDate = Date.from(now.plus(365, ChronoUnit.DAYS));

    // Create X500Name for subject
    X500Name subject = new X500Name("C=PL,CN=roxdb.localhost,O=roxdb");

    // Create certificate builder
    X509v3CertificateBuilder certBuilder =
        new X509v3CertificateBuilder(
            subject, // issuer = subject for self-signed
            BigInteger.valueOf(System.currentTimeMillis()), // serial number
            startDate,
            endDate,
            subject, // subject
            SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded()));

    // Create certificate signer
    ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(keyPair.getPrivate());

    // Generate certificate
    X509CertificateHolder certHolder = certBuilder.build(signer);
    X509Certificate cert = new JcaX509CertificateConverter().getCertificate(certHolder);

    // Save private key
    writePemFile(keyPair.getPrivate(), keyFile);

    // Save certificate
    writePemFile(cert, certFile);
  }

  private KeyPair generateKeyPair() throws NoSuchAlgorithmException {
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    keyPairGenerator.initialize(4096);
    return keyPairGenerator.generateKeyPair();
  }

  private void writePemFile(Object obj, Path path) throws IOException {
    try (JcaPEMWriter pemWriter = new JcaPEMWriter(new FileWriter(path.toFile()))) {
      pemWriter.writeObject(obj);
    }
  }

  private X509Certificate createRootCaCertificate(KeyPair keyPair)
      throws OperatorCreationException,
          CertificateException,
          CertIOException,
          NoSuchAlgorithmException {

    Date now = new Date();
    Date notBefore = new Date(now.getTime() - TimeUnit.DAYS.toMillis(1));
    Date notAfter = new Date(now.getTime() + TimeUnit.DAYS.toMillis(365 * 10)); // 10 years

    X500Name issuerName = new X500Name("CN=Mutual TLS Test Root CA");
    X500Name subjectName = issuerName;

    BigInteger serialNumber = BigInteger.valueOf(System.currentTimeMillis());

    X509v3CertificateBuilder certBuilder =
        new JcaX509v3CertificateBuilder(
            issuerName, serialNumber, notBefore, notAfter, subjectName, keyPair.getPublic());

    JcaX509ExtensionUtils extUtils = new JcaX509ExtensionUtils();
    certBuilder.addExtension(Extension.basicConstraints, true, new BasicConstraints(0));
    certBuilder.addExtension(
        Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
    certBuilder.addExtension(
        Extension.subjectKeyIdentifier,
        false,
        extUtils.createSubjectKeyIdentifier(keyPair.getPublic()));

    JcaContentSignerBuilder signerBuilder =
        new JcaContentSignerBuilder("SHA256withRSA").setProvider("BC");
    ContentSigner contentSigner = signerBuilder.build(keyPair.getPrivate());

    X509CertificateHolder certHolder = certBuilder.build(contentSigner);
    return new JcaX509CertificateConverter().setProvider("BC").getCertificate(certHolder);
  }

  /** Creates a certificate signed by a given issuer CA. */
  private X509Certificate createSignedCertificate(
      String subjectDn,
      PublicKey publicKey,
      PrivateKey issuerPrivateKey,
      X509Certificate issuerCert,
      boolean isServerCert,
      String[] subjectAlternativeNames)
      throws OperatorCreationException,
          CertificateException,
          CertIOException,
          NoSuchAlgorithmException {

    Date now = new Date();
    Date notBefore = new Date(now.getTime() - TimeUnit.DAYS.toMillis(1));
    Date notAfter = new Date(now.getTime() + TimeUnit.DAYS.toMillis(365 * 2)); // 2 years

    X500Name subjectName = new X500Name(subjectDn);
    X500Name issuerName = new X500Name(issuerCert.getSubjectX500Principal().getName());

    BigInteger serialNumber =
        BigInteger.valueOf(
            System.currentTimeMillis() + (isServerCert ? 100 : 200)); // Ensure uniqueness

    X509v3CertificateBuilder certBuilder =
        new JcaX509v3CertificateBuilder(
            issuerName, serialNumber, notBefore, notAfter, subjectName, publicKey);

    JcaX509ExtensionUtils extUtils = new JcaX509ExtensionUtils();
    certBuilder.addExtension(Extension.basicConstraints, true, new BasicConstraints(false));

    KeyUsage keyUsage;
    if (isServerCert) {
      keyUsage = new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment);
    } else {
      keyUsage =
          new KeyUsage(
              KeyUsage.digitalSignature
                  | KeyUsage.keyAgreement); // keyAgreement often useful for clients
    }
    certBuilder.addExtension(Extension.keyUsage, true, keyUsage);

    certBuilder.addExtension(
        Extension.subjectKeyIdentifier, false, extUtils.createSubjectKeyIdentifier(publicKey));
    certBuilder.addExtension(
        Extension.authorityKeyIdentifier, false, extUtils.createAuthorityKeyIdentifier(issuerCert));

    // Add Subject Alternative Names (SANs) if provided
    if (subjectAlternativeNames != null && subjectAlternativeNames.length > 0) {
      GeneralName[] generalNames = new GeneralName[subjectAlternativeNames.length];
      for (int i = 0; i < subjectAlternativeNames.length; i++) {
        String name = subjectAlternativeNames[i];
        if (name.matches("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}")) {
          generalNames[i] = new GeneralName(GeneralName.iPAddress, name);
        } else if (name.contains(":")) { // Simple check for potential IPv6
          generalNames[i] = new GeneralName(GeneralName.iPAddress, name);
        } else {
          generalNames[i] = new GeneralName(GeneralName.dNSName, name);
        }
      }
      certBuilder.addExtension(
          Extension.subjectAlternativeName, true, new GeneralNames(generalNames));
    }

    JcaContentSignerBuilder signerBuilder =
        new JcaContentSignerBuilder("SHA256withRSA").setProvider("BC");
    ContentSigner contentSigner = signerBuilder.build(issuerPrivateKey);

    X509CertificateHolder certHolder = certBuilder.build(contentSigner);
    return new JcaX509CertificateConverter().setProvider("BC").getCertificate(certHolder);
  }
}
