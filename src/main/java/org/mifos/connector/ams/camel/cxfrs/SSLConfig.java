package org.mifos.connector.ams.camel.cxfrs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.apache.camel.support.jsse.ClientAuthentication;
import org.apache.camel.support.jsse.KeyManagersParameters;
import org.apache.camel.support.jsse.KeyStoreParameters;
import org.apache.camel.support.jsse.SSLContextParameters;
import org.apache.camel.support.jsse.SSLContextServerParameters;
import org.apache.camel.support.jsse.TrustManagersParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;

@Component
// @ConditionalOnExpression("${ams.local.enabled}")
public class SSLConfig {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private String keystorePassword;
    private File keyStoreFile;
    private boolean checkServerCert;

    public SSLConfig(@Value("${ams.local.keystore-path}") String keystorePath,
            @Value("${ams.local.keystore-password}") String keystorePassword,
            @Value("${ams.local.server-cert-check}") boolean checkServerCert) {
        this.keystorePassword = keystorePassword;
        keyStoreFile = new FileSystemResource(keystorePath).getFile();
        this.checkServerCert = checkServerCert;
    }

    public SSLContextParameters provideSSLContextParameters() {
        Optional<KeyManagersParameters> keyManagerParameter = getKeyManagerParameter();
        TrustManagersParameters trustManagerParameter = getTrustManagerParameter();

        SSLContextParameters ssl = new SSLContextParameters();
        keyManagerParameter.ifPresent(ssl::setKeyManagers);
        ssl.setTrustManagers(trustManagerParameter);

        SSLContextServerParameters serverParameters = new SSLContextServerParameters();
        serverParameters.setClientAuthentication(ClientAuthentication.REQUIRE.name());
        ssl.setServerParameters(serverParameters);

        return ssl;
    }

    private TrustManagersParameters getTrustManagerParameter() {
        TrustManagersParameters trustManagers = new TrustManagersParameters();
        trustManagers.setTrustManager(createCompositeTrustManager());
        return trustManagers;
    }

    private CompositeX509TrustManager createCompositeTrustManager() {
        List<X509TrustManager> trustManagers = Stream
                .concat(Stream.of(tryToGetApplicationTrustManagerTrustManager()), Stream.of(tryToGetJavaTrustManager()))
                .filter(X509TrustManager.class::isInstance).map(X509TrustManager.class::cast).collect(Collectors.toList());
        return new CompositeX509TrustManager(trustManagers, checkServerCert);
    }

    private X509TrustManager[] tryToGetJavaTrustManager() {
        try {
            return getJavaDefaultTrustStore();
        } catch (NoSuchAlgorithmException | KeyStoreException e) {
            logger.error("Cannot get Java TrustManager", e);
            return new X509TrustManager[0];
        }
    }

    private X509TrustManager[] getJavaDefaultTrustStore() throws NoSuchAlgorithmException, KeyStoreException {
        String defaultAlgorithm = KeyManagerFactory.getDefaultAlgorithm();
        TrustManagerFactory factory = TrustManagerFactory.getInstance(defaultAlgorithm);
        factory.init((KeyStore) null);
        return Stream.of(factory.getTrustManagers()).filter(X509TrustManager.class::isInstance).map(X509TrustManager.class::cast)
                .findFirst().map(it -> new X509TrustManager[] { it }).orElse(new X509TrustManager[0]);
    }

    private Optional<KeyManagersParameters> getKeyManagerParameter() {
        if (keyStoreFile != null) {
            KeyStoreParameters keyStore = new KeyStoreParameters();
            keyStore.setResource(keyStoreFile.toString());
            keyStore.setPassword(keystorePassword);

            KeyManagersParameters keyManagers = new KeyManagersParameters();
            keyManagers.setKeyStore(keyStore);
            keyManagers.setKeyPassword(keystorePassword);
            return Optional.of(keyManagers);
        } else {
            return Optional.empty();
        }
    }

    private TrustManager[] tryToGetApplicationTrustManagerTrustManager() {
        try {
            return getApplicationTrustManager();
        } catch (InterruptedException | CertificateException | NoSuchAlgorithmException | KeyStoreException | IOException e) {
            throw new RuntimeException("Cannot replace the TrustManager", e);
        }
    }

    private TrustManager[] getApplicationTrustManager()
            throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException, InterruptedException {
        if (keyStoreFile == null) {
            return new TrustManager[0];
        }

        InputStream trustStream = new FileInputStream(keyStoreFile);
        char[] trustPassword = keystorePassword.toCharArray();

        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(trustStream, trustPassword);

        TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustFactory.init(trustStore);

        return trustFactory.getTrustManagers();
    }
}
