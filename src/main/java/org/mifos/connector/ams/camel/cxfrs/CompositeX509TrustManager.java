package org.mifos.connector.ams.camel.cxfrs;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import javax.net.ssl.X509TrustManager;

public class CompositeX509TrustManager implements X509TrustManager {

    private final List<X509TrustManager> trustManagers;
    private boolean checkServerCert;

    public CompositeX509TrustManager(List<X509TrustManager> trustManagers, boolean checkServerCert) {
        this.trustManagers = Collections.unmodifiableList(new ArrayList<>(trustManagers));
        this.checkServerCert = checkServerCert;
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        for (X509TrustManager trustManager : trustManagers) {
            try {
                trustManager.checkClientTrusted(chain, authType);
                return; // someone trusts them. success!
            } catch (CertificateException e) {
                // maybe someone else will trust them
            }
        }
        throw new CertificateException("None of the TrustManagers trust the clients certificate chain!");
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        if (checkServerCert) {
            for (X509TrustManager trustManager : trustManagers) {
                try {
                    trustManager.checkServerTrusted(chain, authType);
                    return; // someone trusts them. success!
                } catch (CertificateException e) {
                    // maybe someone else will trust them
                }
            }
            throw new CertificateException("None of the TrustManagers trust the servers certificate chain!");
        }
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return trustManagers.stream().flatMap(it -> Stream.of(it.getAcceptedIssuers())).toArray(X509Certificate[]::new);
    }

}
