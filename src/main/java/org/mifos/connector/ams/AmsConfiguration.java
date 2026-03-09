package org.mifos.connector.ams;

import org.apache.camel.support.jsse.SSLContextParameters;
import org.mifos.connector.ams.camel.cxfrs.SSLConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.Profile;

@Profile({ "fin12", "fincn" })
@Configuration
// @ConditionalOnExpression("${ams.local.enabled}")
@ImportResource("classpath:endpoints.xml")
public class AmsConfiguration {

    @Bean
    public SSLContextParameters sslContextParameters(SSLConfig sslConfig) {
        return sslConfig.provideSSLContextParameters();
    }
}
