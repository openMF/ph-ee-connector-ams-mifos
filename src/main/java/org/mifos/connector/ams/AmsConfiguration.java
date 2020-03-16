package org.mifos.connector.ams;

import org.apache.camel.support.jsse.SSLContextParameters;
import org.mifos.connector.ams.camel.cxfrs.SSLConfig;
import org.mifos.connector.ams.properties.TenantProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.Profile;

@Profile({"fin12", "fincn"})
@Configuration
@EnableConfigurationProperties(TenantProperties.class)
@ImportResource("classpath:endpoints.xml")
public class AmsConfiguration {

    @Bean
    public SSLContextParameters sslContextParameters(SSLConfig sslConfig) {
        return sslConfig.provideSSLContextParameters();
    }
}
