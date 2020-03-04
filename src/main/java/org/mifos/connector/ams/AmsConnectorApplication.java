package org.mifos.connector.ams;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Processor;
import org.apache.camel.support.jsse.SSLContextParameters;
import org.mifos.connector.ams.camel.cxfrs.SSLConfig;
import org.mifos.connector.ams.properties.TenantProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportResource;

@SpringBootApplication
@EnableConfigurationProperties(TenantProperties.class)
@ImportResource("classpath:endpoints.xml")
public class AmsConnectorApplication {

    public static void main(String[] args) {
        SpringApplication.run(AmsConnectorApplication.class, args);
    }

    @Bean
    public SSLContextParameters sslContextParameters(SSLConfig sslConfig) {
        return sslConfig.provideSSLContextParameters();
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
    }

    @Bean
    public Processor pojoToString(ObjectMapper objectMapper) {
        return exchange -> exchange.getIn().setBody(objectMapper.writeValueAsString(exchange.getIn().getBody()));
    }
}
