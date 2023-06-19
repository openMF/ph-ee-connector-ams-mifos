package org.mifos.connector.ams.camel.config;

import org.apache.camel.CamelContext;
import org.apache.camel.health.HealthCheckRegistry;
import org.apache.camel.impl.health.DefaultHealthCheckRegistry;
import org.apache.camel.impl.health.RoutesHealthCheckRepository;
import org.apache.camel.spi.RestConfiguration;
import org.apache.camel.spring.boot.CamelContextConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;

@Configuration
public class CamelContextConfig {

    @Value("${camel.server-port:5000}")
    private int serverPort;

    @Bean
    CamelContextConfiguration contextConfiguration() {
        return new CamelContextConfiguration() {
            @Override
            public void beforeApplicationStart(CamelContext camelContext) {
                camelContext.setTracing(false);
                camelContext.setMessageHistory(false);
                camelContext.setStreamCaching(true);
                camelContext.disableJMX();

                DefaultHealthCheckRegistry checkRegistry = new DefaultHealthCheckRegistry();
                checkRegistry.register(new RoutesHealthCheckRepository());
                camelContext.getCamelContextExtension().addContextPlugin(HealthCheckRegistry.class, checkRegistry);

                RestConfiguration rest = new RestConfiguration();
                camelContext.setRestConfiguration(rest);
                rest.setComponent("jetty");
                rest.setProducerComponent("jetty");
                rest.setPort(serverPort);
                rest.setBindingMode(RestConfiguration.RestBindingMode.json);
                rest.setDataFormatProperties(new HashMap<>());
                rest.getDataFormatProperties().put("prettyPrint", "true");
                rest.setScheme("http");
            }

            @Override
            public void afterApplicationStart(CamelContext camelContext) {
                // empty
            }
        };
    }
}
