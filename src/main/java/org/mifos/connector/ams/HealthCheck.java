package org.mifos.connector.ams;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Collection;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static org.apache.camel.health.HealthCheck.HTTP_RESPONSE_CODE;
import static org.apache.camel.health.HealthCheckHelper.invokeLiveness;
import static org.apache.camel.health.HealthCheckHelper.invokeReadiness;

@Component
public class HealthCheck extends RouteBuilder {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void configure() {
        from("rest:GET:/health/readiness")
                .id("app.health.readiness")
                .setHeader(CONTENT_TYPE, constant("application/json"))
                .process(exchange -> setHealthCheckResult(exchange, invokeReadiness(getContext())));

        from("rest:GET:/health/liveness")
                .id("app.health.liveness")
                .setHeader(CONTENT_TYPE, constant("application/json"))
                .process(exchange -> setHealthCheckResult(exchange, invokeLiveness(getContext())));

        logger.info("liveness / readiness health checks configured");
    }

    private void setHealthCheckResult(Exchange exchange, Collection<org.apache.camel.health.HealthCheck.Result> results) {
        if (results.stream().anyMatch(it -> it.getState() != org.apache.camel.health.HealthCheck.State.UP)) {
            exchange.getMessage().setHeader(HTTP_RESPONSE_CODE, 503);
            logger.warn("health check failed with {}", results);
        }

        exchange.getMessage().setBody(results);
    }
}
