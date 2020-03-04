package org.mifos.connector.ams.camel.local_quote;

import io.zeebe.client.ZeebeClient;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.mifos.common.ams.dto.QuoteFspResponseDTO;
import org.mifos.connector.ams.camel.config.CamelProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class LocalQuoteResponseProcessor implements Processor {

    @Autowired
    private ZeebeClient zeebeClient;

    @Override
    public void process(Exchange exchange) throws Exception {
        QuoteFspResponseDTO response = exchange.getIn().getBody(QuoteFspResponseDTO.class);

        zeebeClient.newPublishMessageCommand()
                .messageName("accept-quote")
                .correlationKey(exchange.getProperty(CamelProperties.TRANSACTION_ID, String.class))
                .timeToLive(Duration.ofMillis(30000))
                .send();
    }
}
