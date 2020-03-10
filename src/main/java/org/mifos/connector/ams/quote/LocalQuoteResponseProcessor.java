package org.mifos.connector.ams.quote;

import io.zeebe.client.ZeebeClient;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import static org.mifos.connector.ams.camel.config.CamelProperties.LOCAL_QUOTE_RESPONSE;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.ZEEBE_JOB_KEY;

@Component
public class LocalQuoteResponseProcessor implements Processor {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private ZeebeClient zeebeClient;

    @Override
    public void process(Exchange exchange) throws Exception {
        Integer responseCode = exchange.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class);
        Long jobKey = exchange.getProperty(ZEEBE_JOB_KEY, Long.class);
        if (responseCode > 202) {
            logger.error("Invalid responseCode {} for local quote, transaction: {} Message: {}",
                    responseCode,
                    exchange.getProperty(TRANSACTION_ID),
                    exchange.getIn().getBody(String.class));

            zeebeClient.newThrowErrorCommand(jobKey)
                    .errorCode("local-quote-error")
                    .errorMessage("Local quote failed!")
                    .send();
        } else {
            Map<String, Object> variables = new HashMap<>();
            variables.put(LOCAL_QUOTE_RESPONSE, exchange.getIn().getBody(String.class));

            zeebeClient.newCompleteCommand(jobKey)
                    .variables(variables)
                    .send();
        }
    }
}