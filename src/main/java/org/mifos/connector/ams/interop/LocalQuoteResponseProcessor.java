package org.mifos.connector.ams.interop;

import io.zeebe.client.ZeebeClient;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import static org.mifos.connector.ams.camel.config.CamelProperties.EXTERNAL_ACCOUNT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.LOCAL_QUOTE_RESPONSE;
import static org.mifos.connector.ams.camel.config.CamelProperties.TENANT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.ZEEBE_JOB_KEY;

@Component
@ConditionalOnExpression("${ams.local.enabled}")
public class LocalQuoteResponseProcessor implements Processor {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private ZeebeClient zeebeClient;

    @Override
    public void process(Exchange exchange) {
        Integer responseCode = exchange.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class);
        Long jobKey = exchange.getProperty(ZEEBE_JOB_KEY, Long.class);
        if (responseCode > 202) {
            String errorMsg = String.format("Invalid responseCode %s for payee-local-quote transaction: %s Message: %s",
                    responseCode,
                    exchange.getProperty(TRANSACTION_ID),
                    exchange.getIn().getBody(String.class));

            logger.error(errorMsg);

            zeebeClient.newThrowErrorCommand(jobKey)
                    .errorCode(ZeebeErrorCode.PAYEE_QUOTE_ERROR)
                    .errorMessage(errorMsg)
                    .send();
        } else {
            Map<String, Object> variables = new HashMap<>();
            variables.put(LOCAL_QUOTE_RESPONSE, exchange.getIn().getBody());
            variables.put(EXTERNAL_ACCOUNT_ID, exchange.getProperty(EXTERNAL_ACCOUNT_ID));
            variables.put(TENANT_ID, exchange.getProperty(TENANT_ID));

            zeebeClient.newCompleteCommand(jobKey)
                    .variables(variables)
                    .send();
        }
    }
}