package org.mifos.connector.ams.interop;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.zeebe.client.ZeebeClient;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.json.JSONObject;
import org.mifos.phee.common.mojaloop.dto.ErrorInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import static org.mifos.connector.ams.camel.config.CamelProperties.ERROR_INFORMATION;
import static org.mifos.connector.ams.camel.config.CamelProperties.EXTERNAL_ACCOUNT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.IS_QUOTE_SUCCESS;
import static org.mifos.connector.ams.camel.config.CamelProperties.LOCAL_QUOTE_RESPONSE;
import static org.mifos.connector.ams.camel.config.CamelProperties.TENANT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.ZEEBE_JOB_KEY;
import static org.mifos.phee.common.mojaloop.type.ErrorCode.PARTY_NOT_FOUND;
import static org.mifos.phee.common.mojaloop.type.ErrorCode.PAYEE_FSP_REJECTED_QUOTE;

@Component
@ConditionalOnExpression("${ams.local.enabled}")
public class LocalQuoteResponseProcessor implements Processor {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private ZeebeClient zeebeClient;

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public void process(Exchange exchange) throws Exception {
        Integer responseCode = exchange.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class);
        Long jobKey = exchange.getProperty(ZEEBE_JOB_KEY, Long.class);
        if (responseCode > 202) {
            String errorMsg = String.format("Invalid responseCode %s for payee-local-quote transaction: %s Message: %s",
                    responseCode,
                    exchange.getProperty(TRANSACTION_ID),
                    exchange.getIn().getBody(String.class));

            logger.error(errorMsg);

            Map<String, Object> variables = new HashMap<>();
            JSONObject errorObject = new JSONObject();
            JSONObject error = new JSONObject();
            error.put("errorCode", String.valueOf(PAYEE_FSP_REJECTED_QUOTE.getCode()));
            error.put("errorDescription", errorMsg);
            errorObject.put("errorInformation", error);
            variables.put(ERROR_INFORMATION, objectMapper.writeValueAsString(errorObject));
            variables.put(IS_QUOTE_SUCCESS, false);

            zeebeClient.newCompleteCommand(jobKey)
                    .variables(variables)
                    .send();
        } else {
            Map<String, Object> variables = new HashMap<>();
            variables.put(LOCAL_QUOTE_RESPONSE, exchange.getIn().getBody());
            variables.put(EXTERNAL_ACCOUNT_ID, exchange.getProperty(EXTERNAL_ACCOUNT_ID));
            variables.put(TENANT_ID, exchange.getProperty(TENANT_ID));
            variables.put(IS_QUOTE_SUCCESS, true);

            zeebeClient.newCompleteCommand(jobKey)
                    .variables(variables)
                    .send();
        }
    }
}