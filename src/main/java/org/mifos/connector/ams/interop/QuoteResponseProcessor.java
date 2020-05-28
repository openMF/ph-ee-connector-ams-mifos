package org.mifos.connector.ams.interop;

import io.zeebe.client.ZeebeClient;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.mifos.connector.common.mojaloop.type.TransactionRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import static org.mifos.connector.ams.camel.config.CamelProperties.ERROR_INFORMATION;
import static org.mifos.connector.ams.camel.config.CamelProperties.EXTERNAL_ACCOUNT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.LOCAL_QUOTE_RESPONSE;
import static org.mifos.connector.ams.camel.config.CamelProperties.TENANT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ROLE;
import static org.mifos.connector.ams.camel.config.CamelProperties.ZEEBE_JOB_KEY;
import static org.mifos.connector.ams.zeebe.ZeebeExpressionVariables.LOCAL_QUOTE_FAILED;
import static org.mifos.connector.ams.zeebe.ZeebeExpressionVariables.QUOTE_FAILED;
import static org.mifos.connector.common.camel.ErrorHandlerRouteBuilder.createError;
import static org.mifos.connector.common.mojaloop.type.ErrorCode.PAYEE_FSP_REJECTED_QUOTE;
import static org.mifos.connector.common.mojaloop.type.ErrorCode.PAYER_FSP_INSUFFICIENT_LIQUIDITY;

@Component
@ConditionalOnExpression("${ams.local.enabled}")
public class QuoteResponseProcessor implements Processor {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private ZeebeClient zeebeClient;

    @Override
    public void process(Exchange exchange) throws Exception {
        Integer responseCode = exchange.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class);
        Long jobKey = exchange.getProperty(ZEEBE_JOB_KEY, Long.class);
        String tranactionRole = exchange.getProperty(TRANSACTION_ROLE, String.class);
        if (responseCode > 202) {
            String errorMsg = String.format("Invalid responseCode %s for quote on %s side, transactionId: %s Message: %s",
                    responseCode,
                    tranactionRole,
                    exchange.getProperty(TRANSACTION_ID),
                    exchange.getIn().getBody(String.class));

            logger.error(errorMsg);

            final String errorCode;
            final String errorKey;
            if(tranactionRole.equals(TransactionRole.PAYER.name())) {
                errorCode = String.valueOf(PAYER_FSP_INSUFFICIENT_LIQUIDITY.getCode());
                errorKey = LOCAL_QUOTE_FAILED;
            } else { // payee
                errorCode = String.valueOf(PAYEE_FSP_REJECTED_QUOTE.getCode());
                errorKey = QUOTE_FAILED;
            }

            Map<String, Object> variables = new HashMap<>();
            variables.put(ERROR_INFORMATION, createError(errorCode, errorMsg).toString());
            variables.put(errorKey, true);

            zeebeClient.newCompleteCommand(jobKey)
                    .variables(variables)
                    .send()
                    .join();
        } else {
            Map<String, Object> variables = new HashMap<>();
            variables.put(LOCAL_QUOTE_RESPONSE, exchange.getIn().getBody());
            variables.put(EXTERNAL_ACCOUNT_ID, exchange.getProperty(EXTERNAL_ACCOUNT_ID));
            variables.put(TENANT_ID, exchange.getProperty(TENANT_ID));
            variables.put(tranactionRole.equals(TransactionRole.PAYER.name()) ? LOCAL_QUOTE_FAILED : QUOTE_FAILED, false);

            zeebeClient.newCompleteCommand(jobKey)
                    .variables(variables)
                    .send()
                    .join();
        }
    }
}