package org.mifos.connector.ams.interop;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.zeebe.client.ZeebeClient;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.mifos.connector.common.ams.dto.QuoteFspResponseDTO;
import org.mifos.connector.common.mojaloop.type.TransactionRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ROLE;
import static org.mifos.connector.ams.camel.config.CamelProperties.ZEEBE_JOB_KEY;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.ERROR_INFORMATION;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.EXTERNAL_ACCOUNT_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.LOCAL_QUOTE_FAILED;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.LOCAL_QUOTE_RESPONSE;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.QUOTE_FAILED;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TENANT_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TRANSACTION_ID;
import static org.mifos.connector.common.camel.ErrorHandlerRouteBuilder.createError;
import static org.mifos.connector.common.mojaloop.type.ErrorCode.PAYEE_FSP_REJECTED_QUOTE;
import static org.mifos.connector.common.mojaloop.type.ErrorCode.PAYER_FSP_INSUFFICIENT_LIQUIDITY;

@Component
@ConditionalOnExpression("${ams.local.enabled}")
public class QuoteResponseProcessor implements Processor {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired(required = false)
    private ZeebeClient zeebeClient;

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public void process(Exchange exchange) throws Exception {
        Integer responseCode = exchange.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class);
        Long jobKey = exchange.getProperty(ZEEBE_JOB_KEY, Long.class);
        String transactionRole = exchange.getProperty(TRANSACTION_ROLE, String.class);
        if (responseCode > 202) {
            String errorMsg = String.format("Invalid responseCode %s for quote on %s side, transactionId: %s Message: %s",
                    responseCode,
                    transactionRole,
                    exchange.getProperty(TRANSACTION_ID),
                    exchange.getIn().getBody(String.class));

            logger.error(errorMsg);

            final String errorCode;
            final String errorKey;
            if (transactionRole.equals(TransactionRole.PAYER.name())) {
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
                    .send();
        } else {
            Map<String, Object> variables = new HashMap<>();
            QuoteFspResponseDTO quoteResponse = objectMapper.readValue(exchange.getIn().getBody(String.class), QuoteFspResponseDTO.class);
            variables.put(LOCAL_QUOTE_RESPONSE, quoteResponse);
            variables.put("fspFee", quoteResponse.getFspFee());
            variables.put("fspCommission", quoteResponse.getFspCommission());
            variables.put(EXTERNAL_ACCOUNT_ID, exchange.getProperty(EXTERNAL_ACCOUNT_ID));
            variables.put(TENANT_ID, exchange.getProperty(TENANT_ID));
            variables.put(transactionRole.equals(TransactionRole.PAYER.name()) ? LOCAL_QUOTE_FAILED : QUOTE_FAILED, false);

            zeebeClient.newCompleteCommand(jobKey)
                    .variables(variables)
                    .send();
        }
    }
}