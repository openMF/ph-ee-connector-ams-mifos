package org.mifos.connector.ams.interop;

import io.zeebe.client.ZeebeClient;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.mifos.phee.common.mojaloop.type.TransactionRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import static org.mifos.connector.ams.camel.config.CamelProperties.ERROR_INFORMATION;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ROLE;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSFER_ACTION;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSFER_CODE;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSFER_RESPONSE_PREFIX;
import static org.mifos.connector.ams.camel.config.CamelProperties.ZEEBE_JOB_KEY;
import static org.mifos.connector.ams.zeebe.ZeebeExpressionVariables.ACTION_FAILURE_MAP;
import static org.mifos.phee.common.ams.dto.TransferActionType.PREPARE;
import static org.mifos.phee.common.camel.ErrorHandlerRouteBuilder.createError;
import static org.mifos.phee.common.mojaloop.type.ErrorCode.PAYEE_FSP_REJECTED_TRANSACTION;
import static org.mifos.phee.common.mojaloop.type.ErrorCode.PAYER_REJECTED_TRANSACTION_REQUEST;

@Component
@ConditionalOnExpression("${ams.local.enabled}")
public class TransfersResponseProcessor implements Processor {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private ZeebeClient zeebeClient;

    @Override
    public void process(Exchange exchange) {
        Integer responseCode = exchange.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class);
        String transferAction = exchange.getProperty(TRANSFER_ACTION, String.class);
        if (responseCode > 202) {
            String transactionRole = exchange.getProperty(TRANSACTION_ROLE, String.class);
            String errorMsg = String.format("Invalid responseCode %s for transfer on %s side, transactionId: %s Message: %s",
                    responseCode,
                    transactionRole,
                    exchange.getProperty(TRANSACTION_ID),
                    exchange.getIn().getBody(String.class));

            logger.error(errorMsg);

            Map<String, Object> variables = new HashMap<>();
            String errorCode = transactionRole.equals(TransactionRole.PAYER.name()) ?
                    String.valueOf(PAYER_REJECTED_TRANSACTION_REQUEST.getCode()) : String.valueOf(PAYEE_FSP_REJECTED_TRANSACTION.getCode());
            variables.put(ERROR_INFORMATION, createError(errorCode, errorMsg).toString());
            variables.put(ACTION_FAILURE_MAP.get(transferAction), true);

            zeebeClient.newCompleteCommand(exchange.getProperty(ZEEBE_JOB_KEY, Long.class))
                    .variables(variables)
                    .send()
                    .join();
        } else {
            Map<String, Object> variables = new HashMap<>();
            variables.put(TRANSFER_RESPONSE_PREFIX + "-" + transferAction, exchange.getIn().getBody());
            if (PREPARE.name().equals(transferAction)) {
                variables.put(TRANSFER_CODE, exchange.getProperty(TRANSFER_CODE));
            }

            zeebeClient.newCompleteCommand(exchange.getProperty(ZEEBE_JOB_KEY, Long.class))
                    .variables(variables)
                    .send()
                    .join();
        }
    }
}
