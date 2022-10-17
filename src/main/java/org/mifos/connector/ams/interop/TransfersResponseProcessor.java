package org.mifos.connector.ams.interop;

import io.camunda.zeebe.client.ZeebeClient;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.json.JSONObject;
import org.mifos.connector.common.mojaloop.type.TransactionRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ROLE;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSFER_ACTION;
import static org.mifos.connector.ams.camel.config.CamelProperties.ZEEBE_JOB_KEY;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.ACTION_FAILURE_MAP;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.ERROR_INFORMATION;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TRANSACTION_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TRANSFER_CODE;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TRANSFER_CREATE_FAILED;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TRANSFER_PREPARE_FAILED;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TRANSFER_RESPONSE_PREFIX;
import static org.mifos.connector.common.ams.dto.TransferActionType.PREPARE;
import static org.mifos.connector.common.camel.ErrorHandlerRouteBuilder.createError;
import static org.mifos.connector.common.mojaloop.type.ErrorCode.PAYEE_FSP_REJECTED_TRANSACTION;
import static org.mifos.connector.common.mojaloop.type.ErrorCode.PAYER_REJECTED_TRANSACTION_REQUEST;

@Component
@ConditionalOnExpression("${ams.local.enabled}")
public class TransfersResponseProcessor implements Processor {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired(required = false)
    private ZeebeClient zeebeClient;

    @Override
    public void process(Exchange exchange) {
        Integer responseCode = exchange.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class);
        String transferAction = exchange.getProperty(TRANSFER_ACTION, String.class);

        Map<String, Object> variables = new HashMap<>();
        variables.put(TRANSFER_CREATE_FAILED, responseCode > 202);

        if (responseCode > 202) {

            String transactionRole = exchange.getProperty(TRANSACTION_ROLE, String.class);
            String errorMsg = String.format("Invalid responseCode %s for transfer on %s side, transactionId: %s Message: %s",
                    responseCode,
                    transactionRole,
                    exchange.getProperty(TRANSACTION_ID),
                    exchange.getIn().getBody(String.class));

            logger.error(errorMsg);

            JSONObject errorJson = new JSONObject(exchange.getIn().getBody(String.class));

            String errorCode = transactionRole.equals(TransactionRole.PAYER.name()) ?
                    String.valueOf(PAYER_REJECTED_TRANSACTION_REQUEST.getCode()) : String.valueOf(PAYEE_FSP_REJECTED_TRANSACTION.getCode());
            variables.put(ERROR_INFORMATION, errorJson.toString());
            variables.put(ACTION_FAILURE_MAP.get(transferAction), true);

        } else {
            variables.put(TRANSFER_RESPONSE_PREFIX + "-" + transferAction, exchange.getIn().getBody());
            if (PREPARE.name().equals(transferAction)) {
                variables.put(TRANSFER_CODE, exchange.getProperty(TRANSFER_CODE));
            }
            variables.put(ACTION_FAILURE_MAP.get(transferAction), false);
        }

        zeebeClient.newCompleteCommand(exchange.getProperty(ZEEBE_JOB_KEY, Long.class))
                .variables(variables)
                .send();
        logger.info("Completed job with key: {}", exchange.getProperty(ZEEBE_JOB_KEY, Long.class));
    }
}
