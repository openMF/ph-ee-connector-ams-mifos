package org.mifos.connector.ams.interop;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.zeebe.client.ZeebeClient;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.mifos.phee.common.mojaloop.dto.ErrorInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import static org.mifos.connector.ams.camel.config.CamelProperties.ERROR_INFORMATION;
import static org.mifos.connector.ams.camel.config.CamelProperties.IS_TRANSFER_PREPARE_SUCCESS;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSFER_ACTION;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSFER_CODE;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSFER_RESPONSE_PREFIX;
import static org.mifos.connector.ams.camel.config.CamelProperties.ZEEBE_JOB_KEY;
import static org.mifos.phee.common.ams.dto.TransferActionType.PREPARE;
import static org.mifos.phee.common.mojaloop.type.ErrorCode.PAYEE_FSP_REJECTED_TRANSACTION;

@Component
@ConditionalOnExpression("${ams.local.enabled}")
public class TransfersResponseProcessor implements Processor {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private ZeebeClient zeebeClient;

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public void process(Exchange exchange) throws Exception {
        Integer responseCode = exchange.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class);
        if (responseCode > 202) {
            String errorMsg = String.format("Invalid responseCode %s for payee-transfer, transaction: %s Message: %s",
                    responseCode,
                    exchange.getProperty(TRANSACTION_ID),
                    exchange.getIn().getBody(String.class));

            logger.error(errorMsg);

            Map<String, Object> variables = new HashMap<>();
            ErrorInformation error = new ErrorInformation((short) PAYEE_FSP_REJECTED_TRANSACTION.getCode(), errorMsg);
            variables.put(ERROR_INFORMATION, objectMapper.writeValueAsString(error));
            variables.put(IS_TRANSFER_PREPARE_SUCCESS, false);

            zeebeClient.newCompleteCommand(exchange.getProperty(ZEEBE_JOB_KEY, Long.class))
                    .variables(variables)
                    .send();
        } else {
            Map<String, Object> variables = new HashMap<>();
            String transferAction = exchange.getProperty(TRANSFER_ACTION, String.class);
            variables.put(TRANSFER_RESPONSE_PREFIX + "-" + transferAction, exchange.getIn().getBody());
            if(PREPARE.name().equals(transferAction)) {
                variables.put(IS_TRANSFER_PREPARE_SUCCESS, true);
                variables.put(TRANSFER_CODE, exchange.getProperty(TRANSFER_CODE));
            }

            zeebeClient.newCompleteCommand(exchange.getProperty(ZEEBE_JOB_KEY, Long.class))
                    .variables(variables)
                    .send();
        }
    }
}
