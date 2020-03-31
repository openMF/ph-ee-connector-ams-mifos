package org.mifos.connector.ams.interop;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.mifos.phee.common.ams.dto.QuoteFspResponseDTO;
import org.mifos.phee.common.ams.dto.TransferFspRequestDTO;
import org.mifos.phee.common.channel.dto.TransactionChannelRequestDTO;
import org.mifos.phee.common.mojaloop.dto.FspMoneyData;
import org.mifos.phee.common.mojaloop.dto.TransactionType;
import org.mifos.phee.common.mojaloop.type.TransactionRole;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import java.util.UUID;

import static org.mifos.connector.ams.camel.config.CamelProperties.EXTERNAL_ACCOUNT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.LOCAL_QUOTE_RESPONSE;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_REQUEST;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ROLE;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSFER_CODE;

@Component
@ConditionalOnExpression("${ams.local.enabled}")
public class PrepareTransferRequest implements Processor {

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public void process(Exchange exchange) throws Exception {
        TransactionChannelRequestDTO channelRequest = objectMapper.readValue(exchange.getProperty(TRANSACTION_REQUEST, String.class), TransactionChannelRequestDTO.class);
        QuoteFspResponseDTO localQuoteResponse = objectMapper.readValue(exchange.getProperty(LOCAL_QUOTE_RESPONSE, String.class), QuoteFspResponseDTO.class);

        TransactionType transactionType = new TransactionType();
        transactionType.setInitiator(channelRequest.getTransactionType().getInitiator());
        transactionType.setInitiatorType(channelRequest.getTransactionType().getInitiatorType());
        transactionType.setScenario(channelRequest.getTransactionType().getScenario());

        FspMoneyData amount = new FspMoneyData(channelRequest.getAmount().getAmountDecimal(),
                channelRequest.getAmount().getCurrency());

        String existingTransferCode = exchange.getProperty(TRANSFER_CODE, String.class);
        String transferCode = null;
        if (existingTransferCode != null) {
            transferCode = existingTransferCode;
        } else {
            transferCode = UUID.randomUUID().toString();
            exchange.setProperty(TRANSFER_CODE, transferCode);
        }

        TransferFspRequestDTO transferRequestDTO = new TransferFspRequestDTO(exchange.getProperty(TRANSACTION_ID, String.class),
                transferCode,
                exchange.getProperty(EXTERNAL_ACCOUNT_ID, String.class),
                amount,
                localQuoteResponse.getFspFee(),
                localQuoteResponse.getFspCommission(),
                TransactionRole.valueOf(exchange.getProperty(TRANSACTION_ROLE, String.class)),
                transactionType,
                "");

        exchange.getIn().setBody(transferRequestDTO);
    }
}
