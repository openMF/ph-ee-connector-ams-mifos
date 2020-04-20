package org.mifos.connector.ams.interop;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.mifos.phee.common.ams.dto.QuoteFspRequestDTO;
import org.mifos.phee.common.channel.dto.TransactionChannelRequestDTO;
import org.mifos.phee.common.mojaloop.dto.FspMoneyData;
import org.mifos.phee.common.mojaloop.dto.TransactionType;
import org.mifos.phee.common.mojaloop.type.AmountType;
import org.mifos.phee.common.mojaloop.type.TransactionRole;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import java.util.UUID;

import static org.mifos.connector.ams.camel.config.CamelProperties.EXTERNAL_ACCOUNT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.QUOTE_AMOUNT_TYPE;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_REQUEST;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ROLE;

@Component
@ConditionalOnExpression("${ams.local.enabled}")
public class PrepareLocalQuoteRequest implements Processor {

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public void process(Exchange exchange) throws Exception {
        TransactionChannelRequestDTO channelRequest = objectMapper.readValue(exchange.getProperty(TRANSACTION_REQUEST, String.class), TransactionChannelRequestDTO.class);

        TransactionType transactionType = new TransactionType();
        transactionType.setInitiator(channelRequest.getTransactionType().getInitiator());
        transactionType.setInitiatorType(channelRequest.getTransactionType().getInitiatorType());
        transactionType.setScenario(channelRequest.getTransactionType().getScenario());

        String requestCode = UUID.randomUUID().toString();
        String quoteId = UUID.randomUUID().toString();

        FspMoneyData amount = new FspMoneyData(channelRequest.getAmount().getAmountDecimal(),
                channelRequest.getAmount().getCurrency());
        QuoteFspRequestDTO request = new QuoteFspRequestDTO(exchange.getProperty(TRANSACTION_ID, String.class),
                requestCode,
                quoteId,
                exchange.getProperty(EXTERNAL_ACCOUNT_ID, String.class),
                amount,
                AmountType.valueOf(exchange.getProperty(QUOTE_AMOUNT_TYPE, String.class)),
                TransactionRole.valueOf(exchange.getProperty(TRANSACTION_ROLE, String.class)),
                transactionType);

        exchange.getIn().setBody(request);
    }
}