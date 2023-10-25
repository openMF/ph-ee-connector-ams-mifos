package org.mifos.connector.ams.interop;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.mifos.connector.common.ams.dto.QuoteFspRequestDTO;
import org.mifos.connector.common.channel.dto.TransactionChannelRequestDTO;
import org.mifos.connector.common.mojaloop.dto.FspMoneyData;
import org.mifos.connector.common.mojaloop.type.AmountType;
import org.mifos.connector.common.mojaloop.type.TransactionRole;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import java.util.UUID;

import static org.mifos.connector.ams.camel.config.CamelProperties.QUOTE_AMOUNT_TYPE;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ROLE;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.CHANNEL_REQUEST;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.EXTERNAL_ACCOUNT_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TRANSACTION_ID;

@Component
//@ConditionalOnExpression("${ams.local.enabled}")
public class PrepareLocalQuoteRequest implements Processor {

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public void process(Exchange exchange) throws Exception {
        TransactionChannelRequestDTO channelRequest = objectMapper.readValue(exchange.getProperty(CHANNEL_REQUEST, String.class), TransactionChannelRequestDTO.class);

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
                channelRequest.getTransactionType());

        exchange.getIn().setBody(request);
    }
}