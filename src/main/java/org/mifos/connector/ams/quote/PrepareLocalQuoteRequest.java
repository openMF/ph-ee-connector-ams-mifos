package org.mifos.connector.ams.quote;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.mifos.phee.common.ams.dto.QuoteFspRequestDTO;
import org.mifos.phee.common.channel.dto.TransactionChannelRequestDTO;
import org.mifos.phee.common.mojaloop.dto.FspMoneyData;
import org.mifos.phee.common.mojaloop.dto.TransactionType;
import org.mifos.phee.common.mojaloop.type.TransactionRole;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.UUID;

import static org.mifos.connector.ams.camel.config.CamelProperties.EXTERNAL_ACCOUNT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_REQUEST;

@Component
public class PrepareLocalQuoteRequest implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        TransactionChannelRequestDTO channelRequest = exchange.getProperty(TRANSACTION_REQUEST, TransactionChannelRequestDTO.class);

        TransactionType transactionType = new TransactionType();
        transactionType.setInitiator(channelRequest.getTransactionType().getInitiator());
        transactionType.setInitiatorType(channelRequest.getTransactionType().getInitiatorType());
        transactionType.setScenario(channelRequest.getTransactionType().getScenario());

        String requestCode = UUID.randomUUID().toString();
        String quoteId = UUID.randomUUID().toString();

        FspMoneyData amount = new FspMoneyData(new BigDecimal(channelRequest.getAmount().getAmount()),
                channelRequest.getAmount().getCurrency());
        QuoteFspRequestDTO request = new QuoteFspRequestDTO(exchange.getProperty(TRANSACTION_ID, String.class),
                requestCode,
                quoteId,
                exchange.getProperty(EXTERNAL_ACCOUNT_ID, String.class),
                amount,
                channelRequest.getAmountType(),
                TransactionRole.PAYER, // for PAYEE the quote amount is ZERO
                transactionType);

        exchange.getIn().setBody(request);
    }
}
