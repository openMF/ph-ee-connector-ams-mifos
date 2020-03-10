package org.mifos.connector.ams.quote;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.json.JSONObject;
import org.mifos.phee.common.channel.dto.TransactionChannelRequestDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static org.mifos.connector.ams.camel.config.CamelProperties.SAVINGS_ACCOUNT;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_REQUEST;


@Component
public class TransactionValidator implements Processor {

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public void process(Exchange exchange) throws Exception {
        JSONObject account = new JSONObject(exchange.getProperty(SAVINGS_ACCOUNT, String.class));
        TransactionChannelRequestDTO channelRequest = objectMapper.readValue(exchange.getProperty(TRANSACTION_REQUEST, String.class), TransactionChannelRequestDTO.class);
        exchange.setProperty(TRANSACTION_REQUEST, channelRequest);
        String requestedCurrency = channelRequest.getAmount().getCurrency();
        String accountCurrency = account.getJSONObject("currency").getString("code");

        if (!accountCurrency.equals(requestedCurrency)) {
            throw new RuntimeException("The requested transaction has different currency "
                    + requestedCurrency + " then the account " + accountCurrency + " currency!");
        }
    }
}
