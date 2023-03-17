package org.mifos.connector.ams.implementaion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.mifos.connector.ams.defination.AccountTransactions;
import org.mifos.connector.ams.model.BalanceResponseDTO;
import org.mifos.connector.ams.model.TransactionsResponseDTO;
import org.mifos.connector.ams.utils.Headers;
import org.mifos.connector.ams.utils.SpringWrapperUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AccountTransactionsController implements AccountTransactions {
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    private ProducerTemplate producerTemplate;

    @Override
    public TransactionsResponseDTO accountTransaction(String IdentifierType, String IdentifierId, String tenantId) throws JsonProcessingException {
        Headers headers = new Headers.HeaderBuilder()
                .addHeader("Platform-TenantId", tenantId)
                .addHeader("IdentifierId", IdentifierId)
                .addHeader("IdentifierType", IdentifierType)
                .build();
        Exchange exchange = SpringWrapperUtil.getDefaultWrappedExchange(producerTemplate.getCamelContext(),
                headers,null);
        producerTemplate.send("direct:get-account-transactions", exchange);

        String body = exchange.getIn().getBody(String.class);
        return objectMapper.readValue(body, TransactionsResponseDTO.class);
    }
}
