package org.mifos.connector.ams.implementaion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.mifos.connector.ams.defination.AccountBalance;
import org.mifos.connector.ams.model.BalanceResponseDTO;
import org.mifos.connector.ams.utils.Headers;
import org.mifos.connector.ams.utils.SpringWrapperUtil;
import org.mifos.connector.common.ams.dto.InteropAccountDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
public class AccountBalanceController implements AccountBalance {

    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    private ProducerTemplate producerTemplate;

    @Override
    public BalanceResponseDTO accountBalance(String IdentifierType, String IdentifierId, String tenantId) throws JsonProcessingException {
        Headers headers = new Headers.HeaderBuilder()
                .addHeader("Platform-TenantId", tenantId)
                .addHeader("IdentifierId", IdentifierId)
                .addHeader("IdentifierType", IdentifierType)
                .build();
        Exchange exchange = SpringWrapperUtil.getDefaultWrappedExchange(producerTemplate.getCamelContext(),
                headers,null);
        producerTemplate.send("direct:get-account-balance", exchange);

        String body = exchange.getIn().getBody(String.class);
        return objectMapper.readValue(body, BalanceResponseDTO.class);
    }
}
