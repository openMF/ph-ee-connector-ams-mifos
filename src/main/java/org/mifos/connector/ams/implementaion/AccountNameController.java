package org.mifos.connector.ams.implementaion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.mifos.connector.ams.defination.AccountName;
import org.mifos.connector.ams.model.AccountNameResponseDTO;
import org.mifos.connector.ams.model.BalanceResponseDTO;
import org.mifos.connector.ams.utils.Headers;
import org.mifos.connector.ams.utils.SpringWrapperUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AccountNameController implements AccountName {
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    private ProducerTemplate producerTemplate;

    @Override
    public AccountNameResponseDTO accountName(String IdentifierType, String IdentifierId, String tenantId) throws JsonProcessingException {
        Headers headers = new Headers.HeaderBuilder()
                .addHeader("Platform-TenantId", tenantId)
                .addHeader("IdentifierId", IdentifierId)
                .addHeader("IdentifierType", IdentifierType)
                .build();
        Exchange exchange = SpringWrapperUtil.getDefaultWrappedExchange(producerTemplate.getCamelContext(),
                headers, null);
        producerTemplate.send("direct:get-account-name", exchange);

        String body = exchange.getIn().getBody(String.class);
        return objectMapper.readValue(body, AccountNameResponseDTO.class);

    }
}
