package org.mifos.connector.ams;

import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.json.JSONObject;
import org.mifos.connector.ams.interop.AmsService;
import org.mifos.connector.common.ams.dto.InteropAccountDTO;
import org.mifos.connector.common.ams.dto.PartyFspResponseDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static org.mifos.connector.ams.camel.config.CamelProperties.CLIENT_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.*;

@Component
public class HealthCheck extends RouteBuilder {
    @Autowired
    private AmsService amsService;
    @Value("${ams.local.version}")
    private String amsVersion;
    @Override
    public void configure() {
        from("rest:GET:/")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(200))
                .setBody(constant(""));

        from("rest:GET:/ams/accounts/{IdentifierType}/{IdentifierId}/status")
                .id("ams-connector-account-management-status-check")
                .log(LoggingLevel.INFO, "##ams-connector-account-management-status-check")
                .process(e -> {
                    String IdentifierType = e.getIn().getHeader("IdentifierType", String.class);
                    String IdentifierId = e.getIn().getHeader("IdentifierId", String.class);
                    String tenantId = e.getIn().getHeader("Platform-TenantId", String.class);
                    e.setProperty(PARTY_ID_TYPE, IdentifierType);
                    e.setProperty(PARTY_ID,IdentifierId);
                    e.setProperty(TENANT_ID, tenantId);
                })
                .log(LoggingLevel.INFO, "##ams-connector-account-management-status-check: ${exchangeProperty." + PARTY_ID_TYPE + "} with value: ${exchangeProperty." + PARTY_ID + "}")
                .to("direct:get-external-account")
                .process(amsService::getSavingsAccount)
                .unmarshal().json(JsonLibrary.Jackson, InteropAccountDTO.class)
                .process(e -> {
                    InteropAccountDTO account = e.getIn().getBody(InteropAccountDTO.class);
                    JSONObject response = new JSONObject();
                    response.put("status", account.getStatus().getCode());
                    e.getIn().setBody(response.toString());
                });

        from("rest:GET:/ams/accounts/{IdentifierType}/{IdentifierId}/accountname")
                .id("account-management-get-name")
                .log(LoggingLevel.INFO, "## account-management-get-name")
                .process(e -> {
                    String IdentifierType = e.getIn().getHeader("IdentifierType", String.class);
                    String IdentifierId = e.getIn().getHeader("IdentifierId", String.class);
                    String tenantId = e.getIn().getHeader("Platform-TenantId", String.class);
                })
                .setBody(constant(null));
        from("rest:GET:/channel/accounts/{IdentifierType}/{IdentifierId}/balance")
                .id("account-management-balance-check")
                .log(LoggingLevel.INFO, "## account-management-balance-check")
                .process(e -> {
                    String IdentifierType = e.getIn().getHeader("IdentifierType", String.class);
                    String IdentifierId = e.getIn().getHeader("IdentifierId", String.class);
                    String tenantId = e.getIn().getHeader("Platform-TenantId", String.class);
                })
                .setBody(constant(null));
        from("rest:GET:/ams/accounts/{IdentifierType}/{IdentifierId}/transactions")
                .id("account-management-get-transactions")
                .log(LoggingLevel.INFO, "## account-management-get-transactions")
                .process(e -> {
                    String IdentifierType = e.getIn().getHeader("IdentifierType", String.class);
                    String IdentifierId = e.getIn().getHeader("IdentifierId", String.class);
                    String tenantId = e.getIn().getHeader("Platform-TenantId", String.class);
                })
                .setBody(constant(null));
        from("rest:GET:/ams/accounts/{IdentifierType}/{IdentifierId}/statemententries")
                .id("account-management-get-statemententries")
                .log(LoggingLevel.INFO, "## account-management-get-statemententries")
                .process(e -> {
                    String IdentifierType = e.getIn().getHeader("IdentifierType", String.class);
                    String IdentifierId = e.getIn().getHeader("IdentifierId", String.class);
                    String tenantId = e.getIn().getHeader("Platform-TenantId", String.class);
                })
                .setBody(constant(null));
    }
}
