package org.mifos.connector.ams.interop;

import static org.mifos.connector.ams.camel.config.CamelProperties.CLIENT_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.PARTY_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.PARTY_ID_TYPE;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TENANT_ID;

import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.json.JSONObject;
import org.mifos.connector.common.ams.dto.ClientData;
import org.mifos.connector.common.ams.dto.Customer;
import org.mifos.connector.common.ams.dto.InteropAccountDTO;
import org.mifos.connector.common.ams.dto.ProductInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class AccountsRouteBuilder extends RouteBuilder {

    @Autowired(required = false)
    private AmsService amsService;
    @Value("${ams.local.version}")
    private String amsVersion;

    @Override
    public void configure() {
        from("rest:GET:/ams/accounts/{IdentifierType}/{IdentifierId}/status").id("ams-connector-account-management-status-check")
                .log(LoggingLevel.INFO, "##ams-connector-account-management-status-check").process(e -> {
                    String IdentifierType = e.getIn().getHeader("IdentifierType", String.class);
                    String IdentifierId = e.getIn().getHeader("IdentifierId", String.class);
                    String tenantId = e.getIn().getHeader("Platform-TenantId", String.class);
                    e.setProperty(PARTY_ID_TYPE, IdentifierType);
                    e.setProperty(PARTY_ID, IdentifierId);
                    e.setProperty(TENANT_ID, tenantId);
                })
                .log(LoggingLevel.INFO,
                        "##ams-connector-account-management-status-check: ${exchangeProperty." + PARTY_ID_TYPE
                                + "} with value: ${exchangeProperty." + PARTY_ID + "}")
                .to("direct:get-external-account").process(amsService::getSavingsAccount).unmarshal()
                .json(JsonLibrary.Jackson, InteropAccountDTO.class).process(e -> {
                    InteropAccountDTO account = e.getIn().getBody(InteropAccountDTO.class);
                    JSONObject response = new JSONObject();
                    response.put("accountStatus", account.getStatus().getCode());
                    response.put("subStatus", account.getSubStatus().getCode());
                    response.put("lei", "");
                    e.getIn().setBody(response.toString());
                });
        // @formatter:off
        from("rest:GET:/ams/accounts/{IdentifierType}/{IdentifierId}/accountname")
                .id("ams-connector-account-management-get-name")
                .log(LoggingLevel.INFO, "##ams-connector-account-management-get-name")
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
                .choice()
                    .when(e -> "1.2".equals(amsVersion))
                        .unmarshal().json(JsonLibrary.Jackson, InteropAccountDTO.class)
                        .process(e -> e.setProperty(CLIENT_ID, e.getIn().getBody(InteropAccountDTO.class).getClientId()))
                        .process(amsService::getClientImage)
                        .process(e -> e.setProperty("client_image", e.getIn().getBody(String.class)))
                        .process(amsService::getClient)
                        .unmarshal().json(JsonLibrary.Jackson, ClientData.class)
                        .process(e -> {
                            ClientData customer = e.getIn().getBody(ClientData.class);
                            JSONObject response = new JSONObject();
                            JSONObject name = new JSONObject();
                            name.put("title", "");
                            name.put("firstName", customer.getFirstname());
                            name.put("middleName", customer.getMiddlename());
                            name.put("lastName", customer.getLastname());
                            name.put("fullName", customer.getFullname());
                            name.put("nativeName", customer.getDisplayName());
                            response.put("name", name);
                            response.put("lei", "");
                            response.put("image", e.getProperty("client_image"));
                            e.getIn().setBody(response.toString());
                        })
                    .endChoice()
                    .otherwise() // cn
                        .unmarshal().json(JsonLibrary.Jackson, ProductInstance.class)
                            .process(e -> e.setProperty(CLIENT_ID, e.getIn().getBody(ProductInstance.class).getCustomerIdentifier()))
                        .process(amsService::getClient)
                        .unmarshal().json(JsonLibrary.Jackson, Customer.class)
                        .process(e -> {
                            Customer customer = e.getIn().getBody(Customer.class);
                            JSONObject response = new JSONObject();
                            JSONObject name = new JSONObject();
                            name.put("title","");
                            name.put("firstName", "");
                            name.put("middleName", "");
                            name.put("lastName", customer.getSurname());
                            name.put("fullName", customer.getGivenName());
                            name.put("nativeName", customer.getGivenName());
                            response.put("name", name);
                            response.put("lei", "");
                            e.getIn().setBody(response.toString());
                        })
                    .endChoice()
                .end();
        from("rest:GET:/ams/accounts/{IdentifierType}/{IdentifierId}/balance")
                .id("ams-connector-account-management-balance-check")
                .log(LoggingLevel.INFO, "## ams-connector-account-management-balance-check")
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
                    response.put("currentBalance", account.getAccountBalance().toString());
                    response.put("availableBalance", account.getAvailableBalance().toString());
                    response.put("reservedBalance",  "");
                    response.put("unclearedBalance", "");
                    response.put("currency", account.getCurrency());
                    response.put("accountStatus", account.getStatus().getCode());
                    e.getIn().setBody(response.toString());
                });
        from("rest:GET:/ams/accounts/{IdentifierType}/{IdentifierId}/transactions")
                .id("ams-connector-account-management-get-transactions")
                .log(LoggingLevel.INFO, "## ams-connector-account-management-get-transactions")
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
                .process(amsService::getSavingsAccountsTransactions)
                .process(e -> {
                    e.getIn().setBody(e.getIn().getBody());
                });
                /*.
                .unmarshal().json(JsonLibrary.Jackson, List.class)
                .process(e -> {
                    List<InteropTransactionData> transactions = e.getIn().getBody(List.class);
                    JSONArray response =
                    e.getIn().setBody(response.toString());
                });*/
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
