package org.mifos.connector.ams.interop;

import org.apache.camel.LoggingLevel;
import org.apache.camel.Processor;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.json.JSONObject;
import org.mifos.connector.common.ams.dto.ClientData;
import org.mifos.connector.common.ams.dto.Customer;
import org.mifos.connector.common.ams.dto.InteropAccountDTO;
import org.mifos.connector.common.ams.dto.LoginFineractCnResponseDTO;
import org.mifos.connector.common.ams.dto.PartyFspResponseDTO;
import org.mifos.connector.common.ams.dto.ProductInstance;
import org.mifos.connector.common.camel.ErrorHandlerRouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.mifos.connector.ams.camel.config.CamelProperties.CLIENT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.CONTINUE_PROCESSING;
import static org.mifos.connector.ams.camel.config.CamelProperties.EXISTING_EXTERNAL_ACCOUNT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSFER_ACTION;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.ACCOUNT;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.ACCOUNT_CURRENCY;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.ACCOUNT_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.EXTERNAL_ACCOUNT_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.PARTY_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.PARTY_ID_TYPE;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TENANT_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TRANSACTION_ID;


@Component
@ConditionalOnExpression("${ams.local.enabled}")
public class InteroperationRouteBuilder extends ErrorHandlerRouteBuilder {

    @Value("${ams.local.version}")
    private String amsVersion;

    @Autowired
    private Processor pojoToString;

    @Autowired
    private AmsService amsService;

    @Autowired
    private PrepareLocalQuoteRequest prepareLocalQuoteRequest;

    @Autowired
    private QuoteResponseProcessor quoteResponseProcessor;

    @Autowired
    private PrepareTransferRequest prepareTransferRequest;

    @Autowired
    private TransfersResponseProcessor transfersResponseProcessor;

    @Autowired
    private ClientResponseProcessor clientResponseProcessor;

    @Autowired
    private InteropResponseProcessor interopResponseProcessor;

    public InteroperationRouteBuilder() {
        super.configure();
    }

    @Override
    public void configure() {
        from("direct:get-external-account")
                .id("get-external-account")
                .log(LoggingLevel.INFO, "Get externalAccount with identifierType: ${exchangeProperty." + PARTY_ID_TYPE + "} with value: ${exchangeProperty."
                        + PARTY_ID + "}")
                .process(amsService::getExternalAccount)
                .unmarshal().json(JsonLibrary.Jackson, PartyFspResponseDTO.class)
                .process(e -> e.setProperty(EXTERNAL_ACCOUNT_ID, e.getIn().getBody(PartyFspResponseDTO.class).getAccountId()));

        from("direct:send-local-quote")
                .id("send-local-quote")
                .to("direct:get-external-account")
                .log(LoggingLevel.INFO, "Sending local quote request for transaction: ${exchangeProperty."
                        + TRANSACTION_ID + "}")
                .process(prepareLocalQuoteRequest)
                .process(pojoToString)
                .process(amsService::getLocalQuote)
                .process(quoteResponseProcessor);

        from("direct:send-transfers")
                .id("send-transfers")
                .log(LoggingLevel.INFO, "Sending transfer with action: ${exchangeProperty." + TRANSFER_ACTION + "} " +
                        " for transaction: ${exchangeProperty." + TRANSACTION_ID + "}")
                .process(prepareTransferRequest)
                .process(pojoToString)
                .process(amsService::sendTransfer)
                .process(transfersResponseProcessor);

        from("direct:fincn-oauth")
                .id("fincn-oauth")
                .log(LoggingLevel.INFO, "Fineract CN oauth request for tenant: ${exchangeProperty." + TENANT_ID + "}")
                .process(amsService::login)
                .unmarshal().json(JsonLibrary.Jackson, LoginFineractCnResponseDTO.class);

        // @formatter:off
        from("direct:get-party")
                .id("get-party")
                .log(LoggingLevel.INFO, "Get party information for identifierType: ${exchangeProperty." + PARTY_ID_TYPE + "} with value: ${exchangeProperty." + PARTY_ID + "}")
                .to("direct:get-external-account")
                .process(amsService::getSavingsAccount)
                .choice()
                    .when(e -> "1.2".equals(amsVersion))
                        .unmarshal().json(JsonLibrary.Jackson, InteropAccountDTO.class)
                        .process(e -> e.setProperty(CLIENT_ID, e.getIn().getBody(InteropAccountDTO.class).getClientId()))
                        .process(amsService::getClient)
                        .unmarshal().json(JsonLibrary.Jackson, ClientData.class)
                    .endChoice()
                    .otherwise() // cn
                        .unmarshal().json(JsonLibrary.Jackson, ProductInstance.class)
                        .process(e -> e.setProperty(CLIENT_ID, e.getIn().getBody(ProductInstance.class).getCustomerIdentifier()))
                        .process(amsService::getClient)
                        .unmarshal().json(JsonLibrary.Jackson, Customer.class)
                    .endChoice()
                .end()
                .process(clientResponseProcessor);
        // @formatter:on

        // @formatter:off
        from("direct:register-party")
                .id("register-party")
                .log(LoggingLevel.INFO, "Register party with type: ${exchangeProperty." + PARTY_ID_TYPE + "} identifier: ${exchangeProperty." + PARTY_ID + "} account ${exchangeProperty." + ACCOUNT + "}")
                .process(amsService::getSavingsAccounts)
                .process(e -> {
                    JSONObject account = stream(spliteratorUnknownSize(
                            new JSONObject(e.getIn().getBody(String.class)).getJSONArray("pageItems").iterator(),
                            ORDERED), false)
                            .filter(sa -> e.getProperty(ACCOUNT, String.class).equals(((JSONObject)sa).getString("accountNo")))
                            .findFirst()
                            .map(sa -> ((JSONObject)sa))
                            .orElseThrow(() -> new RuntimeException("Could not find account with number: " + e.getProperty(ACCOUNT)));
                    e.setProperty(ACCOUNT_ID, account.getString("accountNo"));
                    e.setProperty(ACCOUNT_CURRENCY, account.getJSONObject("currency").getString("code"));
                    e.setProperty(EXISTING_EXTERNAL_ACCOUNT_ID, account.getString("externalId"));
                })
                .to("direct:get-external-account")
                .choice()
                    .when(e -> e.getProperty(EXTERNAL_ACCOUNT_ID) == null) // identifier not registered to any account
                        .to("direct:add-interop-identifier-to-account")
                    .endChoice()
                    .when(e -> { // identifier registered to other account
                        String interopQueriedId = e.getProperty(EXTERNAL_ACCOUNT_ID, String.class);
                        return interopQueriedId != null && !interopQueriedId.equals(e.getProperty(EXISTING_EXTERNAL_ACCOUNT_ID, String.class));
                    })
                        .setProperty(CONTINUE_PROCESSING, constant(true))
                        .to("direct:remove-interop-identifier-from-account")
                        .setProperty(CONTINUE_PROCESSING, constant(false))
                        .to("direct:add-interop-identifier-to-account")
                    .endChoice()
                .end();
        // @formatter:on

        from("direct:add-interop-identifier-to-account")
                .id("add-interop-identifier-to-account")
                .process(e -> {
                    JSONObject request = new JSONObject();
                    request.put("accountId", e.getProperty(EXISTING_EXTERNAL_ACCOUNT_ID));
                    e.getIn().setBody(request.toString());
                })
                .process(amsService::registerInteropIdentifier)
                .process(interopResponseProcessor);

        from("direct:remove-interop-identifier-from-account")
                .id("remove-interop-identifier-from-account")
                .process(amsService::removeInteropIdentifier)
                .process(interopResponseProcessor);
    }
}