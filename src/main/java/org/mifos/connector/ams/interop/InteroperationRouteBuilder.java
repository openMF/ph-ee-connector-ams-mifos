package org.mifos.connector.ams.interop;

import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Processor;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.json.JSONObject;
import org.mifos.connector.ams.tenant.TenantNotExistException;
import org.mifos.connector.common.ams.dto.ClientData;
import org.mifos.connector.common.ams.dto.Customer;
import org.mifos.connector.common.ams.dto.InteropAccountDTO;
import org.mifos.connector.common.ams.dto.LoginFineractCnResponseDTO;
import org.mifos.connector.common.ams.dto.PartyFspResponseDTO;
import org.mifos.connector.common.ams.dto.ProductDefinition;
import org.mifos.connector.common.ams.dto.ProductInstance;
import org.mifos.connector.common.camel.ErrorHandlerRouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import java.util.Optional;

import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.mifos.connector.ams.camel.config.CamelProperties.CLIENT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.CONTINUE_PROCESSING;
import static org.mifos.connector.ams.camel.config.CamelProperties.DEFINITON_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.EXISTING_EXTERNAL_ACCOUNT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.INTEROP_ACCOUNT_TO_REGISTER;
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
    private InteropPartyResponseProcessor interopPartyResponseProcessor;

    public InteroperationRouteBuilder() {
        super.configure();
    }

    @Override
    public void configure() {
        onException(TenantNotExistException.class)
                .process(e -> {
                    e.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, 404);
                    Exception exception = e.getException();
                    if (exception != null) {
                        e.getIn().setBody(exception.getMessage());
                    }
                })
                .process(clientResponseProcessor)
                .stop();

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
                .process(e -> e.setProperty(ACCOUNT_ID, e.getProperty(EXTERNAL_ACCOUNT_ID)))
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
                .choice()
                    .when(e -> "1.2".equals(amsVersion))
                        .to("direct:register-party-finx")
                    .endChoice()
                    .otherwise()
                        .to("direct:register-party-fincn")
                    .endChoice()
                .end();

        from("direct:register-party-finx")
                .process(amsService::getSavingsAccounts)
                .setProperty(CONTINUE_PROCESSING, constant(true))
                .process(interopPartyResponseProcessor)
                .process(e -> {
                    Optional<Object> account = stream(spliteratorUnknownSize( // TODO this solution is potentially bad if there are too many accounts in the system
                            new JSONObject(e.getIn().getBody(String.class)).getJSONArray("pageItems").iterator(),
                            ORDERED), false)
                            .filter(sa -> e.getProperty(ACCOUNT, String.class).equals(((JSONObject)sa).getString("accountNo")))
                            .findFirst();
                    if(!account.isPresent()) {
                        e.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, 404);
                    } else {
                        JSONObject jsonAccount = (JSONObject)account.get();
                        e.setProperty(ACCOUNT_ID, jsonAccount.getString("accountNo"));
                        e.setProperty(ACCOUNT_CURRENCY, jsonAccount.getJSONObject("currency").getString("code"));
                        e.setProperty(EXISTING_EXTERNAL_ACCOUNT_ID, jsonAccount.getString("externalId"));
                        e.setProperty(INTEROP_ACCOUNT_TO_REGISTER, jsonAccount.getString("externalId"));
                    }
                })
                .process(interopPartyResponseProcessor)
                .to("direct:get-external-account")
                .choice()
                    .when(e -> e.getProperty(EXTERNAL_ACCOUNT_ID) == null) // identifier not registered to any account
                        .setProperty(CONTINUE_PROCESSING, constant(false))
                        .to("direct:add-interop-identifier-to-account")
                    .endChoice()
                    .when(e -> !e.getProperty(EXTERNAL_ACCOUNT_ID, String.class).equals(e.getProperty(EXISTING_EXTERNAL_ACCOUNT_ID, String.class))) // identifier registered to other account
                        .to("direct:remove-interop-identifier-from-account")
                        .setProperty(CONTINUE_PROCESSING, constant(false))
                        .to("direct:add-interop-identifier-to-account")
                    .endChoice()
                    .otherwise()
                        .setProperty(CONTINUE_PROCESSING, constant(false))
                        .process(interopPartyResponseProcessor) // identifier already registered to the selected account
                    .endChoice()
                .end();

        from("direct:register-party-fincn")
                .process(e -> e.setProperty(ACCOUNT_ID, e.getProperty(ACCOUNT)))
                .process(amsService::getSavingsAccount)
                .setProperty(CONTINUE_PROCESSING, constant(true))
                .process(interopPartyResponseProcessor)
                .unmarshal().json(JsonLibrary.Jackson, ProductInstance.class)
                .process(e -> e.setProperty(DEFINITON_ID, e.getIn().getBody(ProductInstance.class).getProductIdentifier()))
                .process(amsService::getSavingsAccountDefiniton)
                .process(interopPartyResponseProcessor)
                .unmarshal().json(JsonLibrary.Jackson, ProductDefinition.class)
                .process(e -> e.setProperty(ACCOUNT_CURRENCY, e.getIn().getBody(ProductDefinition.class).getCurrency().getCode()))
                .setProperty(INTEROP_ACCOUNT_TO_REGISTER, simple("${exchangeProperty."+ ACCOUNT_ID+"}"))
                .to("direct:get-external-account")
                .choice()
                    .when(e -> e.getProperty(EXTERNAL_ACCOUNT_ID) == null) // identifier not registered to any account
                        .setProperty(CONTINUE_PROCESSING, constant(false))
                        .to("direct:add-interop-identifier-to-account")
                    .endChoice()
                    .when(e -> !e.getProperty(EXTERNAL_ACCOUNT_ID, String.class).equals(e.getProperty(ACCOUNT_ID, String.class))) // identifier registered to other account
                        .to("direct:remove-interop-identifier-from-account")
                        .setProperty(CONTINUE_PROCESSING, constant(false))
                        .to("direct:add-interop-identifier-to-account")
                    .endChoice()
                    .otherwise() // identifier already registered to the account
                        .setProperty(CONTINUE_PROCESSING, constant(false))
                        .process(interopPartyResponseProcessor)
                    .endChoice()
                .end();
        // @formatter:on

        from("direct:add-interop-identifier-to-account")
                .id("add-interop-identifier-to-account")
                .process(e -> {
                    JSONObject request = new JSONObject();
                    request.put("accountId", e.getProperty(INTEROP_ACCOUNT_TO_REGISTER));
                    e.getIn().setBody(request.toString());
                })
                .process(amsService::registerInteropIdentifier)
                .process(interopPartyResponseProcessor);

        from("direct:remove-interop-identifier-from-account")
                .id("remove-interop-identifier-from-account")
                .process(amsService::removeInteropIdentifier)
                .process(interopPartyResponseProcessor);
    }
}