package org.mifos.connector.ams.quote;

import io.zeebe.client.ZeebeClient;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Processor;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.mifos.connector.ams.zeebe.ZeebeProcessStarter;
import org.mifos.phee.common.ams.dto.PartyFspResponseDTO;
import org.mifos.phee.common.camel.ErrorHandlerRouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static org.mifos.connector.ams.camel.config.CamelProperties.EXTERNAL_ACCOUNT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.PAYER_PARTY_IDENTIFIER;
import static org.mifos.connector.ams.camel.config.CamelProperties.TENANT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ID;


@Component
public class LocalQuoteRouteBuilder extends ErrorHandlerRouteBuilder {

    @Autowired
    private LocalQuoteResponseProcessor localQuoteResponseProcessor;

    @Autowired
    private Processor pojoToString;

    @Autowired
    private AmsService amsService;

    @Autowired
    private ZeebeClient zeebeClient;

    @Autowired
    private ZeebeProcessStarter zeebeProcessStarter;

    @Autowired
    private TransactionValidator transactionValidator;

    @Autowired
    private PrepareLocalQuoteRequest prepareLocalQuoteRequest;

    @Value("${ams.local.version}")
    private String amsLocalVersion;

    public LocalQuoteRouteBuilder() {
        super.configure();
    }

    @Override
    public void configure() {
        from("rest:POST:/localq/{tenantId}/{msisdn}")
                .id("local-quote-http")
                .process(e -> {
                    zeebeProcessStarter.startZeebeWorkflow("Process_134o2qs", e.getIn().getBody(String.class), variables -> {
                        variables.put(PAYER_PARTY_IDENTIFIER, e.getIn().getHeader("msisdn"));
                        variables.put(TENANT_ID, e.getIn().getHeader("tenantId"));
                    });
                });

        if("1.2".equals(amsLocalVersion)) {
            setupFineract12route();
        } else if ("cn".equals(amsLocalVersion)) {
            setupFineractCNroute();
        } else {
            throw new RuntimeException("Unsupported Fineract version: " + amsLocalVersion);
        }
    }

    private void setupFineract12route() {
        from("direct:send-local-quote")
                .id("send-local-quote")
                .log(LoggingLevel.INFO, "Get externalAccount with identifierType: MSISDN with value: ${exchangeProperty."
                        + PAYER_PARTY_IDENTIFIER + "} for transaction: ${exchangeProperty." + TRANSACTION_ID + "}")
                .process(amsService::getExternalAccount)
                .unmarshal().json(JsonLibrary.Jackson, PartyFspResponseDTO.class)
                .process(e -> e.setProperty(EXTERNAL_ACCOUNT_ID, e.getIn().getBody(PartyFspResponseDTO.class).getAccountId()))
                .log(LoggingLevel.INFO, "Get savingsAccount with externalId: ${exchangeProperty."
                        + EXTERNAL_ACCOUNT_ID + "} for transaction: ${exchangeProperty." + TRANSACTION_ID + "}")
                .process(amsService::getSavingsAccount)
                .process(transactionValidator)
                .log(LoggingLevel.INFO, "Sending local quote request for transaction: ${exchangeProperty."
                        + TRANSACTION_ID + "}")
                .process(prepareLocalQuoteRequest)
                .process(pojoToString)
                .process(amsService::getLocalQuote)
                .process(localQuoteResponseProcessor);
    }

    private void setupFineractCNroute() {
    }
}