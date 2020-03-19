package org.mifos.connector.ams.interop;

import org.apache.camel.LoggingLevel;
import org.apache.camel.Processor;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.mifos.phee.common.ams.dto.PartyFspResponseDTO;
import org.mifos.phee.common.camel.ErrorHandlerRouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import static org.mifos.connector.ams.camel.config.CamelProperties.EXTERNAL_ACCOUNT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.PARTY_IDENTIFIER_FOR_EXT_ACC;
import static org.mifos.connector.ams.camel.config.CamelProperties.PARTY_ID_TYPE_FOR_EXT_ACC;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ID;


@Component
@ConditionalOnExpression("${ams.local.enabled}")
public class LocalQuoteRouteBuilder extends ErrorHandlerRouteBuilder {

    @Autowired
    private LocalQuoteResponseProcessor localQuoteResponseProcessor;

    @Autowired
    private Processor pojoToString;

    @Autowired
    private AmsService amsService;

    @Autowired
    private PrepareLocalQuoteRequest prepareLocalQuoteRequest;

    @Value("${ams.local.version}")
    private String amsLocalVersion;

    public LocalQuoteRouteBuilder() {
        super.configure();
    }

    @Override
    public void configure() {
        if ("1.2".equals(amsLocalVersion)) {
            setupFineract12route();
        } else if ("cn".equals(amsLocalVersion)) {
            setupFineractCNroute();
        } else {
            throw new RuntimeException("Unsupported Fineract version: " + amsLocalVersion);
        }
    }

    private void setupFineract12route() {
        from("direct:get-external-account")
                .id("get-external-account")
                .log(LoggingLevel.INFO, "Get externalAccount with identifierType: ${exchangeProperty." + PARTY_ID_TYPE_FOR_EXT_ACC + "} with value: ${exchangeProperty."
                        + PARTY_IDENTIFIER_FOR_EXT_ACC + "} for transaction: ${exchangeProperty." + TRANSACTION_ID + "}")
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
                .process(localQuoteResponseProcessor);
    }

    private void setupFineractCNroute() {
    }
}