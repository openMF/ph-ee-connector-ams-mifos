package org.mifos.connector.ams.interop;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Processor;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.mifos.phee.common.ams.dto.PartyFspResponseDTO;
import org.mifos.phee.common.camel.ErrorHandlerRouteBuilder;
import org.mifos.phee.common.channel.dto.TransactionChannelRequestDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import static org.mifos.connector.ams.camel.config.CamelProperties.EXTERNAL_ACCOUNT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.PAYER_PARTY_IDENTIFIER;
import static org.mifos.connector.ams.camel.config.CamelProperties.PAYER_PARTY_ID_TYPE;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_REQUEST;


@Component
@ConditionalOnExpression("${ams.local.enabled}")
public class LocalQuoteRouteBuilder extends ErrorHandlerRouteBuilder {

    @Autowired
    private LocalQuoteResponseProcessor localQuoteResponseProcessor;

    @Autowired
    private Processor pojoToString;

    @Autowired
    private ObjectMapper objectMapper;

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
        from("direct:send-local-quote")
                .id("send-local-quote")
                .process(e -> {
                    TransactionChannelRequestDTO channelRequest = objectMapper.readValue(e.getProperty(TRANSACTION_REQUEST, String.class), TransactionChannelRequestDTO.class);
                    e.setProperty(PAYER_PARTY_IDENTIFIER, channelRequest.getPayer().getPartyIdInfo().getPartyIdentifier());
                    e.setProperty(PAYER_PARTY_ID_TYPE, channelRequest.getPayer().getPartyIdInfo().getPartyIdType());
                })
                .log(LoggingLevel.INFO, "Get externalAccount with identifierType: ${exchangeProperty." + PAYER_PARTY_ID_TYPE + "} with value: ${exchangeProperty."
                        + PAYER_PARTY_IDENTIFIER + "} for transaction: ${exchangeProperty." + TRANSACTION_ID + "}")
                .process(amsService::getExternalAccount)
                .unmarshal().json(JsonLibrary.Jackson, PartyFspResponseDTO.class)
                .process(e -> e.setProperty(EXTERNAL_ACCOUNT_ID, e.getIn().getBody(PartyFspResponseDTO.class).getAccountId()))
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