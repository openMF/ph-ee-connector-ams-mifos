package org.mifos.connector.ams.interop;

import org.apache.camel.LoggingLevel;
import org.apache.camel.Processor;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.mifos.phee.common.ams.dto.LoginFineractCnResponseDTO;
import org.mifos.phee.common.ams.dto.PartyFspResponseDTO;
import org.mifos.phee.common.camel.ErrorHandlerRouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import static org.mifos.connector.ams.camel.config.CamelProperties.EXTERNAL_ACCOUNT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.PARTY_IDENTIFIER_FOR_EXT_ACC;
import static org.mifos.connector.ams.camel.config.CamelProperties.PARTY_ID_TYPE_FOR_EXT_ACC;
import static org.mifos.connector.ams.camel.config.CamelProperties.TENANT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSFER_ACTION;


@Component
@ConditionalOnExpression("${ams.local.enabled}")
public class InteroperationRouteBuilder extends ErrorHandlerRouteBuilder {

    @Autowired
    private Processor pojoToString;

    @Autowired
    private AmsService amsService;

    @Autowired
    private PrepareLocalQuoteRequest prepareLocalQuoteRequest;

    @Autowired
    private LocalQuoteResponseProcessor localQuoteResponseProcessor;

    @Autowired
    private PrepareTransferRequest prepareTransferRequest;

    @Autowired
    private TransfersResponseProcessor transfersResponseProcessor;

    public InteroperationRouteBuilder() {
        super.configure();
    }

    @Override
    public void configure() {
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
                .log(LoggingLevel.INFO, "Fineract CN oauth request for tenant: ${exchangeProperty. " + TENANT_ID + "}")
                .process(amsService::login)
                .unmarshal().json(JsonLibrary.Jackson, LoginFineractCnResponseDTO.class);
    }
}