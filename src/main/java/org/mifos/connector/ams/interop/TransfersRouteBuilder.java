package org.mifos.connector.ams.interop;

import io.zeebe.client.ZeebeClient;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Processor;
import org.mifos.connector.ams.zeebe.ZeebeProcessStarter;
import org.mifos.phee.common.camel.ErrorHandlerRouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSFER_ACTION;

@Component
@ConditionalOnExpression("${ams.local.enabled}")
public class TransfersRouteBuilder extends ErrorHandlerRouteBuilder {

    @Autowired
    private Processor pojoToString;

    @Autowired
    private AmsService amsService;

    @Autowired
    private ZeebeClient zeebeClient;

    @Autowired
    private ZeebeProcessStarter zeebeProcessStarter;

    @Autowired
    private PrepareTransferRequest prepareTransferRequest;

    @Autowired
    private TransfersResponseProcessor transfersResponseProcessor;

    @Value("${ams.local.version}")
    private String amsLocalVersion;

    public TransfersRouteBuilder() {
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
        from("direct:send-transfers")
                .id("send-transfers")
                .log(LoggingLevel.INFO, "Sending transfer with action: ${exchangeProperty." + TRANSFER_ACTION + "} " +
                        " for transaction: ${exchangeProperty." + TRANSACTION_ID + "}")
                .process(prepareTransferRequest)
                .process(pojoToString)
                .process(amsService::sendTransfer)
                .process(transfersResponseProcessor);
    }

    private void setupFineractCNroute() {
    }
}
