package org.mifos.connector.ams.interop;

import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.mifos.connector.ams.camel.config.CamelProperties.CLIENT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.CONTINUE_PROCESSING;
import static org.mifos.connector.ams.camel.config.CamelProperties.DEFINITON_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.EXISTING_EXTERNAL_ACCOUNT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.INTEROP_ACCOUNT_TO_REGISTER;
import static org.mifos.connector.ams.camel.config.CamelProperties.IS_ERROR_SET_MANUALLY;
import static org.mifos.connector.ams.camel.config.CamelProperties.PROCESS_TYPE;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ROLE;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSFER_ACTION;
import static org.mifos.connector.ams.camel.config.CamelProperties.X_CALLBACKURL;
import static org.mifos.connector.ams.camel.config.CamelProperties.ZEEBE_JOB_KEY;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.ACCOUNT;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.ACCOUNT_CURRENCY;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.ACCOUNT_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.ACCOUNT_NUMBER;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.CHANNEL_REQUEST;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.ERROR_CODE;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.ERROR_INFORMATION;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.ERROR_PAYLOAD;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.EXTERNAL_ACCOUNT_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.FINERACT_RESPONSE_BODY;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.PARTY_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.PARTY_ID_TYPE;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TENANT_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TRANSACTION_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TRANSFER_CODE;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TRANSFER_CREATE_FAILED;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TRANSFER_PREPARE_FAILED;
import static org.mifos.connector.common.ams.dto.TransferActionType.CREATE;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.client.ZeebeClient;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Processor;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.json.JSONObject;
import org.mifos.connector.ams.errorhandler.ErrorTranslator;
import org.mifos.connector.ams.tenant.TenantNotExistException;
import org.mifos.connector.ams.utils.Utils;
import org.mifos.connector.ams.zeebe.ZeebeUtil;
import org.mifos.connector.common.ams.dto.ClientData;
import org.mifos.connector.common.ams.dto.Customer;
import org.mifos.connector.common.ams.dto.InteropAccountDTO;
import org.mifos.connector.common.ams.dto.LoanRepaymentDTO;
import org.mifos.connector.common.ams.dto.LoginFineractCnResponseDTO;
import org.mifos.connector.common.ams.dto.PartyFspResponseDTO;
import org.mifos.connector.common.ams.dto.ProductDefinition;
import org.mifos.connector.common.ams.dto.ProductInstance;
import org.mifos.connector.common.camel.ErrorHandlerRouteBuilder;
import org.mifos.connector.common.channel.dto.TransactionChannelRequestDTO;
import org.mifos.connector.common.exception.PaymentHubError;
import org.mifos.connector.common.mojaloop.dto.TransactionType;
import org.mifos.connector.common.mojaloop.type.InitiatorType;
import org.mifos.connector.common.mojaloop.type.Scenario;
import org.mifos.connector.common.mojaloop.type.TransactionRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
// @ConditionalOnExpression("${ams.local.enabled}")
public class InteroperationRouteBuilder extends ErrorHandlerRouteBuilder {

    @Value("${ams.local.version}")
    private String amsVersion;

    @Autowired
    private Processor pojoToString;

    @Autowired(required = false)
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

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ZeebeClient zeebeClient;

    @Autowired
    private ErrorTranslator errorTranslator;

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private String callbackUrl;
    private String fineractResponseBody;

    public InteroperationRouteBuilder() {
        super.configure();
    }

    @Override
    public void configure() {
        onException(TenantNotExistException.class).process(e -> {
            e.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, 404);
            Exception exception = e.getException();
            if (exception != null) {
                e.getIn().setBody(exception.getMessage());
            }
        }).process(clientResponseProcessor).stop();

        from("direct:get-external-account").id("get-external-account")
                .log(LoggingLevel.INFO,
                        "Get externalAccount with identifierType: ${exchangeProperty." + PARTY_ID_TYPE + "} with value: ${exchangeProperty."
                                + PARTY_ID + "}")
                // .process(amsService::getExternalAccount)
                .process(exchange -> {
                    try {
                        amsService.getExternalAccount(exchange);
                    } catch (TenantNotExistException e) {
                        log.debug(e.getMessage());
                        exchange.setProperty(ERROR_CODE, PaymentHubError.PayeeFspNotConfigured.getErrorCode());
                        exchange.setProperty(ERROR_INFORMATION, PaymentHubError.PayeeFspNotConfigured.getErrorDescription());
                        exchange.setProperty(ERROR_PAYLOAD, PaymentHubError.PayeeFspNotConfigured.getErrorDescription());
                        exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, 404);
                        exchange.setProperty(IS_ERROR_SET_MANUALLY, true);
                    }
                }).log("Response body from get-external-account").choice()
                // check if http status code is <= 202
                .when(e -> e.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class) <= 202).unmarshal()
                .json(JsonLibrary.Jackson, PartyFspResponseDTO.class)
                .process(e -> e.setProperty(EXTERNAL_ACCOUNT_ID, e.getIn().getBody(PartyFspResponseDTO.class).getAccountId()))
                .process(exchange -> {
                    PartyFspResponseDTO dto = exchange.getIn().getBody(PartyFspResponseDTO.class);

                    logger.info("Account Id: {}", dto.getAccountId());
                    logger.info("Http response code: {}", exchange.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class));
                }).when(e -> e.getProperty(IS_ERROR_SET_MANUALLY, Boolean.class) == null
                        || !e.getProperty(IS_ERROR_SET_MANUALLY, Boolean.class))
                .to("direct:error-handler").otherwise().endChoice();

        from("direct:send-local-quote").id("send-local-quote").to("direct:get-external-account")
                .log(LoggingLevel.INFO, "Sending local quote request for transaction: ${exchangeProperty." + TRANSACTION_ID + "}")
                .process(prepareLocalQuoteRequest).process(pojoToString).process(amsService::getLocalQuote).process(quoteResponseProcessor);

        from("direct:send-transfers").id("send-transfers")
                .log(LoggingLevel.INFO,
                        "Sending transfer with action: ${exchangeProperty." + TRANSFER_ACTION + "} "
                                + " for transaction: ${exchangeProperty." + TRANSACTION_ID + "}")
                .to("direct:get-external-account").process(prepareTransferRequest).process(pojoToString).process(amsService::sendTransfer)
                .to("direct:error-handler") // this route will parse and set error field if exist
                .log("Process type: ${exchangeProperty." + PROCESS_TYPE + "}").choice()
                .when(exchange -> exchange.getProperty(PROCESS_TYPE) != null && exchange.getProperty(PROCESS_TYPE).equals("api"))
                .process(exchange -> {
                    int statusCode = exchange.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class);
                    if (statusCode > 202) {
                        Map<String, Object> variables = Utils.getDefaultZeebeErrorVariable(exchange, errorTranslator);
                        variables.put(FINERACT_RESPONSE_BODY, exchange.getIn().getBody(String.class));
                        zeebeClient.newCompleteCommand(exchange.getProperty(ZEEBE_JOB_KEY, Long.class)).variables(variables).send();

                        logger.error("{}", variables.get(ERROR_INFORMATION));
                    } else {
                        Map<String, Object> variables = new HashMap<>();
                        JSONObject responseJson = new JSONObject(exchange.getIn().getBody(String.class));
                        variables.put(TRANSFER_PREPARE_FAILED, false);
                        variables.put(TRANSFER_CREATE_FAILED, false);
                        variables.put("payeeTenantId", exchange.getProperty("payeeTenantId"));
                        variables.put(TRANSFER_CODE, responseJson.getString("transferCode"));
                        logger.info("API call successful. Response Body: " + exchange.getIn().getBody(String.class));
                        variables.put(FINERACT_RESPONSE_BODY, exchange.getIn().getBody(String.class));
                        zeebeClient.newCompleteCommand(exchange.getProperty(ZEEBE_JOB_KEY, Long.class)).variables(variables).send();
                    }

                    logger.info("End of process in send-transfers");
                }).otherwise().process(transfersResponseProcessor).end();

        from("direct:send-transfers-loan").id("send-transfers-loan")
                .log(LoggingLevel.DEBUG,
                        "Sending transfer with action: ${exchangeProperty." + TRANSFER_ACTION + "} "
                                + " for transaction: ${exchangeProperty." + TRANSACTION_ID + "}")
                .log("Process type: ${exchangeProperty." + PROCESS_TYPE + "}").process(exchange -> {
                    LoanRepaymentDTO loanRepaymentDTO = ZeebeUtil.setLoanRepaymentBody(exchange);
                    String requestBody = objectMapper.writeValueAsString(loanRepaymentDTO);
                    logger.debug("Request Body : {}", requestBody);
                    exchange.getIn().setBody(requestBody);
                    exchange.setProperty("accountNumber", exchange.getProperty(ACCOUNT_NUMBER));
                }).process(amsService::repayLoan).to("direct:error-handler") // this route will parse and set error
                                                                             // field if exist
                .log("Process type: ${exchangeProperty." + PROCESS_TYPE + "}").choice()
                .when(exchange -> exchange.getProperty(PROCESS_TYPE) != null && exchange.getProperty(PROCESS_TYPE).equals("api"))
                .process(exchange -> {
                    int statusCode = exchange.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class);
                    if (statusCode > 202) {
                        Map<String, Object> variables = Utils.getDefaultZeebeErrorVariable(exchange, errorTranslator);
                        variables.put(FINERACT_RESPONSE_BODY, exchange.getIn().getBody(String.class));
                        zeebeClient.newCompleteCommand(exchange.getProperty(ZEEBE_JOB_KEY, Long.class)).variables(variables).send();

                        logger.error("{}", variables.get(ERROR_INFORMATION));
                    } else {
                        Map<String, Object> variables = new HashMap<>();
                        JSONObject responseJson = new JSONObject(exchange.getIn().getBody(String.class));
                        variables.put(TRANSFER_PREPARE_FAILED, false);
                        variables.put(TRANSFER_CREATE_FAILED, false);
                        variables.put("payeeTenantId", exchange.getProperty("payeeTenantId"));
                        variables.put(TRANSFER_CODE, responseJson.getString("transferCode"));
                        logger.info("API call successful. Response Body: " + exchange.getIn().getBody(String.class));
                        variables.put(FINERACT_RESPONSE_BODY, exchange.getIn().getBody(String.class));
                        zeebeClient.newCompleteCommand(exchange.getProperty(ZEEBE_JOB_KEY, Long.class)).variables(variables).send();
                    }

                    logger.info("End of process in send-transfers-loan");
                }).otherwise().process(transfersResponseProcessor).end();

        from("direct:fincn-oauth").id("fincn-oauth")
                .log(LoggingLevel.INFO, "Fineract CN oauth request for tenant: ${exchangeProperty." + TENANT_ID + "}")
                .process(amsService::login).unmarshal().json(JsonLibrary.Jackson, LoginFineractCnResponseDTO.class);

        // @formatter:off
        from("direct:get-party")
                .id("get-party")
                .log(LoggingLevel.INFO, "Get party information for identifierType: ${exchangeProperty." + PARTY_ID_TYPE + "} with value: ${exchangeProperty." + PARTY_ID + "}")
                .to("direct:get-external-account")
                .process(e -> e.setProperty(ACCOUNT_ID, e.getProperty(EXTERNAL_ACCOUNT_ID)))
                .choice()
                .when(e -> e.getProperty(ACCOUNT_ID) == null)
                .log(LoggingLevel.INFO, "AccountId is null")
                .process(clientResponseProcessor)
                .otherwise()
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
                    Optional<Object> account = stream(spliteratorUnknownSize(// TODO this solution is potentially bad if there are too many accounts in the system
                            new JSONObject(e.getIn().getBody(String.class)).getJSONArray("pageItems").iterator(),
                            ORDERED), false)
                            .filter(sa -> e.getProperty(ACCOUNT, String.class).equals(((JSONObject)sa).getString("accountNo")))
                            .findFirst();
                    if (!account.isPresent()) {
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
                .setProperty(INTEROP_ACCOUNT_TO_REGISTER, simple("${exchangeProperty." + ACCOUNT_ID + "}"))
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

        from("direct:add-interop-identifier-to-account").id("add-interop-identifier-to-account").process(e -> {
            JSONObject request = new JSONObject();
            request.put("accountId", e.getProperty(INTEROP_ACCOUNT_TO_REGISTER));
            e.getIn().setBody(request.toString());
        }).process(amsService::registerInteropIdentifier).process(interopPartyResponseProcessor);

        from("direct:remove-interop-identifier-from-account").id("remove-interop-identifier-from-account")
                .process(amsService::removeInteropIdentifier).process(interopPartyResponseProcessor);

        // Direct API to deposit PAYEE initiated money
        from("rest:POST:/transfer/deposit").log(LoggingLevel.INFO, "Deposit call: ${body}").unmarshal()
                .json(JsonLibrary.Jackson, TransactionChannelRequestDTO.class).process(exchange -> {
                    exchange.setProperty(PROCESS_TYPE, "api");
                    exchange.setProperty(TRANSACTION_ID, UUID.randomUUID().toString());
                    exchange.setProperty(TENANT_ID, exchange.getIn().getHeader("Platform-TenantId"));
                    exchange.setProperty(TRANSFER_ACTION, CREATE.name());

                    TransactionChannelRequestDTO transactionRequest = exchange.getIn().getBody(TransactionChannelRequestDTO.class);
                    TransactionType transactionType = new TransactionType();
                    transactionType.setInitiator(TransactionRole.PAYEE);
                    transactionType.setInitiatorType(InitiatorType.CONSUMER);
                    transactionType.setScenario(Scenario.DEPOSIT);
                    transactionRequest.setTransactionType(transactionType);

                    exchange.setProperty(CHANNEL_REQUEST, objectMapper.writeValueAsString(transactionRequest));
                    exchange.setProperty(TRANSACTION_ROLE, TransactionRole.PAYEE.name());

                    exchange.setProperty(PARTY_ID_TYPE, transactionRequest.getPayee().getPartyIdInfo().getPartyIdType());
                    exchange.setProperty(PARTY_ID, transactionRequest.getPayee().getPartyIdInfo().getPartyIdentifier());
                }).to("direct:send-transfers");

        from("direct:send-callback").id("send-callback")
                .log(LoggingLevel.DEBUG,
                        "Sending callback with action: ${exchangeProperty." + TRANSFER_ACTION + "} "
                                + " for transaction: ${exchangeProperty." + TRANSACTION_ID + "}")
                .log("Process type: ${exchangeProperty." + PROCESS_TYPE + "}").process(exchange -> {
                    if (exchange.getProperties().containsKey(FINERACT_RESPONSE_BODY)) {
                        fineractResponseBody = exchange.getProperty(FINERACT_RESPONSE_BODY).toString();
                    }
                    callbackUrl = exchange.getProperty(X_CALLBACKURL).toString();
                    boolean callbackSent = amsService.sendCallback(callbackUrl, fineractResponseBody);
                    exchange.setProperty("callbackSent", callbackSent);
                });
    }
}
