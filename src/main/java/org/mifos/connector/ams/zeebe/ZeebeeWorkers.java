package org.mifos.connector.ams.zeebe;

import static org.mifos.connector.ams.camel.config.CamelProperties.PROCESS_TYPE;
import static org.mifos.connector.ams.camel.config.CamelProperties.QUOTE_AMOUNT_TYPE;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ROLE;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSFER_ACTION;
import static org.mifos.connector.ams.camel.config.CamelProperties.X_CALLBACKURL;
import static org.mifos.connector.ams.camel.config.CamelProperties.ZEEBE_JOB_KEY;
import static org.mifos.connector.ams.zeebe.ZeebeUtil.zeebeVariable;
import static org.mifos.connector.ams.zeebe.ZeebeUtil.zeebeVariablesToCamelProperties;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.ACCOUNT;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.ACCOUNT_CURRENCY;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.ACCOUNT_IDENTIFIER;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.ACCOUNT_NUMBER;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.BOOK_TRANSACTION_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.CALLBACK_SUCCESS;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.CHANNEL_REQUEST;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.EXTERNAL_ACCOUNT_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.FINERACT_RESPONSE_BODY;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.INTEROP_REGISTRATION_FAILED;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.LOCAL_QUOTE_FAILED;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.LOCAL_QUOTE_RESPONSE;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.NOTE;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.PARTY_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.PARTY_ID_TYPE;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.PAYEE_PARTY_RESPONSE;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.QUOTE_FAILED;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.QUOTE_SWITCH_REQUEST;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TENANT_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TRANSACTION_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TRANSFER_CODE;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TRANSFER_PREPARE_FAILED;
import static org.mifos.connector.common.ams.dto.TransferActionType.CREATE;
import static org.mifos.connector.common.ams.dto.TransferActionType.PREPARE;
import static org.mifos.connector.common.ams.dto.TransferActionType.RELEASE;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.support.DefaultExchange;
import org.json.JSONObject;
import org.mifos.connector.ams.properties.TenantProperties;
import org.mifos.connector.common.ams.dto.QuoteFspResponseDTO;
import org.mifos.connector.common.channel.dto.TransactionChannelRequestDTO;
import org.mifos.connector.common.gsma.dto.GsmaTransfer;
import org.mifos.connector.common.mojaloop.dto.FspMoneyData;
import org.mifos.connector.common.mojaloop.dto.MoneyData;
import org.mifos.connector.common.mojaloop.dto.Party;
import org.mifos.connector.common.mojaloop.dto.PartyIdInfo;
import org.mifos.connector.common.mojaloop.dto.QuoteSwitchRequestDTO;
import org.mifos.connector.common.mojaloop.dto.TransactionType;
import org.mifos.connector.common.mojaloop.type.AmountType;
import org.mifos.connector.common.mojaloop.type.IdentifierType;
import org.mifos.connector.common.mojaloop.type.InitiatorType;
import org.mifos.connector.common.mojaloop.type.Scenario;
import org.mifos.connector.common.mojaloop.type.TransactionRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ZeebeeWorkers {

    public static final String WORKER_PARTY_LOOKUP_LOCAL = "party-lookup-local-";
    public static final String WORKER_PAYEE_COMMIT_TRANSFER = "payee-commit-transfer-";
    public static final String WORKER_PAYEE_QUOTE = "payee-quote-";
    public static final String WORKER_PAYER_LOCAL_QUOTE = "payer-local-quote-";
    public static final String WORKER_INTEROP_PARTY_REGISTRATION = "interop-party-registration-";
    public static final String WORKER_PAYEE_DEPOSIT_TRANSFER = "payee-deposit-transfer-";
    public static final String WORKER_PAYEE_LOAN_TRANSFER = "payee-loan-transfer-";
    public static final String WORKER_ACCOUNT_IDENTIFIER = "account-identifier-";
    public static final String WORKER_SEND_CALLBACK = "send-callback";

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired(required = false)
    private ZeebeClient zeebeClient;

    @Autowired
    private ProducerTemplate producerTemplate;

    @Autowired
    private CamelContext camelContext;

    @Autowired
    private ZeebeUtil zeebeUtil;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private TenantProperties tenantProperties;

    @Value("${ams.local.enabled:false}")
    private boolean isAmsLocalEnabled;

    @Value("#{'${dfspids}'.split(',')}")
    private List<String> dfspids;

    @Value("${zeebe.client.evenly-allocated-max-jobs}")
    private int workerMaxJobs;

    @Value("${zeebe.enabled:true}")
    private boolean isZeebeEnabled;

    @Value("${interop-party-registration.enabled}")
    private boolean interopPartyRegistrationEnabled;

    @PostConstruct
    public void setupWorkers() {
        if (isZeebeEnabled) {
            zeebeClient.newWorker().jobType("block-funds").handler((client, job) -> {
                logWorkerDetails(job);
                if (isAmsLocalEnabled) {
                    Exchange ex = new DefaultExchange(camelContext);
                    zeebeVariablesToCamelProperties(job.getVariablesAsMap(), ex, TRANSACTION_ID, CHANNEL_REQUEST, EXTERNAL_ACCOUNT_ID,
                            TENANT_ID, LOCAL_QUOTE_RESPONSE, PROCESS_TYPE);
                    TransactionChannelRequestDTO channelRequest = objectMapper.readValue(ex.getProperty(CHANNEL_REQUEST, String.class),
                            TransactionChannelRequestDTO.class);
                    ex.setProperty(PARTY_ID_TYPE, channelRequest.getPayer().getPartyIdInfo().getPartyIdType().name());
                    ex.setProperty(PARTY_ID, channelRequest.getPayer().getPartyIdInfo().getPartyIdentifier());
                    ex.setProperty(TRANSFER_ACTION, PREPARE.name());
                    ex.setProperty(ZEEBE_JOB_KEY, job.getKey());
                    ex.setProperty(TRANSACTION_ROLE, TransactionRole.PAYER.name());
                    ex.setProperty("payeeTenantId", job.getVariablesAsMap().get("payeeTenantId"));
                    logger.debug("Payee Id before block funds {}", job.getVariablesAsMap().get("payeeTenantId"));
                    producerTemplate.send("direct:send-transfers", ex);
                    logger.info("Zeebe variable {}", job.getVariablesAsMap());
                } else {
                    Map<String, Object> variables = new HashMap<>();
                    variables.put(TRANSFER_PREPARE_FAILED, false);
                    zeebeClient.newCompleteCommand(job.getKey()).variables(variables).send();
                }
            }).name("block-funds").maxJobsActive(workerMaxJobs).open();

            zeebeClient.newWorker().jobType("book-funds").handler((client, job) -> {
                logWorkerDetails(job);
                if (isAmsLocalEnabled) {
                    Exchange ex = new DefaultExchange(camelContext);
                    zeebeVariablesToCamelProperties(job.getVariablesAsMap(), ex, TRANSACTION_ID, CHANNEL_REQUEST, TENANT_ID,
                            EXTERNAL_ACCOUNT_ID, LOCAL_QUOTE_RESPONSE, TRANSFER_CODE);
                    TransactionChannelRequestDTO channelRequest = objectMapper.readValue(ex.getProperty(CHANNEL_REQUEST, String.class),
                            TransactionChannelRequestDTO.class);
                    ex.setProperty(PARTY_ID_TYPE, channelRequest.getPayer().getPartyIdInfo().getPartyIdType().name());
                    ex.setProperty(PARTY_ID, channelRequest.getPayer().getPartyIdInfo().getPartyIdentifier());
                    ex.setProperty(TRANSFER_ACTION, CREATE.name());
                    ex.setProperty(ZEEBE_JOB_KEY, job.getKey());
                    ex.setProperty(TRANSACTION_ROLE, TransactionRole.PAYER.name());
                    ex.setProperty("payeeTenantId", job.getVariablesAsMap().get("payeeTenantId"));
                    ex.setProperty("processType", "api");
                    producerTemplate.send("direct:send-transfers", ex);
                } else {
                    Map<String, Object> variables = new HashMap<>();
                    variables.put("transferCreateFailed", false);
                    zeebeClient.newCompleteCommand(job.getKey()).variables(variables).send();
                }
            }).name("book-funds").maxJobsActive(workerMaxJobs).open();

            zeebeClient.newWorker().jobType("release-block").handler((client, job) -> {
                logWorkerDetails(job);
                if (isAmsLocalEnabled) {
                    Exchange ex = new DefaultExchange(camelContext);
                    zeebeVariablesToCamelProperties(job.getVariablesAsMap(), ex, TRANSACTION_ID, CHANNEL_REQUEST, TENANT_ID,
                            EXTERNAL_ACCOUNT_ID, LOCAL_QUOTE_RESPONSE, TRANSFER_CODE);
                    TransactionChannelRequestDTO channelRequest = objectMapper.readValue(ex.getProperty(CHANNEL_REQUEST, String.class),
                            TransactionChannelRequestDTO.class);
                    ex.setProperty(PARTY_ID_TYPE, channelRequest.getPayer().getPartyIdInfo().getPartyIdType().name());
                    ex.setProperty(PARTY_ID, channelRequest.getPayer().getPartyIdInfo().getPartyIdentifier());
                    ex.setProperty(TRANSFER_ACTION, RELEASE.name());
                    ex.setProperty(ZEEBE_JOB_KEY, job.getKey());
                    ex.setProperty(TRANSACTION_ROLE, TransactionRole.PAYEE.name());
                    ex.setProperty("payeeTenantId", job.getVariablesAsMap().get("payeeTenantId"));
                    producerTemplate.send("direct:send-transfers", ex);
                } else {
                    Map<String, Object> variables = new HashMap<>();
                    variables.put("transferReleaseFailed", false);
                    zeebeClient.newCompleteCommand(job.getKey()).variables(variables).send();
                }
            }).name("release-block").maxJobsActive(workerMaxJobs).open();

            for (String dfspid : dfspids) {
                logger.info("DFSPID {}", dfspid);
                logger.info("## generating " + WORKER_PAYER_LOCAL_QUOTE + "{} worker", dfspid);
                zeebeClient.newWorker().jobType(WORKER_PAYER_LOCAL_QUOTE + dfspid).handler((client, job) -> {
                    logWorkerDetails(job);
                    if (isAmsLocalEnabled) {
                        Map<String, Object> existingVariables = job.getVariablesAsMap();
                        TransactionChannelRequestDTO channelRequest = objectMapper
                                .readValue((String) existingVariables.get(CHANNEL_REQUEST), TransactionChannelRequestDTO.class);

                        Exchange ex = new DefaultExchange(camelContext);
                        zeebeVariablesToCamelProperties(existingVariables, ex, CHANNEL_REQUEST, TENANT_ID, TRANSACTION_ID);

                        ex.setProperty(PARTY_ID_TYPE, channelRequest.getPayer().getPartyIdInfo().getPartyIdType().name());
                        ex.setProperty(PARTY_ID, channelRequest.getPayer().getPartyIdInfo().getPartyIdentifier());
                        ex.setProperty(ZEEBE_JOB_KEY, job.getKey());
                        ex.setProperty(TRANSACTION_ROLE, TransactionRole.PAYER);
                        ex.setProperty(QUOTE_AMOUNT_TYPE, AmountType.SEND.name());
                        producerTemplate.send("direct:send-local-quote", ex);
                    } else {
                        Map<String, Object> variables = new HashMap<>();
                        variables.put(LOCAL_QUOTE_FAILED, false);
                        zeebeClient.newCompleteCommand(job.getKey()).variables(variables).send();
                    }
                }).name(WORKER_PAYER_LOCAL_QUOTE + dfspid).maxJobsActive(workerMaxJobs).open();

                logger.info("## generating " + WORKER_PAYEE_QUOTE + "{} worker", dfspid);
                zeebeClient.newWorker().jobType(WORKER_PAYEE_QUOTE + dfspid).handler((client, job) -> {
                    logWorkerDetails(job);
                    Map<String, Object> existingVariables = job.getVariablesAsMap();
                    QuoteSwitchRequestDTO quoteRequest = objectMapper.readValue((String) existingVariables.get(QUOTE_SWITCH_REQUEST),
                            QuoteSwitchRequestDTO.class);

                    if (isAmsLocalEnabled) {
                        TransactionChannelRequestDTO channelRequest = new TransactionChannelRequestDTO();
                        TransactionType transactionType = new TransactionType();
                        transactionType.setInitiator(TransactionRole.PAYEE);
                        transactionType.setInitiatorType(InitiatorType.CONSUMER);
                        transactionType.setScenario(Scenario.DEPOSIT);
                        channelRequest.setTransactionType(transactionType);
                        channelRequest.setAmountType(AmountType.RECEIVE);
                        MoneyData amount = new MoneyData(quoteRequest.getAmount().getAmount(), quoteRequest.getAmount().getCurrency());
                        channelRequest.setAmount(amount);

                        Exchange ex = new DefaultExchange(camelContext);
                        ex.setProperty(PARTY_ID, quoteRequest.getPayee().getPartyIdInfo().getPartyIdentifier());
                        ex.setProperty(PARTY_ID_TYPE, quoteRequest.getPayee().getPartyIdInfo().getPartyIdType());
                        ex.setProperty(TRANSACTION_ID, existingVariables.get(TRANSACTION_ID));
                        ex.setProperty(TENANT_ID, existingVariables.get(TENANT_ID));
                        ex.setProperty(TRANSACTION_ROLE, TransactionRole.PAYEE.name());
                        ex.setProperty(CHANNEL_REQUEST, objectMapper.writeValueAsString(channelRequest));
                        ex.setProperty(ZEEBE_JOB_KEY, job.getKey());
                        ex.setProperty(QUOTE_AMOUNT_TYPE, quoteRequest.getAmountType().name());
                        producerTemplate.send("direct:send-local-quote", ex);
                    } else {
                        Map<String, Object> variables = createFreeQuote(quoteRequest.getAmount().getCurrency());
                        variables.put(QUOTE_FAILED, false);
                        zeebeClient.newCompleteCommand(job.getKey()).variables(variables).send();
                    }
                }).name(WORKER_PAYEE_QUOTE + dfspid).maxJobsActive(workerMaxJobs).open();

                logger.info("## generating " + WORKER_PAYEE_COMMIT_TRANSFER + "{} worker", dfspid);
                zeebeClient.newWorker().jobType(WORKER_PAYEE_COMMIT_TRANSFER + dfspid).handler((client, job) -> {
                    logWorkerDetails(job);
                    if (isAmsLocalEnabled) {
                        Exchange exchange = new DefaultExchange(camelContext);
                        Map<String, Object> variables = job.getVariablesAsMap();
                        zeebeVariablesToCamelProperties(variables, exchange, BOOK_TRANSACTION_ID, TRANSACTION_ID, TENANT_ID,
                                EXTERNAL_ACCOUNT_ID, LOCAL_QUOTE_RESPONSE);
                        exchange.setProperty(TRANSFER_ACTION, CREATE.name());
                        exchange.setProperty(ZEEBE_JOB_KEY, job.getKey());

                        // setting party related variables as exchange property
                        QuoteSwitchRequestDTO quoteRequest = objectMapper.readValue((String) variables.get(QUOTE_SWITCH_REQUEST),
                                QuoteSwitchRequestDTO.class);
                        exchange.setProperty(PARTY_ID, quoteRequest.getPayee().getPartyIdInfo().getPartyIdentifier());
                        exchange.setProperty(PARTY_ID_TYPE, quoteRequest.getPayee().getPartyIdInfo().getPartyIdType());

                        FspMoneyData amountData = zeebeVariable(exchange, "amount", FspMoneyData.class);
                        MoneyData amount = new MoneyData(amountData.getAmount(), amountData.getCurrency());

                        TransactionChannelRequestDTO transactionRequest = new TransactionChannelRequestDTO();
                        TransactionType transactionType = new TransactionType();
                        transactionType.setInitiator(TransactionRole.PAYEE);
                        transactionType.setInitiatorType(InitiatorType.CONSUMER);
                        transactionType.setScenario(Scenario.DEPOSIT);
                        transactionRequest.setTransactionType(transactionType);
                        transactionRequest.setAmount(amount);
                        exchange.setProperty(CHANNEL_REQUEST, objectMapper.writeValueAsString(transactionRequest));
                        exchange.setProperty(TRANSACTION_ROLE, TransactionRole.PAYEE.name());

                        logger.info("####### transaction request {}", objectMapper.writeValueAsString(transactionRequest));
                        producerTemplate.send("direct:send-transfers", exchange);
                    } else {
                        Map<String, Object> variables = new HashMap<>();
                        variables.put("transferCreateFailed", false);
                        zeebeClient.newCompleteCommand(job.getKey()).variables(variables).send();
                    }
                }).name(WORKER_PAYEE_COMMIT_TRANSFER + dfspid).maxJobsActive(workerMaxJobs).open();

                logger.info("## generating " + WORKER_PARTY_LOOKUP_LOCAL + "{} worker", dfspid);
                zeebeClient.newWorker().jobType(WORKER_PARTY_LOOKUP_LOCAL + dfspid).handler((client, job) -> {
                    logWorkerDetails(job);
                    Map<String, Object> existingVariables = job.getVariablesAsMap();
                    String partyIdType = (String) existingVariables.get(PARTY_ID_TYPE);
                    String partyId = (String) existingVariables.get(PARTY_ID);
                    String tenantId = (String) existingVariables.get(TENANT_ID); // payer
                    if (isAmsLocalEnabled) {
                        Exchange ex = new DefaultExchange(camelContext);
                        ex.setProperty(PARTY_ID_TYPE, partyIdType);
                        ex.setProperty(PARTY_ID, partyId);
                        ex.setProperty(ZEEBE_JOB_KEY, job.getKey());
                        if (dfspid.equalsIgnoreCase(existingVariables.get("payeeTenantId").toString())) {
                            ex.setProperty(TENANT_ID, existingVariables.get("payeeTenantId"));
                        } else {
                            ex.setProperty(TENANT_ID, tenantId);
                        }
                        ex.setProperty("payeeTenantId", existingVariables.get("payeeTenantId"));

                        producerTemplate.send("direct:get-party", ex);

                        /*
                         * payeeTenantId == dfspid => payee else payer
                         *
                         * a = payer tenant b = payee tenant
                         *
                         * debit -> gorilla credit -> gorilla
                         *
                         * PLATFORM-TENANT-ID -> PAYER/PAYEE
                         */
                    } else {
                        Map<String, Object> variables = new HashMap<>();
                        Party party = new Party(// only return fspId from configuration
                                new PartyIdInfo(IdentifierType.valueOf(partyIdType), partyId, null,
                                        tenantProperties.getTenant(tenantId).getFspId()),
                                null, null, null);

                        variables.put(PAYEE_PARTY_RESPONSE, objectMapper.writeValueAsString(party));
                        variables.put("payeeTenantId", existingVariables.get("payeeTenantId"));
                        client.newCompleteCommand(job.getKey()).variables(variables).send();
                    }
                }).name(WORKER_PARTY_LOOKUP_LOCAL + dfspid).maxJobsActive(workerMaxJobs).open();

                logger.info("## generating " + WORKER_INTEROP_PARTY_REGISTRATION + "{} worker", dfspid);
                zeebeClient.newWorker().jobType(WORKER_INTEROP_PARTY_REGISTRATION + dfspid).handler((client, job) -> {
                    logWorkerDetails(job);
                    Map<String, Object> existingVariables = job.getVariablesAsMap();

                    if (!interopPartyRegistrationEnabled) {
                        Map<String, Object> variables = new HashMap<>();
                        variables.put(ACCOUNT_CURRENCY, "USD");
                        variables.put(INTEROP_REGISTRATION_FAILED, false);
                        client.newCompleteCommand(job.getKey()).variables(variables).send();
                        logger.info("Interop disabled with variables {}", variables);
                        return;
                    }

                    if (isAmsLocalEnabled) {
                        Exchange ex = new DefaultExchange(camelContext);
                        ex.setProperty(PARTY_ID_TYPE, existingVariables.get(PARTY_ID_TYPE));
                        ex.setProperty(PARTY_ID, existingVariables.get(PARTY_ID));
                        ex.setProperty(ACCOUNT, existingVariables.get(ACCOUNT));
                        ex.setProperty(TENANT_ID, existingVariables.get(TENANT_ID));
                        ex.setProperty(ZEEBE_JOB_KEY, job.getKey());
                        producerTemplate.send("direct:register-party", ex);
                    } else {
                        Map<String, Object> variables = new HashMap<>();
                        variables.put(ACCOUNT_CURRENCY, "TZS");
                        client.newCompleteCommand(job.getKey()).variables(variables).send();
                    }
                }).name(WORKER_INTEROP_PARTY_REGISTRATION + dfspid).maxJobsActive(workerMaxJobs).open();

                logger.info("## generating " + WORKER_PAYEE_DEPOSIT_TRANSFER + "{} worker", dfspid);
                zeebeClient.newWorker().jobType(WORKER_PAYEE_DEPOSIT_TRANSFER + dfspid).handler((client, job) -> {
                    logWorkerDetails(job);
                    Map<String, Object> existingVariables = job.getVariablesAsMap();
                    logger.info("Exisiting variables {}", existingVariables);

                    String tenantId = (String) existingVariables.get(TENANT_ID);

                    Exchange ex = new DefaultExchange(camelContext);
                    Map<String, Object> variables = job.getVariablesAsMap();
                    zeebeVariablesToCamelProperties(variables, ex, TRANSACTION_ID, TENANT_ID, EXTERNAL_ACCOUNT_ID, CHANNEL_REQUEST);
                    ex.setProperty(TRANSFER_ACTION, CREATE.name());
                    ex.setProperty("payeeTenantId", existingVariables.get("payeeTenantId"));
                    ex.setProperty(ZEEBE_JOB_KEY, job.getKey());

                    TransactionChannelRequestDTO transactionRequest = objectMapper.readValue((String) variables.get(CHANNEL_REQUEST),
                            TransactionChannelRequestDTO.class);
                    TransactionType transactionType = new TransactionType();
                    transactionType.setInitiator(TransactionRole.PAYEE);
                    transactionType.setInitiatorType(InitiatorType.CONSUMER);
                    transactionType.setScenario(Scenario.DEPOSIT);
                    transactionRequest.setTransactionType(transactionType);
                    variables.put("initiator", transactionType.getInitiator().name());
                    variables.put("initiatorType", transactionType.getInitiatorType().name());
                    variables.put("scenario", transactionType.getScenario().name());
                    variables.get(NOTE);
                    variables.put("amount", new FspMoneyData(transactionRequest.getAmount().getAmountDecimal(),
                            transactionRequest.getAmount().getCurrency()));
                    variables.put("processType", "api");

                    String partyIdType = transactionRequest.getPayee().getPartyIdInfo().getPartyIdType().name();
                    String partyId = transactionRequest.getPayee().getPartyIdInfo().getPartyIdentifier();
                    ex.setProperty(PARTY_ID_TYPE, partyIdType);
                    ex.setProperty(PARTY_ID, partyId);
                    logger.info("PartyIdType: {}, PartyId: {}", partyIdType, partyId);

                    ex.setProperty(CHANNEL_REQUEST, objectMapper.writeValueAsString(transactionRequest));
                    ex.setProperty(TRANSACTION_ROLE, TransactionRole.PAYEE.name());
                    producerTemplate.send("direct:send-transfers", ex);
                    variables.put("transferCreateFailed", false);
                    variables.put("payeeTenantId", existingVariables.get("payeeTenantId"));
                    zeebeClient.newCompleteCommand(job.getKey()).variables(variables).send();
                }).name(WORKER_PAYEE_DEPOSIT_TRANSFER + dfspid).maxJobsActive(workerMaxJobs).open();

                logger.info("## generating {}" + "{} worker", WORKER_PAYEE_LOAN_TRANSFER, dfspid);
                zeebeClient.newWorker().jobType(WORKER_PAYEE_LOAN_TRANSFER + dfspid).handler((client, job) -> {
                    logWorkerDetails(job);
                    Map<String, Object> existingVariables = job.getVariablesAsMap();
                    logger.debug("Exisiting variables {}", existingVariables);

                    String tenantId = (String) existingVariables.get(TENANT_ID);

                    Exchange ex = new DefaultExchange(camelContext);
                    Map<String, Object> variables = job.getVariablesAsMap();
                    zeebeVariablesToCamelProperties(variables, ex, TRANSACTION_ID, TENANT_ID, CHANNEL_REQUEST);
                    ex.setProperty(TRANSFER_ACTION, CREATE.name());
                    ex.setProperty("payeeTenantId", existingVariables.get("payeeTenantId"));
                    ex.setProperty(ZEEBE_JOB_KEY, job.getKey());

                    TransactionChannelRequestDTO transactionRequest = objectMapper.readValue((String) variables.get(CHANNEL_REQUEST),
                            TransactionChannelRequestDTO.class);

                    ZeebeUtil.setZeebeVariablesLoanWorker(variables, transactionRequest);

                    String partyIdType = transactionRequest.getPayee().getPartyIdInfo().getPartyIdType().name();
                    String partyId = transactionRequest.getPayee().getPartyIdInfo().getPartyIdentifier();

                    ZeebeUtil.setExchangePropertyLoan(ex, partyId, partyIdType, transactionRequest, existingVariables);

                    producerTemplate.send("direct:send-transfers-loan", ex);
                    variables.put("transferCreateFailed", false);
                    variables.put("payeeTenantId", existingVariables.get("payeeTenantId"));
                    zeebeClient.newCompleteCommand(job.getKey()).variables(variables).send();
                }).name(WORKER_PAYEE_LOAN_TRANSFER + dfspid).maxJobsActive(workerMaxJobs).open();

                logger.info("## generating " + WORKER_ACCOUNT_IDENTIFIER + "{} worker", dfspid);
                zeebeClient.newWorker().jobType(WORKER_ACCOUNT_IDENTIFIER + dfspid).handler((client, job) -> {
                    logWorkerDetails(job);
                    Map<String, Object> existingVariables = job.getVariablesAsMap();
                    logger.debug("Exisiting variables {}", existingVariables);
                    String accountHoldingInstitutionId = (String) existingVariables.get(TENANT_ID);
                    Exchange ex = new DefaultExchange(camelContext);
                    Map<String, Object> variables = job.getVariablesAsMap();
                    GsmaTransfer gsmaTransfer = objectMapper.readValue((String) variables.get(CHANNEL_REQUEST), GsmaTransfer.class);
                    logger.debug("GSMA Transfer Body:{}", gsmaTransfer);
                    String accountNo = gsmaTransfer.getPayee().get(0).getPartyIdIdentifier();
                    Exchange e = zeebeUtil.setAccountTypeAndNumber(ex, accountNo);
                    String transactionChannelRequestDTO = ZeebeUtil.convertGsmaTransfertoTransactionChannel(gsmaTransfer,
                            e.getProperty(ACCOUNT_NUMBER));
                    logger.debug("Transaction Channel Request DTO:{}", transactionChannelRequestDTO);
                    logger.debug("Account Identifier:{}", ACCOUNT_IDENTIFIER);

                    ZeebeUtil.setZeebeVariables(e, variables, gsmaTransfer.getRequestDate(), accountHoldingInstitutionId,
                            transactionChannelRequestDTO);

                    zeebeClient.newCompleteCommand(job.getKey()).variables(variables).send();
                }).name(WORKER_ACCOUNT_IDENTIFIER + dfspid).maxJobsActive(workerMaxJobs).open();

                logger.info("## generating {} worker", WORKER_SEND_CALLBACK);
                zeebeClient.newWorker().jobType(WORKER_SEND_CALLBACK).handler((client, job) -> {
                    logWorkerDetails(job);
                    Map<String, Object> existingVariables = job.getVariablesAsMap();
                    logger.debug("Exisiting variables {}", existingVariables);
                    Exchange ex = new DefaultExchange(camelContext);
                    Map<String, Object> variables = job.getVariablesAsMap();

                    ex.setProperty(X_CALLBACKURL, existingVariables.get(X_CALLBACKURL));
                    ex.setProperty(FINERACT_RESPONSE_BODY, existingVariables.get(FINERACT_RESPONSE_BODY));
                    producerTemplate.send("direct:send-callback", ex);
                    variables.put(CALLBACK_SUCCESS, ex.getProperty("callbackSent").toString());
                    zeebeClient.newCompleteCommand(job.getKey()).variables(variables).send();

                }).name(WORKER_SEND_CALLBACK).maxJobsActive(workerMaxJobs).open();
            }
        }
    }

    private void logWorkerDetails(ActivatedJob job) {
        JSONObject jsonJob = new JSONObject();
        jsonJob.put("bpmnProcessId", job.getBpmnProcessId());
        jsonJob.put("elementInstanceKey", job.getElementInstanceKey());
        jsonJob.put("jobKey", job.getKey());
        jsonJob.put("jobType", job.getType());
        jsonJob.put("workflowElementId", job.getElementId());
        jsonJob.put("workflowDefinitionVersion", job.getProcessDefinitionVersion());
        jsonJob.put("workflowKey", job.getProcessDefinitionKey());
        jsonJob.put("workflowInstanceKey", job.getProcessInstanceKey());
        logger.info("Job started: {}", jsonJob.toString(4));
    }

    private Map<String, Object> createFreeQuote(String currency) throws Exception {
        FspMoneyData fspFee = new FspMoneyData(BigDecimal.ZERO, currency);
        FspMoneyData fspCommission = new FspMoneyData(BigDecimal.ZERO, currency);

        QuoteFspResponseDTO response = new QuoteFspResponseDTO();
        response.setFspFee(fspFee);
        response.setFspCommission(fspCommission);

        Map<String, Object> variables = new HashMap<>();
        variables.put(LOCAL_QUOTE_RESPONSE, objectMapper.writeValueAsString(response));
        variables.put("fspFee", fspFee);
        variables.put("fspCommission", fspCommission);
        return variables;
    }
}
