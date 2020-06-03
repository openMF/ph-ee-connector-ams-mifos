package org.mifos.connector.ams.zeebe;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.zeebe.client.ZeebeClient;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.support.DefaultExchange;
import org.mifos.connector.ams.properties.Tenant;
import org.mifos.connector.ams.properties.TenantProperties;
import org.mifos.connector.common.ams.dto.QuoteFspResponseDTO;
import org.mifos.connector.common.channel.dto.TransactionChannelRequestDTO;
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

import javax.annotation.PostConstruct;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mifos.connector.ams.camel.config.CamelProperties.CHANNEL_REQUEST;
import static org.mifos.connector.ams.camel.config.CamelProperties.EXTERNAL_ACCOUNT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.LOCAL_QUOTE_RESPONSE;
import static org.mifos.connector.ams.camel.config.CamelProperties.PARTY_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.PARTY_ID_TYPE;
import static org.mifos.connector.ams.camel.config.CamelProperties.PAYEE_PARTY_RESPONSE;
import static org.mifos.connector.ams.camel.config.CamelProperties.QUOTE_AMOUNT_TYPE;
import static org.mifos.connector.ams.camel.config.CamelProperties.QUOTE_SWITCH_REQUEST;
import static org.mifos.connector.ams.camel.config.CamelProperties.TENANT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ROLE;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSFER_ACTION;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSFER_CODE;
import static org.mifos.connector.ams.camel.config.CamelProperties.ZEEBE_JOB_KEY;
import static org.mifos.connector.ams.zeebe.ZeebeExpressionVariables.LOCAL_QUOTE_FAILED;
import static org.mifos.connector.ams.zeebe.ZeebeExpressionVariables.QUOTE_FAILED;
import static org.mifos.connector.ams.zeebe.ZeebeUtil.zeebeVariablesToCamelProperties;
import static org.mifos.connector.common.ams.dto.TransferActionType.CREATE;
import static org.mifos.connector.common.ams.dto.TransferActionType.PREPARE;
import static org.mifos.connector.common.ams.dto.TransferActionType.RELEASE;

@Component
public class ZeebeeWorkers {

    public static final String WORKER_PARTY_LOOKUP_LOCAL = "party-lookup-local-";
    public static final String WORKER_PAYEE_COMMIT_TRANSFER = "payee-commit-transfer-";
    public static final String WORKER_PAYEE_QUOTE = "payee-quote-";
    public static final String WORKER_PAYER_LOCAL_QUOTE = "payer-local-quote-";

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private ZeebeClient zeebeClient;

    @Autowired
    private ProducerTemplate producerTemplate;

    @Autowired
    private CamelContext camelContext;

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

    @PostConstruct
    public void setupWorkers() {
        zeebeClient.newWorker()
                .jobType("block-funds")
                .handler((client, job) -> {
                    logger.info("Job '{}' started from process '{}' with key {}", job.getType(), job.getBpmnProcessId(), job.getKey());
                    if (isAmsLocalEnabled) {
                        Exchange ex = new DefaultExchange(camelContext);
                        zeebeVariablesToCamelProperties(job.getVariablesAsMap(), ex,
                                TRANSACTION_ID,
                                CHANNEL_REQUEST,
                                TENANT_ID,
                                EXTERNAL_ACCOUNT_ID,
                                LOCAL_QUOTE_RESPONSE);
                        ex.setProperty(TRANSFER_ACTION, PREPARE.name());
                        ex.setProperty(ZEEBE_JOB_KEY, job.getKey());
                        ex.setProperty(TRANSACTION_ROLE, TransactionRole.PAYER.name());
                        producerTemplate.send("direct:send-transfers", ex);
                    } else {
                        zeebeClient.newCompleteCommand(job.getKey())
                                .send()
                                .join();
                    }
                })
                .name("block-funds")
                .maxJobsActive(workerMaxJobs)
                .open();

        zeebeClient.newWorker()
                .jobType("book-funds")
                .handler((client, job) -> {
                    logger.info("Job '{}' started from process '{}' with key {}", job.getType(), job.getBpmnProcessId(), job.getKey());
                    if (isAmsLocalEnabled) {
                        Exchange ex = new DefaultExchange(camelContext);
                        zeebeVariablesToCamelProperties(job.getVariablesAsMap(), ex,
                                TRANSACTION_ID,
                                CHANNEL_REQUEST,
                                TENANT_ID,
                                EXTERNAL_ACCOUNT_ID,
                                LOCAL_QUOTE_RESPONSE,
                                TRANSFER_CODE);
                        ex.setProperty(TRANSFER_ACTION, CREATE.name());
                        ex.setProperty(ZEEBE_JOB_KEY, job.getKey());
                        ex.setProperty(TRANSACTION_ROLE, TransactionRole.PAYER.name());
                        producerTemplate.send("direct:send-transfers", ex);
                    } else {
                        zeebeClient.newCompleteCommand(job.getKey())
                                .send()
                                .join();
                    }
                })
                .name("book-funds")
                .maxJobsActive(workerMaxJobs)
                .open();

        zeebeClient.newWorker()
                .jobType("release-block")
                .handler((client, job) -> {
                    logger.info("Job '{}' started from process '{}' with key {}", job.getType(), job.getBpmnProcessId(), job.getKey());
                    if (isAmsLocalEnabled) {
                        Exchange ex = new DefaultExchange(camelContext);
                        zeebeVariablesToCamelProperties(job.getVariablesAsMap(), ex,
                                TRANSACTION_ID,
                                CHANNEL_REQUEST,
                                TENANT_ID,
                                EXTERNAL_ACCOUNT_ID,
                                LOCAL_QUOTE_RESPONSE,
                                TRANSFER_CODE);
                        ex.setProperty(TRANSFER_ACTION, RELEASE.name());
                        ex.setProperty(ZEEBE_JOB_KEY, job.getKey());
                        ex.setProperty(TRANSACTION_ROLE, TransactionRole.PAYEE.name());
                        producerTemplate.send("direct:send-transfers", ex);
                    } else {
                        zeebeClient.newCompleteCommand(job.getKey())
                                .send()
                                .join();
                    }
                })
                .name("release-block")
                .maxJobsActive(workerMaxJobs)
                .open();

        for (String dfspid : dfspids) {
            logger.info("## generating " + WORKER_PAYER_LOCAL_QUOTE + "{} worker", dfspid);
            zeebeClient.newWorker()
                    .jobType(WORKER_PAYER_LOCAL_QUOTE + dfspid)
                    .handler((client, job) -> {
                        logger.info("Job '{}' started from process '{}' with key {}", job.getType(), job.getBpmnProcessId(), job.getKey());
                        if (isAmsLocalEnabled) {
                            Map<String, Object> variables = job.getVariablesAsMap();
                            TransactionChannelRequestDTO channelRequest = objectMapper.readValue((String) variables.get(CHANNEL_REQUEST), TransactionChannelRequestDTO.class);
                            String partyIdType = channelRequest.getPayer().getPartyIdInfo().getPartyIdType().name();
                            String partyIdentifier = channelRequest.getPayer().getPartyIdInfo().getPartyIdentifier();
                            Tenant tenant = tenantProperties.getTenant(partyIdType, partyIdentifier);

                            Exchange ex = new DefaultExchange(camelContext);
                            zeebeVariablesToCamelProperties(variables, ex,
                                    CHANNEL_REQUEST,
                                    TRANSACTION_ID);

                            ex.setProperty(PARTY_ID_TYPE, partyIdType);
                            ex.setProperty(PARTY_ID, partyIdentifier);
                            ex.setProperty(TENANT_ID, tenant.getName());
                            ex.setProperty(ZEEBE_JOB_KEY, job.getKey());
                            ex.setProperty(TRANSACTION_ROLE, TransactionRole.PAYER);
                            ex.setProperty(QUOTE_AMOUNT_TYPE, AmountType.SEND.name());
                            producerTemplate.send("direct:send-local-quote", ex);
                        } else {
                            Map<String, Object> variables = new HashMap<>();
                            variables.put(LOCAL_QUOTE_FAILED, false);
                            zeebeClient.newCompleteCommand(job.getKey())
                                    .variables(variables)
                                    .send()
                                    .join();
                        }
                    })
                    .name(WORKER_PAYER_LOCAL_QUOTE + dfspid)
                    .maxJobsActive(workerMaxJobs)
                    .open();

            logger.info("## generating " + WORKER_PAYEE_QUOTE + "{} worker", dfspid);
            zeebeClient.newWorker()
                    .jobType(WORKER_PAYEE_QUOTE + dfspid)
                    .handler((client, job) -> {
                        logger.info("Job '{}' started from process '{}' with key {}", job.getType(), job.getBpmnProcessId(), job.getKey());
                        Map<String, Object> existingVariables = job.getVariablesAsMap();
                        QuoteSwitchRequestDTO quoteRequest = objectMapper.readValue((String) existingVariables.get(QUOTE_SWITCH_REQUEST), QuoteSwitchRequestDTO.class);

                        if (isAmsLocalEnabled) {
                            String tenantId = tenantProperties.getTenant(quoteRequest.getPayee().getPartyIdInfo().getPartyIdType().name(),
                                    quoteRequest.getPayee().getPartyIdInfo().getPartyIdentifier()).getName();

                            TransactionChannelRequestDTO channelRequest = new TransactionChannelRequestDTO();
                            TransactionType transactionType = new TransactionType();
                            transactionType.setInitiator(TransactionRole.PAYEE);
                            transactionType.setInitiatorType(InitiatorType.CONSUMER);
                            transactionType.setScenario(Scenario.DEPOSIT);
                            channelRequest.setTransactionType(transactionType);
                            channelRequest.setAmountType(AmountType.RECEIVE);
                            MoneyData amount = new MoneyData(quoteRequest.getAmount().getAmount(),
                                    quoteRequest.getAmount().getCurrency());
                            channelRequest.setAmount(amount);

                            Exchange ex = new DefaultExchange(camelContext);
                            ex.setProperty(PARTY_ID, quoteRequest.getPayee().getPartyIdInfo().getPartyIdentifier());
                            ex.setProperty(PARTY_ID_TYPE, quoteRequest.getPayee().getPartyIdInfo().getPartyIdType());
                            ex.setProperty(TRANSACTION_ID, existingVariables.get(TRANSACTION_ID));
                            ex.setProperty(TENANT_ID, tenantId);
                            ex.setProperty(TRANSACTION_ROLE, TransactionRole.PAYEE.name());
                            ex.setProperty(CHANNEL_REQUEST, objectMapper.writeValueAsString(channelRequest));
                            ex.setProperty(ZEEBE_JOB_KEY, job.getKey());
                            ex.setProperty(QUOTE_AMOUNT_TYPE, quoteRequest.getAmountType().name());
                            producerTemplate.send("direct:send-local-quote", ex);
                        } else {
                            Map<String, Object> variables = createFreeQuote(quoteRequest.getAmount().getCurrency());
                            variables.put(QUOTE_FAILED, false);
                            zeebeClient.newCompleteCommand(job.getKey())
                                    .variables(variables)
                                    .send()
                                    .join();
                        }
                    })
                    .name(WORKER_PAYEE_QUOTE + dfspid)
                    .maxJobsActive(workerMaxJobs)
                    .open();

            logger.info("## generating " + WORKER_PAYEE_COMMIT_TRANSFER + "{} worker", dfspid);
            zeebeClient.newWorker()
                    .jobType(WORKER_PAYEE_COMMIT_TRANSFER + dfspid)
                    .handler((client, job) -> {
                        logger.info("Job '{}' started from process '{}' with key {}", job.getType(), job.getBpmnProcessId(), job.getKey());
                        if (isAmsLocalEnabled) {
                            Exchange ex = new DefaultExchange(camelContext);
                            Map<String, Object> variables = job.getVariablesAsMap();
                            zeebeVariablesToCamelProperties(variables, ex,
                                    TRANSACTION_ID,
                                    TENANT_ID,
                                    EXTERNAL_ACCOUNT_ID,
                                    LOCAL_QUOTE_RESPONSE);
                            ex.setProperty(TRANSFER_ACTION, CREATE.name());
                            ex.setProperty(ZEEBE_JOB_KEY, job.getKey());

                            QuoteSwitchRequestDTO quoteRequest = objectMapper.readValue((String) variables.get(QUOTE_SWITCH_REQUEST), QuoteSwitchRequestDTO.class);

                            TransactionChannelRequestDTO transactionRequest = new TransactionChannelRequestDTO();
                            TransactionType transactionType = new TransactionType();
                            transactionType.setInitiator(TransactionRole.PAYEE);
                            transactionType.setInitiatorType(InitiatorType.CONSUMER);
                            transactionType.setScenario(Scenario.DEPOSIT);
                            transactionRequest.setTransactionType(transactionType);

                            MoneyData amount = new MoneyData(quoteRequest.getAmount().getAmountDecimal(),
                                    quoteRequest.getAmount().getCurrency());
                            transactionRequest.setAmount(amount);
                            ex.setProperty(CHANNEL_REQUEST, objectMapper.writeValueAsString(transactionRequest));
                            ex.setProperty(TRANSACTION_ROLE, TransactionRole.PAYEE.name());

                            producerTemplate.send("direct:send-transfers", ex);
                        } else {
                            zeebeClient.newCompleteCommand(job.getKey())
                                    .send()
                                    .join();
                        }
                    })
                    .name(WORKER_PAYEE_COMMIT_TRANSFER + dfspid)
                    .maxJobsActive(workerMaxJobs)
                    .open();

            logger.info("## generating " + WORKER_PARTY_LOOKUP_LOCAL + "{} worker", dfspid);
            zeebeClient.newWorker()
                    .jobType(WORKER_PARTY_LOOKUP_LOCAL + dfspid)
                    .handler((client, job) -> {
                        logger.info("Job '{}' started from process '{}' with key {}", job.getType(), job.getBpmnProcessId(), job.getKey());
                        Map<String, Object> existingVariables = job.getVariablesAsMap();
                        String partyIdType = (String) existingVariables.get(PARTY_ID_TYPE);
                        String partyId = (String) existingVariables.get(PARTY_ID);

                        if (isAmsLocalEnabled) {
                            Exchange ex = new DefaultExchange(camelContext);
                            ex.setProperty(PARTY_ID_TYPE, partyIdType);
                            ex.setProperty(PARTY_ID, partyId);
                            ex.setProperty(TENANT_ID, existingVariables.get(TENANT_ID));
                            ex.setProperty(ZEEBE_JOB_KEY, job.getKey());
                            producerTemplate.send("direct:get-party", ex);
                        } else {
                            Map<String, Object> variables = new HashMap<>();
                            Party party = new Party( // only return fspId from configuration
                                    new PartyIdInfo(IdentifierType.valueOf(partyIdType),
                                            partyId,
                                            null,
                                            tenantProperties.getTenant(partyIdType, partyId).getFspId()),
                                    null,
                                    null,
                                    null);

                            variables.put(PAYEE_PARTY_RESPONSE, objectMapper.writeValueAsString(party));
                            client.newCompleteCommand(job.getKey())
                                    .variables(variables)
                                    .send()
                                    .join();
                        }
                    })
                    .name(WORKER_PARTY_LOOKUP_LOCAL + dfspid)
                    .maxJobsActive(workerMaxJobs)
                    .open();
        }
    }

    private Map<String, Object> createFreeQuote(String currency) throws Exception {
        QuoteFspResponseDTO response = new QuoteFspResponseDTO();
        response.setFspFee(new FspMoneyData(BigDecimal.ZERO, currency));
        response.setFspCommission(new FspMoneyData(BigDecimal.ZERO, currency));

        Map<String, Object> variables = new HashMap<>();
        variables.put(LOCAL_QUOTE_RESPONSE, objectMapper.writeValueAsString(response));
        return variables;
    }
}