package org.mifos.connector.ams.zeebe.workers.bookamount;

import com.baasflow.commons.events.Event;
import com.baasflow.commons.events.EventService;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import iso.std.iso._20022.tech.json.camt_053_001.ReportEntry10;
import iso.std.iso._20022.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;
import org.mifos.connector.ams.fineract.Config;
import org.mifos.connector.ams.fineract.ConfigFactory;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.log.LogInternalCorrelationId;
import org.mifos.connector.ams.log.TraceZeebeArguments;
import org.mifos.connector.ams.mapstruct.Pain001Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.BatchItemBuilder;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionBody;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionDetails;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class TransferNonTransactionalFeeInAmsWorker extends AbstractMoneyInOutWorker {

    @Autowired
    private Pain001Camt053Mapper camt053Mapper;

    @Value("${fineract.incoming-money-api}")
    protected String incomingMoneyApi;

    @Autowired
    private ConfigFactory paymentTypeConfigFactory;

    @Autowired
    private BatchItemBuilder batchItemBuilder;

    @Autowired
    private EventService eventService;

    private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);

    @JobWorker
    @LogInternalCorrelationId
    @TraceZeebeArguments
    public Map<String, Object> transferNonTransactionalFeeInAms(JobClient jobClient,
                                                                ActivatedJob activatedJob,
                                                                @Variable Integer conversionAccountAmsId,
                                                                @Variable Integer disposalAccountAmsId,
                                                                @Variable String tenantIdentifier,
                                                                @Variable String paymentScheme,
                                                                @Variable BigDecimal amount,
                                                                @Variable String internalCorrelationId,
                                                                @Variable String transactionGroupId,
                                                                @Variable String categoryPurpose,
                                                                @Variable String originalPain001,
                                                                @Variable String debtorIban) {
        logger.info("transferNonTransactionalFeeInAms");
        return eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "bookCreditedAmountToTechnicalAccount", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> transferNonTransactionalFeeInAms(conversionAccountAmsId,
                        disposalAccountAmsId,
                        tenantIdentifier,
                        paymentScheme,
                        amount,
                        internalCorrelationId,
                        transactionGroupId,
                        categoryPurpose,
                        originalPain001,
                        debtorIban,
                        eventBuilder));
    }

    private Map<String, Object> transferNonTransactionalFeeInAms(Integer conversionAccountAmsId,
                                                                 Integer disposalAccountAmsId,
                                                                 String tenantIdentifier,
                                                                 String paymentScheme,
                                                                 BigDecimal amount,
                                                                 String internalCorrelationId,
                                                                 String transactionGroupId,
                                                                 String categoryPurpose,
                                                                 String originalPain001,
                                                                 String debtorIban,
                                                                 Event.Builder eventBuilder) {
        String disposalAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "withdrawal");
        Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
        logger.debug("Got payment scheme {}", paymentScheme);
        String transactionDate = LocalDate.now().format(PATTERN);
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(Include.NON_NULL);
        batchItemBuilder.tenantId(tenantIdentifier);
        logger.debug("Got category purpose code {}", categoryPurpose);

        try {
            Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = objectMapper.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);

            Integer paymentTypeId = paymentTypeConfig.findByOperation(String.format("%s.%s.%s", paymentScheme, categoryPurpose, "transferToConversionAccountInAms.DisposalAccount.WithdrawNonTransactionalFee"));
            logger.debug("Looking up {}, got payment type id {}", String.format("%s.%s.%s", paymentScheme, categoryPurpose, "transferToConversionAccountInAms.DisposalAccount.WithdrawNonTransactionalFee"), paymentTypeId);
            TransactionBody body = new TransactionBody(
                    transactionDate,
                    amount,
                    paymentTypeId,
                    "",
                    FORMAT,
                    locale);

            String bodyItem = objectMapper.writeValueAsString(body);

            List<TransactionItem> items = new ArrayList<>();

            batchItemBuilder.add(items, disposalAccountWithdrawRelativeUrl, bodyItem, false);

            ReportEntry10 convertedcamt053 = camt053Mapper.toCamt053Entry(pain001.getDocument());
            String camt053Entry = objectMapper.writeValueAsString(convertedcamt053);

            String camt053RelativeUrl = "datatables/transaction_details/$.resourceId";

            TransactionDetails td = new TransactionDetails(
                    internalCorrelationId,
                    camt053Entry,
                    debtorIban,
                    transactionDate,
                    FORMAT,
                    locale,
                    transactionGroupId,
                    categoryPurpose);

            String camt053Body = objectMapper.writeValueAsString(td);

            batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);

            paymentTypeId = paymentTypeConfig.findByOperation(String.format("%s.%s.%s", paymentScheme, categoryPurpose, "transferToConversionAccountInAms.ConversionAccount.DepositNonTransactionalFee"));
            logger.debug("Looking up {}, got payment type id {}", String.format("%s.%s.%s", paymentScheme, categoryPurpose, "transferToConversionAccountInAms.ConversionAccount.DepositNonTransactionalFee"), paymentTypeId);
            body = new TransactionBody(
                    transactionDate,
                    amount,
                    paymentTypeId,
                    "",
                    FORMAT,
                    locale);

            bodyItem = objectMapper.writeValueAsString(body);

            String conversionAccountDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "deposit");

            batchItemBuilder.add(items, conversionAccountDepositRelativeUrl, bodyItem, false);

            td = new TransactionDetails(
                    internalCorrelationId,
                    camt053Entry,
                    debtorIban,
                    transactionDate,
                    FORMAT,
                    locale,
                    transactionGroupId,
                    categoryPurpose);

            camt053Body = objectMapper.writeValueAsString(td);

            batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);


            paymentTypeId = paymentTypeConfig.findByOperation(String.format("%s.%s.%s", paymentScheme, categoryPurpose, "transferToConversionAccountInAms.ConversionAccount.WithdrawNonTransactionalFee"));
            logger.debug("Looking up {}, got payment type id {}", String.format("%s.%s.%s", paymentScheme, categoryPurpose, "transferToConversionAccountInAms.ConversionAccount.WithdrawNonTransactionalFee"), paymentTypeId);
            body = new TransactionBody(
                    transactionDate,
                    amount,
                    paymentTypeId,
                    "",
                    FORMAT,
                    locale);

            bodyItem = objectMapper.writeValueAsString(body);

            String conversionAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");

            batchItemBuilder.add(items, conversionAccountWithdrawRelativeUrl, bodyItem, false);

            td = new TransactionDetails(
                    internalCorrelationId,
                    camt053Entry,
                    debtorIban,
                    transactionDate,
                    FORMAT,
                    locale,
                    transactionGroupId,
                    categoryPurpose);

            camt053Body = objectMapper.writeValueAsString(td);

            batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);

            logger.debug("Attempting to send {}", objectMapper.writeValueAsString(items));

            doBatch(items, tenantIdentifier, internalCorrelationId);

            return Map.of("transactionDate", transactionDate);
        } catch (Exception e) {
            // TODO technical error handling
            logger.error(e.getMessage(), e);
            throw new ZeebeBpmnError("Error_InsufficientFunds", e.getMessage());
        }
    }
}