package org.mifos.connector.ams.zeebe.workers.bookamount;

import com.baasflow.commons.events.Event;
import com.baasflow.commons.events.EventService;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
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
public class OnUsTransferWorker extends AbstractMoneyInOutWorker {

    private static final String ERROR_FAILED_CREDIT_TRANSFER = "Error_FailedCreditTransfer";

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
    public Map<String, Object> transferTheAmountBetweenDisposalAccounts(JobClient jobClient,
                                                                        ActivatedJob activatedJob,
                                                                        @Variable String internalCorrelationId,
                                                                        @Variable String paymentScheme,
                                                                        @Variable String originalPain001,
                                                                        @Variable BigDecimal amount,
                                                                        @Variable Integer creditorDisposalAccountAmsId,
                                                                        @Variable Integer debtorDisposalAccountAmsId,
                                                                        @Variable Integer debtorConversionAccountAmsId,
                                                                        @Variable BigDecimal transactionFeeAmount,
                                                                        @Variable String tenantIdentifier,
                                                                        @Variable String transactionGroupId,
                                                                        @Variable String transactionCategoryPurposeCode,
                                                                        @Variable String transactionFeeCategoryPurposeCode,
                                                                        @Variable String transactionFeeInternalCorrelationId,
                                                                        @Variable String creditorIban,
                                                                        @Variable String debtorIban) {
        logger.info("transferTheAmountBetweenDisposalAccounts");
        return eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "transferTheAmountBetweenDisposalAccounts", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> transferTheAmountBetweenDisposalAccounts(internalCorrelationId,
                        paymentScheme,
                        originalPain001,
                        amount,
                        creditorDisposalAccountAmsId,
                        debtorDisposalAccountAmsId,
                        debtorConversionAccountAmsId,
                        transactionFeeAmount,
                        tenantIdentifier,
                        transactionGroupId,
                        transactionCategoryPurposeCode,
                        transactionFeeCategoryPurposeCode,
                        transactionFeeInternalCorrelationId,
                        creditorIban,
                        debtorIban,
                        eventBuilder));
    }

    private Map<String, Object> transferTheAmountBetweenDisposalAccounts(String internalCorrelationId,
                                                                         String paymentScheme,
                                                                         String originalPain001,
                                                                         BigDecimal amount,
                                                                         Integer creditorDisposalAccountAmsId,
                                                                         Integer debtorDisposalAccountAmsId,
                                                                         Integer debtorConversionAccountAmsId,
                                                                         BigDecimal transactionFeeAmount,
                                                                         String tenantIdentifier,
                                                                         String transactionGroupId,
                                                                         String transactionCategoryPurposeCode,
                                                                         String transactionFeeCategoryPurposeCode,
                                                                         String transactionFeeInternalCorrelationId,
                                                                         String creditorIban,
                                                                         String debtorIban,
                                                                         Event.Builder eventBuilder) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.setSerializationInclusion(Include.NON_NULL);
            Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = objectMapper.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);

            ReportEntry10 convertedcamt053Entry = camt053Mapper.toCamt053Entry(pain001.getDocument());
            String camt053Entry = objectMapper.writeValueAsString(convertedcamt053Entry);

            String interbankSettlementDate = LocalDate.now().format(PATTERN);

            batchItemBuilder.tenantId(tenantIdentifier);

            String debtorDisposalWithdrawalRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), debtorDisposalAccountAmsId, "withdrawal");

            Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
            Integer paymentTypeId = paymentTypeConfig.findByOperation(String.format("%s.%s", paymentScheme, "transferTheAmountBetweenDisposalAccounts.Debtor.DisposalAccount.WithdrawTransactionAmount"));

            TransactionBody body = new TransactionBody(
                    interbankSettlementDate,
                    amount,
                    paymentTypeId,
                    "",
                    FORMAT,
                    locale);

            String bodyItem = objectMapper.writeValueAsString(body);

            List<TransactionItem> items = new ArrayList<>();

            batchItemBuilder.add(items, debtorDisposalWithdrawalRelativeUrl, bodyItem, false);

            String camt053RelativeUrl = "datatables/transaction_details/$.resourceId";

            TransactionDetails td = new TransactionDetails(
                    internalCorrelationId,
                    camt053Entry,
                    debtorIban,
                    interbankSettlementDate,
                    FORMAT,
                    locale,
                    transactionGroupId,
                    transactionCategoryPurposeCode);

            String camt053Body = objectMapper.writeValueAsString(td);

            batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);


            if (!BigDecimal.ZERO.equals(transactionFeeAmount)) {
                paymentTypeId = paymentTypeConfig.findByOperation(String.format("%s.%s", paymentScheme, "transferTheAmountBetweenDisposalAccounts.Debtor.DisposalAccount.WithdrawTransactionFee"));

                body = new TransactionBody(
                        interbankSettlementDate,
                        transactionFeeAmount,
                        paymentTypeId,
                        "",
                        FORMAT,
                        locale);

                bodyItem = objectMapper.writeValueAsString(body);

                batchItemBuilder.add(items, debtorDisposalWithdrawalRelativeUrl, bodyItem, false);

                convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getSupplementaryData().get(0).getEnvelope().setAdditionalProperty("InternalCorrelationId", transactionFeeInternalCorrelationId);
                camt053Entry = objectMapper.writeValueAsString(convertedcamt053Entry);

                td = new TransactionDetails(
                        transactionFeeInternalCorrelationId,
                        camt053Entry,
                        debtorIban,
                        interbankSettlementDate,
                        FORMAT,
                        locale,
                        transactionGroupId,
                        transactionFeeCategoryPurposeCode);

                camt053Body = objectMapper.writeValueAsString(td);
                batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);


                paymentTypeId = paymentTypeConfig.findByOperation(String.format("%s.%s", paymentScheme, "transferTheAmountBetweenDisposalAccounts.Debtor.ConversionAccount.DepositTransactionFee"));

                body = new TransactionBody(
                        interbankSettlementDate,
                        transactionFeeAmount,
                        paymentTypeId,
                        "",
                        FORMAT,
                        locale);

                bodyItem = objectMapper.writeValueAsString(body);

                String debtorConversionDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), debtorConversionAccountAmsId, "deposit");

                batchItemBuilder.add(items, debtorConversionDepositRelativeUrl, bodyItem, false);

                td = new TransactionDetails(
                        transactionFeeInternalCorrelationId,
                        camt053Entry,
                        creditorIban,
                        interbankSettlementDate,
                        FORMAT,
                        locale,
                        transactionGroupId,
                        transactionFeeCategoryPurposeCode);

                camt053Body = objectMapper.writeValueAsString(td);
                batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
            }

            paymentTypeId = paymentTypeConfig.findByOperation(String.format("%s.%s", paymentScheme, "transferTheAmountBetweenDisposalAccounts.Creditor.DisposalAccount.DepositTransactionAmount"));

            body = new TransactionBody(
                    interbankSettlementDate,
                    amount,
                    paymentTypeId,
                    "",
                    FORMAT,
                    locale);

            bodyItem = objectMapper.writeValueAsString(body);

            String creditorDisposalDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), creditorDisposalAccountAmsId, "deposit");

            batchItemBuilder.add(items, creditorDisposalDepositRelativeUrl, bodyItem, false);

            convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getSupplementaryData().get(0).getEnvelope().setAdditionalProperty("InternalCorrelationId", internalCorrelationId);
            camt053Entry = objectMapper.writeValueAsString(convertedcamt053Entry);

            td = new TransactionDetails(
                    transactionFeeInternalCorrelationId,
                    camt053Entry,
                    creditorIban,
                    interbankSettlementDate,
                    FORMAT,
                    locale,
                    transactionGroupId,
                    transactionCategoryPurposeCode);

            camt053Body = objectMapper.writeValueAsString(td);
            batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);


            if (!BigDecimal.ZERO.equals(transactionFeeAmount)) {
                paymentTypeId = paymentTypeConfig.findByOperation(String.format("%s.%s", paymentScheme, "transferTheAmountBetweenDisposalAccounts.Debtor.ConversionAccount.WithdrawTransactionFee"));

                body = new TransactionBody(
                        interbankSettlementDate,
                        transactionFeeAmount,
                        paymentTypeId,
                        "",
                        FORMAT,
                        locale);

                bodyItem = objectMapper.writeValueAsString(body);

                String debtorConversionWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), debtorConversionAccountAmsId, "withdrawal");

                batchItemBuilder.add(items, debtorConversionWithdrawRelativeUrl, bodyItem, false);

                convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getSupplementaryData().get(0).getEnvelope().setAdditionalProperty("InternalCorrelationId", transactionFeeInternalCorrelationId);
                camt053Entry = objectMapper.writeValueAsString(convertedcamt053Entry);

                td = new TransactionDetails(
                        transactionFeeInternalCorrelationId,
                        camt053Entry,
                        debtorIban,
                        interbankSettlementDate,
                        FORMAT,
                        locale,
                        transactionGroupId,
                        transactionFeeCategoryPurposeCode);

                camt053Body = objectMapper.writeValueAsString(td);

                batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
            }

            doBatch(items, tenantIdentifier, internalCorrelationId);

            return Map.of("transactionDate", interbankSettlementDate);

        } catch (JsonProcessingException e) {
            // TODO technical error handling
            throw new RuntimeException("failed to create camt.053", e);
        }
    }
}