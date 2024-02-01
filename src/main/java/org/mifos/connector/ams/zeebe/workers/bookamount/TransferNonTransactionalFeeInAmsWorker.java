package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.mifos.connector.ams.fineract.Config;
import org.mifos.connector.ams.fineract.ConfigFactory;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.log.LogInternalCorrelationId;
import org.mifos.connector.ams.log.TraceZeebeArguments;
import org.mifos.connector.ams.mapstruct.Pain001Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.BatchItemBuilder;
import org.mifos.connector.ams.zeebe.workers.utils.ContactDetailsUtil;
import org.mifos.connector.ams.zeebe.workers.utils.DtSavingsTransactionDetails;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionBody;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.baasflow.commons.events.Event;
import com.baasflow.commons.events.EventService;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import iso.std.iso._20022.tech.json.camt_053_001.ActiveOrHistoricCurrencyAndAmountRange2.CreditDebitCode;
import iso.std.iso._20022.tech.json.camt_053_001.BankToCustomerStatementV08;
import iso.std.iso._20022.tech.json.camt_053_001.EntryStatus1Choice;
import iso.std.iso._20022.tech.json.camt_053_001.ReportEntry10;
import iso.std.iso._20022.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
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
    private ContactDetailsUtil contactDetailsUtil;

    @Autowired
    private EventService eventService;

    @Autowired
    @Qualifier("painMapper")
    private ObjectMapper painMapper;

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
        log.info("transferNonTransactionalFeeInAms");
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
        log.debug("Got payment scheme {}", paymentScheme);
        String transactionDate = LocalDate.now().format(PATTERN);
        log.debug("Got category purpose code {}", categoryPurpose);

        try {
            MDC.put("internalCorrelationId", internalCorrelationId);
            Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = painMapper.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);

            String withdrawNonTxFeeDisposalOperation = "transferToConversionAccountInAms.DisposalAccount.WithdrawNonTransactionalFee";
            String withdrawNonTxDisposalConfigOperationKey = String.format("%s.%s.%s", paymentScheme, categoryPurpose, withdrawNonTxFeeDisposalOperation);
            Integer paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawNonTxDisposalConfigOperationKey);
            String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawNonTxDisposalConfigOperationKey);
            log.debug("Looking up {}, got payment type id {}", withdrawNonTxDisposalConfigOperationKey, paymentTypeId);
            TransactionBody body = new TransactionBody(
                    transactionDate,
                    amount,
                    paymentTypeId,
                    "",
                    FORMAT,
                    locale);

            String bodyItem = painMapper.writeValueAsString(body);

            List<TransactionItem> items = new ArrayList<>();

            batchItemBuilder.add(tenantIdentifier, items, disposalAccountWithdrawRelativeUrl, bodyItem, false);

            BankToCustomerStatementV08 convertedStatement = camt053Mapper.toCamt053Entry(pain001.getDocument());
            ReportEntry10 convertedcamt053 = convertedStatement.getStatement().get(0).getEntry().get(0);
            convertedcamt053.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
            convertedcamt053.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedcamt053.setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedcamt053.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            String camt053Entry = painMapper.writeValueAsString(convertedcamt053);

            String camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";

            DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
                    internalCorrelationId,
                    camt053Entry,
                    pain001.getDocument().getPaymentInformation().get(0).getDebtorAccount().getIdentification().getIban(),
                    paymentTypeCode,
                    transactionGroupId,
                    pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getName(),
                    pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditorAccount().getIdentification().getIban(),
                    null,
                    contactDetailsUtil.getId(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getContactDetails()),
                    Optional.ofNullable(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getRemittanceInformation())
                            .map(iso.std.iso._20022.tech.json.pain_001_001.RemittanceInformation16::getUnstructured).map(List::toString).orElse(""),
                    categoryPurpose,
                    paymentScheme,
                    disposalAccountAmsId,
                    null,
                    pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getPaymentIdentification().getEndToEndIdentification());

            String camt053Body = painMapper.writeValueAsString(td);

            batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);


            String depositNonTxFeeOperation = "transferToConversionAccountInAms.ConversionAccount.DepositNonTransactionalFee";
            String depositNonTxFeeConfigOperationKey = String.format("%s.%s.%s", paymentScheme, categoryPurpose, depositNonTxFeeOperation);
            paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(depositNonTxFeeConfigOperationKey);
            paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(depositNonTxFeeConfigOperationKey);
            convertedcamt053.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
            convertedcamt053.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedcamt053.setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedcamt053.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            camt053Entry = painMapper.writeValueAsString(convertedcamt053);
            log.debug("Looking up {}, got payment type id {}", depositNonTxFeeConfigOperationKey, paymentTypeId);
            body = new TransactionBody(
                    transactionDate,
                    amount,
                    paymentTypeId,
                    "",
                    FORMAT,
                    locale);

            bodyItem = painMapper.writeValueAsString(body);

            String conversionAccountDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "deposit");

            batchItemBuilder.add(tenantIdentifier, items, conversionAccountDepositRelativeUrl, bodyItem, false);

            td = new DtSavingsTransactionDetails(
                    internalCorrelationId,
                    camt053Entry,
                    pain001.getDocument().getPaymentInformation().get(0).getDebtorAccount().getIdentification().getIban(),
                    paymentTypeCode,
                    transactionGroupId,
                    pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getName(),
                    pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditorAccount().getIdentification().getIban(),
                    null,
                    contactDetailsUtil.getId(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getContactDetails()),
                    Optional.ofNullable(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getRemittanceInformation())
                            .map(iso.std.iso._20022.tech.json.pain_001_001.RemittanceInformation16::getUnstructured).map(List::toString).orElse(""),
                    categoryPurpose,
                    paymentScheme,
                    disposalAccountAmsId,
                    null,
                    pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getPaymentIdentification().getEndToEndIdentification());

            camt053Body = painMapper.writeValueAsString(td);

            batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);


            String withdrawNonTxFeeConversionOperation = "transferToConversionAccountInAms.ConversionAccount.WithdrawNonTransactionalFee";
            String withdrawNonTxFeeConversionConfigOperationKey = String.format("%s.%s.%s", paymentScheme, categoryPurpose, withdrawNonTxFeeConversionOperation);
            paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawNonTxFeeConversionConfigOperationKey);
            paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawNonTxFeeConversionConfigOperationKey);
            convertedcamt053.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
            convertedcamt053.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedcamt053.setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedcamt053.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            camt053Entry = painMapper.writeValueAsString(convertedcamt053);
            log.debug("Looking up {}, got payment type id {}", withdrawNonTxFeeConversionConfigOperationKey, paymentTypeId);
            body = new TransactionBody(
                    transactionDate,
                    amount,
                    paymentTypeId,
                    "",
                    FORMAT,
                    locale);

            bodyItem = painMapper.writeValueAsString(body);

            String conversionAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");

            batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, bodyItem, false);

            td = new DtSavingsTransactionDetails(
                    internalCorrelationId,
                    camt053Entry,
                    pain001.getDocument().getPaymentInformation().get(0).getDebtorAccount().getIdentification().getIban(),
                    paymentTypeCode,
                    transactionGroupId,
                    pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getName(),
                    pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditorAccount().getIdentification().getIban(),
                    null,
                    contactDetailsUtil.getId(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getContactDetails()),
                    Optional.ofNullable(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getRemittanceInformation())
                            .map(iso.std.iso._20022.tech.json.pain_001_001.RemittanceInformation16::getUnstructured).map(List::toString).orElse(""),
                    categoryPurpose,
                    paymentScheme,
                    disposalAccountAmsId,
                    null,
                    pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getPaymentIdentification().getEndToEndIdentification());

            camt053Body = painMapper.writeValueAsString(td);

            batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);

            log.debug("Attempting to send {}", painMapper.writeValueAsString(items));

            doBatch(items,
                    tenantIdentifier,
                    disposalAccountAmsId,
                    conversionAccountAmsId,
                    internalCorrelationId,
                    "transferNonTransactionalFeeInAms");

            return Map.of("transactionDate", transactionDate);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            MDC.remove("internalCorrelationId");
        }
    }
}