package org.mifos.connector.ams.zeebe.workers.bookamount;

import com.baasflow.commons.events.EventService;
import com.fasterxml.jackson.databind.ObjectMapper;
import hu.dpc.rt.utils.converter.Pacs004ToCamt053Converter;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import iso.std.iso._20022.tech.json.camt_053_001.*;
import iso.std.iso._20022.tech.json.camt_053_001.ActiveOrHistoricCurrencyAndAmountRange2.CreditDebitCode;
import iso.std.iso._20022.tech.xsd.pacs_008_001.CashAccount16;
import iso.std.iso._20022.tech.xsd.pacs_008_001.CreditTransferTransactionInformation11;
import iso.std.iso._20022.tech.xsd.pacs_008_001.RemittanceInformation5;
import lombok.extern.slf4j.Slf4j;
import org.mifos.connector.ams.common.SerializationHelper;
import org.mifos.connector.ams.fineract.Config;
import org.mifos.connector.ams.fineract.ConfigFactory;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.log.LogInternalCorrelationId;
import org.mifos.connector.ams.log.TraceZeebeArguments;
import org.mifos.connector.ams.mapstruct.Pacs008Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.*;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Component
@Slf4j
public class TransferToDisposalAccountWorker extends AbstractMoneyInOutWorker {

    @Autowired
    private Pacs008Camt053Mapper pacs008Camt053Mapper;

    private Pacs004ToCamt053Converter pacs004Camt053Mapper = new Pacs004ToCamt053Converter();

    @Value("${fineract.incoming-money-api}")
    protected String incomingMoneyApi;

    @Value("${fineract.current-account-api}")
    protected String currentAccountApi;

    @Autowired
    private ConfigFactory paymentTypeConfigFactory;

    @Autowired
    private JAXBUtils jaxbUtils;

    @Autowired
    private BatchItemBuilder batchItemBuilder;

    @Autowired
    private ContactDetailsUtil contactDetailsUtil;

    @Autowired
    private EventService eventService;

    @Autowired
    private NotificationHelper notificationHelper;

    @Autowired
    private SerializationHelper serializationHelper;

    @Autowired
    @Qualifier("painMapper")
    private ObjectMapper painMapper;

    @JobWorker
    @LogInternalCorrelationId
    @TraceZeebeArguments
    public void transferToDisposalAccount(JobClient jobClient,
                                          ActivatedJob activatedJob,
                                          @Variable String originalPacs008,
                                          @Variable String internalCorrelationId,
                                          @Variable String paymentScheme,
                                          @Variable String transactionDate,
                                          @Variable String transactionGroupId,
                                          @Variable String transactionCategoryPurposeCode,
                                          @Variable BigDecimal amount,
                                          @Variable String currency,
                                          @Variable String conversionAccountAmsId,
                                          @Variable String disposalAccountAmsId,
                                          @Variable String tenantIdentifier,
                                          @Variable String creditorIban,
                                          @Variable String accountProductType,
                                          @Variable String valueDated
    ) {
        log.info("transferToDisposalAccount");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "transferToDisposalAccount", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> transferToDisposalAccount(originalPacs008,
                        internalCorrelationId,
                        paymentScheme,
                        transactionDate,
                        transactionGroupId,
                        transactionCategoryPurposeCode,
                        amount,
                        currency,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        tenantIdentifier,
                        creditorIban,
                        accountProductType,
                        Boolean.parseBoolean(Optional.ofNullable(valueDated).orElse("false"))
                ));
    }

    private Void transferToDisposalAccount(String originalPacs008,
                                           String internalCorrelationId,
                                           String paymentScheme,
                                           String transactionDate,
                                           String transactionGroupId,
                                           String transactionCategoryPurposeCode,
                                           BigDecimal amount,
                                           String currency,
                                           String conversionAccountAmsId,
                                           String disposalAccountAmsId,
                                           String tenantIdentifier,
                                           String creditorIban,
                                           String accountProductType,
                                           boolean valueDated) {
        try {
            // STEP 0 - collect / extract information
            MDC.put("internalCorrelationId", internalCorrelationId);
            log.info("transfer to disposal account in payment (pacs.008) {} started for {} on {} ", internalCorrelationId, paymentScheme, tenantIdentifier);
            Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
            String apiPath = accountProductType.equalsIgnoreCase("SAVINGS") ? incomingMoneyApi.substring(1) : currentAccountApi.substring(1);
            String disposalAccountDepositRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, disposalAccountAmsId, "deposit");
            String depositAmountOperation = "transferToDisposalAccount.DisposalAccount.DepositTransactionAmount";
            String depositAmountConfigOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
            String conversionAccountWithdrawRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, conversionAccountAmsId, "withdrawal");
            var paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(depositAmountConfigOperationKey);
            var paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(depositAmountConfigOperationKey);
            iso.std.iso._20022.tech.xsd.pacs_008_001.Document pacs008 = jaxbUtils.unmarshalPacs008(originalPacs008);
            ReportEntry10 convertedCamt053Entry = pacs008Camt053Mapper.toCamt053Entry(pacs008).getStatement().get(0).getEntry().get(0);
            EntryTransaction10 transactionDetails = convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0);
            CreditTransferTransactionInformation11 creditTransferTransaction = pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0);
            String debtorName = creditTransferTransaction.getDbtr().getNm();
            String debtorIban = creditTransferTransaction.getDbtrAcct().getId().getIBAN();
            String unstructured = Optional.ofNullable(creditTransferTransaction.getRmtInf()).map(RemittanceInformation5::getUstrd).map(List::toString).orElse("");
            String debtorContactDetails = contactDetailsUtil.getId(creditTransferTransaction.getDbtr().getCtctDtls());
            String endToEndId = creditTransferTransaction.getPmtId().getEndToEndId();
            String partnerAccountSecondaryIdentifier = contactDetailsUtil.getId(creditTransferTransaction.getDbtr().getCtctDtls());
            List<TransactionItem> items = new ArrayList<>();

            // STEP 1 - batch: deposit amount
            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                var bodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, paymentTypeId, "", FORMAT, locale));
                batchItemBuilder.add(tenantIdentifier, items, disposalAccountDepositRelativeUrl, bodyItem, false);
            } // CURRENT account sends a single call only at the details step

            // STEP 2 - batch: deposit details
            transactionDetails.setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            transactionDetails.setAdditionalTransactionInformation(paymentTypeCode);
            String camt053Entry = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                var camt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, camt053Entry, creditorIban, paymentTypeCode, transactionGroupId, debtorName, debtorIban, null, debtorContactDetails, unstructured, transactionCategoryPurposeCode, paymentScheme, conversionAccountAmsId, disposalAccountAmsId, endToEndId));
                String camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";
                batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);
            } else {
                var camt053Body = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, locale, paymentTypeId, currency, List.of(new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(creditorIban, camt053Entry, internalCorrelationId, debtorName, debtorIban, transactionGroupId, endToEndId, transactionCategoryPurposeCode, paymentScheme, unstructured, conversionAccountAmsId, disposalAccountAmsId, null, partnerAccountSecondaryIdentifier, null, valueDated)), "dt_current_transaction_details"))));
                batchItemBuilder.add(tenantIdentifier, items, disposalAccountDepositRelativeUrl, camt053Body, true);
            }

            // STEP 3 - batch: withdraw amount
            String withdrawAmountOperation = "transferToDisposalAccount.ConversionAccount.WithdrawTransactionAmount";
            String withdrawAmountConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
            paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawAmountConfigOperationKey);
            paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawAmountConfigOperationKey);
            transactionDetails.setAdditionalTransactionInformation(paymentTypeCode);
            transactionDetails.setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
            camt053Entry = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                var bodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, paymentTypeId, "", FORMAT, locale));
                batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, bodyItem, false);
            } // CURRENT account sends a single call only at the details step

            // STEP 4 - batch: withdraw details
            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                var camt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, camt053Entry, creditorIban, paymentTypeCode, transactionGroupId, debtorName, debtorIban, null, debtorContactDetails, unstructured, transactionCategoryPurposeCode, paymentScheme, conversionAccountAmsId, disposalAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, camt053Body, true);
            } else {
                var camt053Body = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, locale, paymentTypeId, currency, List.of(new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                                creditorIban,
                                camt053Entry,
                                internalCorrelationId,
                                debtorName,
                                debtorIban,
                                transactionGroupId,
                                endToEndId,
                                transactionCategoryPurposeCode,
                                paymentScheme,
                                unstructured,
                                conversionAccountAmsId,
                                disposalAccountAmsId,
                                null,
                                partnerAccountSecondaryIdentifier,
                                null,
                                valueDated)
                        ), "dt_current_transaction_details")))
                );
                batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, camt053Body, false);
            }

            doBatch(items, tenantIdentifier, transactionGroupId, disposalAccountAmsId, conversionAccountAmsId, internalCorrelationId, "transferToDisposalAccount");
            notificationHelper.send("transferToDisposalAccount", amount, currency, debtorName, paymentScheme, creditorIban);
            log.info("Exchange to disposal worker has finished successfully");

        } catch (Exception e) {
            log.error("Exchange to disposal worker has failed, dispatching user task to handle exchange", e);
            throw new ZeebeBpmnError("Error_TransferToDisposalToBeHandledManually", e.getMessage());
        } finally {
            MDC.remove("internalCorrelationId");
        }

        return null;
    }

    @JobWorker
    @LogInternalCorrelationId
    @TraceZeebeArguments
    public void transferToDisposalAccountInRecall(JobClient jobClient,
                                                  ActivatedJob activatedJob,
                                                  @Variable String originalPacs008,
                                                  @Variable String internalCorrelationId,
                                                  @Variable String paymentScheme,
                                                  @Variable String transactionDate,
                                                  @Variable String transactionGroupId,
                                                  @Variable String transactionCategoryPurposeCode,
                                                  @Variable BigDecimal amount,
                                                  @Variable String currency,
                                                  @Variable String conversionAccountAmsId,
                                                  @Variable String disposalAccountAmsId,
                                                  @Variable String tenantIdentifier,
                                                  @Variable String pacs004,
                                                  @Variable String creditorIban,
                                                  @Variable String accountProductType) {
        log.info("transferToDisposalAccountInRecall");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "transferToDisposalAccountInRecall", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> transferToDisposalAccountInRecall(originalPacs008,
                        internalCorrelationId,
                        paymentScheme,
                        transactionDate,
                        transactionGroupId,
                        transactionCategoryPurposeCode,
                        amount,
                        currency,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        tenantIdentifier,
                        pacs004,
                        creditorIban,
                        accountProductType));
    }

    private Void transferToDisposalAccountInRecall(String originalPacs008,
                                                   String internalCorrelationId,
                                                   String paymentScheme,
                                                   String transactionDate,
                                                   String transactionGroupId,
                                                   String transactionCategoryPurposeCode,
                                                   BigDecimal amount,
                                                   String currency,
                                                   String conversionAccountAmsId,
                                                   String disposalAccountAmsId,
                                                   String tenantIdentifier,
                                                   String originalPacs004,
                                                   String creditorIban,
                                                   String accountProductType) {
        try {
            MDC.put("internalCorrelationId", internalCorrelationId);
            log.info("transfer to disposal account in recall (pacs.004) {} started for {} on {} ", internalCorrelationId, paymentScheme, tenantIdentifier);

            iso.std.iso._20022.tech.xsd.pacs_008_001.Document pacs008 = jaxbUtils.unmarshalPacs008(originalPacs008);

            iso.std.iso._20022.tech.xsd.pacs_004_001.Document pacs004 = jaxbUtils.unmarshalPacs004(originalPacs004);

            Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);

            String disposalAccountDepositRelativeUrl = String.format("%s%s/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "deposit");
            String depositAmountOperation = "transferToDisposalAccountInRecall.DisposalAccount.DepositTransactionAmount";
            String depositAmountConfigOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
            var paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(depositAmountConfigOperationKey);
            var paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(depositAmountConfigOperationKey);

            var body = new TransactionBody(
                    transactionDate,
                    amount,
                    paymentTypeId,
                    "",
                    FORMAT,
                    locale);

            var bodyItem = painMapper.writeValueAsString(body);

            List<TransactionItem> items = new ArrayList<>();

            batchItemBuilder.add(tenantIdentifier, items, disposalAccountDepositRelativeUrl, bodyItem, false);

            BankToCustomerStatementV08 intermediateCamt053 = pacs008Camt053Mapper.toCamt053Entry(pacs008);
            ReportEntry10 convertedCamt053Entry = pacs004Camt053Mapper.convert(pacs004, intermediateCamt053).getStatement().get(0).getEntry().get(0);
            convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
            String camt053Entry = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);

            CashAccount16 debtorAccount = pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtrAcct();
            String debtorName = pacs004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getCdtr().getNm();

            CashAccount16 creditorAccount = pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getCdtrAcct();

            var td = new DtSavingsTransactionDetails(
                    internalCorrelationId,
                    camt053Entry,
                    debtorAccount.getId().getIBAN(),
                    paymentTypeCode,
                    transactionGroupId,
                    pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getCdtr().getNm(),
                    creditorAccount.getId().getIBAN(),
                    null,
                    contactDetailsUtil.getId(pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtr().getCtctDtls()),
                    Optional.ofNullable(pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getRmtInf()).map(RemittanceInformation5::getUstrd).map(List::toString).orElse(""),
                    transactionCategoryPurposeCode,
                    paymentScheme,
                    conversionAccountAmsId,
                    disposalAccountAmsId,
                    pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getPmtId().getEndToEndId());

            var camt053Body = painMapper.writeValueAsString(td);

            String camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";

            batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);

            String conversionAccountWithdrawRelativeUrl = String.format("%s%s/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");

            String withdrawAmountOperation = "transferToDisposalAccountInRecall.ConversionAccount.WithdrawTransactionAmount";
            String withdrawAmountConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
            paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawAmountConfigOperationKey);
            paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawAmountConfigOperationKey);
            convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
            convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
            camt053Entry = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);

            body = new TransactionBody(
                    transactionDate,
                    amount,
                    paymentTypeId,
                    "",
                    FORMAT,
                    locale);

            bodyItem = painMapper.writeValueAsString(body);

            batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, bodyItem, false);

            td = new DtSavingsTransactionDetails(
                    internalCorrelationId,
                    camt053Entry,
                    debtorAccount.getId().getIBAN(),
                    paymentTypeCode,
                    transactionGroupId,
                    pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getCdtr().getNm(),
                    creditorAccount.getId().getIBAN(),
                    null,
                    contactDetailsUtil.getId(pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtr().getCtctDtls()),
                    Optional.ofNullable(pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getRmtInf()).map(RemittanceInformation5::getUstrd).map(List::toString).orElse(""),
                    transactionCategoryPurposeCode,
                    paymentScheme,
                    conversionAccountAmsId,
                    disposalAccountAmsId,
                    pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getPmtId().getEndToEndId());

            camt053Body = painMapper.writeValueAsString(td);

            batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);

            doBatch(items,
                    tenantIdentifier,
                    transactionGroupId,
                    disposalAccountAmsId,
                    conversionAccountAmsId,
                    internalCorrelationId,
                    "transferToDisposalAccountInRecall");

            notificationHelper.send("transferToDisposalAccountInRecall", amount, currency, debtorName, paymentScheme, creditorIban);
            log.info("Exchange to disposal worker has finished successfully");
        } catch (Exception e) {
            log.error("Exchange to disposal worker has failed, dispatching user task to handle exchange", e);
            throw new ZeebeBpmnError("Error_TransferToDisposalToBeHandledManually", e.getMessage());
        } finally {
            MDC.remove("internalCorrelationId");
        }

        return null;
    }

    @JobWorker
    @LogInternalCorrelationId
    @TraceZeebeArguments
    public void transferToDisposalAccountInReturn(JobClient jobClient,
                                                  ActivatedJob activatedJob,
                                                  @Variable String pacs004,
                                                  @Variable String internalCorrelationId,
                                                  @Variable String paymentScheme,
                                                  @Variable String transactionDate,
                                                  @Variable String transactionGroupId,
                                                  @Variable String transactionCategoryPurposeCode,
                                                  @Variable BigDecimal amount,
                                                  @Variable String currency,
                                                  @Variable String conversionAccountAmsId,
                                                  @Variable String disposalAccountAmsId,
                                                  @Variable String tenantIdentifier,
                                                  @Variable String creditorIban,
                                                  @Variable String accountProductType) {
        log.info("transferToDisposalAccountInReturn");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "transferToDisposalAccountInReturn", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> transferToDisposalAccountInReturn(pacs004,
                        internalCorrelationId,
                        paymentScheme,
                        transactionDate,
                        transactionGroupId,
                        transactionCategoryPurposeCode,
                        amount,
                        currency,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        tenantIdentifier,
                        creditorIban,
                        accountProductType));
    }

    private Void transferToDisposalAccountInReturn(String pacs004,
                                                   String internalCorrelationId,
                                                   String paymentScheme,
                                                   String transactionDate,
                                                   String transactionGroupId,
                                                   String transactionCategoryPurposeCode,
                                                   BigDecimal amount,
                                                   String currency,
                                                   String conversionAccountAmsId,
                                                   String disposalAccountAmsId,
                                                   String tenantIdentifier,
                                                   String creditorIban,
                                                   String accountProductType) {
        try {
            MDC.put("internalCorrelationId", internalCorrelationId);
            log.info("transfer to disposal account in return (pacs.004) {} started for {} on {} ", internalCorrelationId, paymentScheme, tenantIdentifier);

            String disposalAccountDepositRelativeUrl = String.format("%s%s/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "deposit");
            String depositAmountOperation = "transferToDisposalAccountInReturn.DisposalAccount.DepositTransactionAmount";
            String depositAmountConfigOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
            Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
            var paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(depositAmountConfigOperationKey);
            var paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(depositAmountConfigOperationKey);

            var body = new TransactionBody(
                    transactionDate,
                    amount,
                    paymentTypeId,
                    "",
                    FORMAT,
                    locale);

            var bodyItem = painMapper.writeValueAsString(body);

            List<TransactionItem> items = new ArrayList<>();

            batchItemBuilder.add(tenantIdentifier, items, disposalAccountDepositRelativeUrl, bodyItem, false);

            var camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";

            iso.std.iso._20022.tech.xsd.pacs_004_001.Document pacs_004 = jaxbUtils.unmarshalPacs004(pacs004);
            BankToCustomerStatementV08 camt053 = pacs004Camt053Mapper.convert(pacs_004,
                    new BankToCustomerStatementV08()
                            .withStatement(List.of(new AccountStatement9()
                                    .withEntry(List.of(new ReportEntry10()
                                            .withEntryDetails(List.of(new EntryDetails9()
                                                    .withTransactionDetails(List.of(new EntryTransaction10())))))))));
            ReportEntry10 convertedCamt053Entry = camt053.getStatement().get(0).getEntry().get(0);
            convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
            convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));

            String camt053Entry = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);


            String creditorName = pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getCdtr().getNm();
            var td = new DtSavingsTransactionDetails(
                    internalCorrelationId,
                    camt053Entry,
                    pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getDbtrAcct().getId().getIBAN(),
                    paymentTypeCode,
                    transactionGroupId,
                    creditorName,
                    pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getCdtrAcct().getId().getIBAN(),
                    null,
                    contactDetailsUtil.getId(pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getCdtr().getCtctDtls()),
                    Optional.ofNullable(pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getRmtInf())
                            .map(iso.std.iso._20022.tech.xsd.pacs_004_001.RemittanceInformation5::getUstrd).map(List::toString).orElse(""),
                    transactionCategoryPurposeCode,
                    paymentScheme,
                    conversionAccountAmsId,
                    disposalAccountAmsId,
                    pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlEndToEndId());

            var camt053Body = painMapper.writeValueAsString(td);

            batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);

            String conversionAccountWithdrawRelativeUrl = String.format("%s%s/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");

            String withdrawAmountOperation = "transferToDisposalAccountInReturn.ConversionAccount.WithdrawTransactionAmount";
            String withdrawAmountConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
            paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawAmountConfigOperationKey);
            paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawAmountConfigOperationKey);

            convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
            convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
            camt053Entry = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);

            body = new TransactionBody(
                    transactionDate,
                    amount,
                    paymentTypeId,
                    "",
                    FORMAT,
                    locale);

            bodyItem = painMapper.writeValueAsString(body);

            batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, bodyItem, false);

            camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";

            td = new DtSavingsTransactionDetails(
                    internalCorrelationId,
                    camt053Entry,
                    pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getDbtrAcct().getId().getIBAN(),
                    paymentTypeCode,
                    transactionGroupId,
                    creditorName,
                    pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getCdtrAcct().getId().getIBAN(),
                    null,
                    contactDetailsUtil.getId(pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getCdtr().getCtctDtls()),
                    Optional.ofNullable(pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getRmtInf())
                            .map(iso.std.iso._20022.tech.xsd.pacs_004_001.RemittanceInformation5::getUstrd).map(List::toString).orElse(""),
                    transactionCategoryPurposeCode,
                    paymentScheme,
                    conversionAccountAmsId,
                    disposalAccountAmsId,
                    pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlEndToEndId());

            camt053Body = painMapper.writeValueAsString(td);

            batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);


            doBatch(items,
                    tenantIdentifier,
                    transactionGroupId,
                    disposalAccountAmsId,
                    conversionAccountAmsId,
                    internalCorrelationId,
                    "transferToDisposalAccountInReturn");

            notificationHelper.send("transferToDisposalAccountInReturn", amount, currency, creditorName, paymentScheme, creditorIban);

            log.info("Exchange to disposal worker has finished successfully");
        } catch (Exception e) {
            log.error("Exchange to disposal worker has failed, dispatching user task to handle exchange", e);
            throw new ZeebeBpmnError("Error_TransferToDisposalToBeHandledManually", e.getMessage());
        } finally {
            MDC.remove("internalCorrelationId");
        }

        return null;
    }
}