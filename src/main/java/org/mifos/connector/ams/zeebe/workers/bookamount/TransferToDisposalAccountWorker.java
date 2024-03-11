package org.mifos.connector.ams.zeebe.workers.bookamount;

import com.baasflow.commons.events.EventService;
import com.fasterxml.jackson.databind.ObjectMapper;
import hu.dpc.rt.utils.converter.Pacs004ToCamt053Converter;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import iso.std.iso._20022.tech.json.camt_053_001.AccountStatement9;
import iso.std.iso._20022.tech.json.camt_053_001.ActiveOrHistoricCurrencyAndAmountRange2.CreditDebitCode;
import iso.std.iso._20022.tech.json.camt_053_001.BankToCustomerStatementV08;
import iso.std.iso._20022.tech.json.camt_053_001.EntryDetails9;
import iso.std.iso._20022.tech.json.camt_053_001.EntryStatus1Choice;
import iso.std.iso._20022.tech.json.camt_053_001.EntryTransaction10;
import iso.std.iso._20022.tech.json.camt_053_001.ReportEntry10;
import iso.std.iso._20022.tech.xsd.pacs_008_001.CashAccount16;
import iso.std.iso._20022.tech.xsd.pacs_008_001.CreditTransferTransactionInformation11;
import iso.std.iso._20022.tech.xsd.pacs_008_001.RemittanceInformation5;
import lombok.extern.slf4j.Slf4j;
import org.mifos.connector.ams.common.SerializationHelper;
import org.mifos.connector.ams.fineract.TenantConfigs;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.log.LogInternalCorrelationId;
import org.mifos.connector.ams.log.TraceZeebeArguments;
import org.mifos.connector.ams.mapstruct.Pacs008Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.BatchItemBuilder;
import org.mifos.connector.ams.zeebe.workers.utils.ContactDetailsUtil;
import org.mifos.connector.ams.zeebe.workers.utils.CurrentAccountTransactionBody;
import org.mifos.connector.ams.zeebe.workers.utils.DtSavingsTransactionDetails;
import org.mifos.connector.ams.zeebe.workers.utils.JAXBUtils;
import org.mifos.connector.ams.zeebe.workers.utils.NotificationHelper;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionBody;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.mifos.connector.ams.zeebe.workers.bookamount.MoneyInOutWorker.FORMAT;

@Component
@Slf4j
public class TransferToDisposalAccountWorker {

    @Autowired
    private Pacs008Camt053Mapper pacs008Camt053Mapper;

    private Pacs004ToCamt053Converter pacs004Camt053Mapper = new Pacs004ToCamt053Converter();

    @Value("${fineract.incoming-money-api}")
    protected String incomingMoneyApi;

    @Value("${fineract.current-account-api}")
    protected String currentAccountApi;

    @Autowired
    private TenantConfigs tenantConfigs;

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

    @Autowired
    private MoneyInOutWorker moneyInOutWorker;

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
                                          @Variable String transactionId,
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
                        transactionId,
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
                                           String transactionId,
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
            String apiPath = accountProductType.equalsIgnoreCase("SAVINGS") ? incomingMoneyApi.substring(1) : currentAccountApi.substring(1);
            String disposalAccountDepositRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, disposalAccountAmsId, "deposit");
            String depositAmountOperation = "transferToDisposalAccount.DisposalAccount.DepositTransactionAmount";
            String depositAmountConfigOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
            String conversionAccountWithdrawRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, conversionAccountAmsId, "withdrawal");
            String paymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, depositAmountConfigOperationKey);
            String paymentTypeCode = tenantConfigs.findResourceCode(tenantIdentifier, depositAmountConfigOperationKey);
            String direction = tenantConfigs.findDirection(tenantIdentifier, depositAmountConfigOperationKey);
            iso.std.iso._20022.tech.xsd.pacs_008_001.Document pacs008 = jaxbUtils.unmarshalPacs008(originalPacs008);
            ReportEntry10 convertedCamt053Entry = pacs008Camt053Mapper.toCamt053Entry(pacs008).getStatement().get(0).getEntry().get(0);
            EntryTransaction10 entryTransaction10 = convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0);
            batchItemBuilder.setAmount(entryTransaction10, amount, currency);
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
                var bodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, paymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, disposalAccountDepositRelativeUrl, bodyItem, false);
            } // CURRENT account sends a single call only at the details step

            // STEP 2 - batch: deposit details
            entryTransaction10.setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            entryTransaction10.setAdditionalTransactionInformation(paymentTypeCode);
            String camt053Entry = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                var camt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, camt053Entry, creditorIban, paymentTypeCode, transactionGroupId, debtorName, debtorIban, null, debtorContactDetails, unstructured, transactionCategoryPurposeCode, paymentScheme, conversionAccountAmsId, disposalAccountAmsId, endToEndId));
                String camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, camt053RelativeUrl, camt053Body, true);
            } else {
                var camt053Body = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, moneyInOutWorker.getLocale(), paymentTypeId, currency, List.of(new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(creditorIban,
                        camt053Entry,
                        internalCorrelationId,
                        debtorName,
                        debtorIban,
                        transactionGroupId,
                        transactionId,
                        endToEndId,
                        transactionCategoryPurposeCode,
                        paymentScheme,
                        unstructured,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        null,
                        partnerAccountSecondaryIdentifier,
                        null,
                        valueDated,
                        direction)), "dt_current_transaction_details"))));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, disposalAccountDepositRelativeUrl, camt053Body, false);
            }

            // STEP 3 - batch: withdraw amount
            String withdrawAmountOperation = "transferToDisposalAccount.ConversionAccount.WithdrawTransactionAmount";
            String withdrawAmountConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
            paymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, withdrawAmountConfigOperationKey);
            paymentTypeCode = tenantConfigs.findResourceCode(tenantIdentifier, withdrawAmountConfigOperationKey);
            entryTransaction10.setAdditionalTransactionInformation(paymentTypeCode);
            entryTransaction10.setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
            camt053Entry = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                var bodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, paymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, conversionAccountWithdrawRelativeUrl, bodyItem, false);
            } // CURRENT account sends a single call only at the details step

            // STEP 4 - batch: withdraw details
            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                var camt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, camt053Entry, creditorIban, paymentTypeCode, transactionGroupId, debtorName, debtorIban, null, debtorContactDetails, unstructured, transactionCategoryPurposeCode, paymentScheme, conversionAccountAmsId, disposalAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, conversionAccountWithdrawRelativeUrl, camt053Body, true);
            } else {
                var camt053Body = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, moneyInOutWorker.getLocale(), paymentTypeId, currency, List.of(new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                                creditorIban,
                                camt053Entry,
                                internalCorrelationId,
                                debtorName,
                                debtorIban,
                                transactionGroupId,
                                transactionId,
                                endToEndId,
                                transactionCategoryPurposeCode,
                                paymentScheme,
                                unstructured,
                                conversionAccountAmsId,
                                disposalAccountAmsId,
                                null,
                                partnerAccountSecondaryIdentifier,
                                null,
                                valueDated,
                                direction)
                        ), "dt_current_transaction_details")))
                );
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, conversionAccountWithdrawRelativeUrl, camt053Body, false);
            }

            moneyInOutWorker.doBatch(items, tenantIdentifier, transactionGroupId, disposalAccountAmsId, conversionAccountAmsId, internalCorrelationId, "transferToDisposalAccount");
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
                                                  @Variable String transactionId,
                                                  @Variable String transactionCategoryPurposeCode,
                                                  @Variable BigDecimal amount,
                                                  @Variable String currency,
                                                  @Variable String conversionAccountAmsId,
                                                  @Variable String disposalAccountAmsId,
                                                  @Variable String tenantIdentifier,
                                                  @Variable String pacs004,
                                                  @Variable String creditorIban,
                                                  @Variable String accountProductType,
                                                  @Variable String valueDated
    ) {
        log.info("transferToDisposalAccountInRecall");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "transferToDisposalAccountInRecall", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> transferToDisposalAccountInRecall(originalPacs008,
                        internalCorrelationId,
                        paymentScheme,
                        transactionDate,
                        transactionGroupId,
                        transactionId,
                        transactionCategoryPurposeCode,
                        amount,
                        currency,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        tenantIdentifier,
                        pacs004,
                        creditorIban,
                        accountProductType,
                        Boolean.parseBoolean(Optional.ofNullable(valueDated).orElse("false"))));
    }

    private Void transferToDisposalAccountInRecall(String originalPacs008,
                                                   String internalCorrelationId,
                                                   String paymentScheme,
                                                   String transactionDate,
                                                   String transactionGroupId,
                                                   String transactionId,
                                                   String transactionCategoryPurposeCode,
                                                   BigDecimal amount,
                                                   String currency,
                                                   String conversionAccountAmsId,
                                                   String disposalAccountAmsId,
                                                   String tenantIdentifier,
                                                   String originalPacs004,
                                                   String creditorIban,
                                                   String accountProductType,
                                                   boolean valueDated) {
        try {
            // STEP 0 - prepare
            MDC.put("internalCorrelationId", internalCorrelationId);
            log.info("transfer to disposal account in recall (pacs.004) {} started for {} on {} ", internalCorrelationId, paymentScheme, tenantIdentifier);
            String apiPath = accountProductType.equalsIgnoreCase("SAVINGS") ? incomingMoneyApi.substring(1) : currentAccountApi.substring(1);
            iso.std.iso._20022.tech.xsd.pacs_008_001.Document pacs008 = jaxbUtils.unmarshalPacs008(originalPacs008);
            iso.std.iso._20022.tech.xsd.pacs_004_001.Document pacs004 = jaxbUtils.unmarshalPacs004(originalPacs004);
            String disposalAccountDepositRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, disposalAccountAmsId, "deposit");
            String depositAmountOperation = "transferToDisposalAccountInRecall.DisposalAccount.DepositTransactionAmount";
            String depositAmountConfigOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
            String paymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, depositAmountConfigOperationKey);
            String paymentTypeCode = tenantConfigs.findResourceCode(tenantIdentifier, depositAmountConfigOperationKey);
            String direction = tenantConfigs.findDirection(tenantIdentifier, depositAmountConfigOperationKey);
            List<TransactionItem> items = new ArrayList<>();

            // STEP 1 - deposit transaction
            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                var bodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, paymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, disposalAccountDepositRelativeUrl, bodyItem, false);
            }

            BankToCustomerStatementV08 intermediateCamt053 = pacs008Camt053Mapper.toCamt053Entry(pacs008);
            ReportEntry10 convertedCamt053Entry = pacs004Camt053Mapper.convert(pacs004, intermediateCamt053).getStatement().get(0).getEntry().get(0);
            EntryTransaction10 entryTransaction10 = convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0);
            batchItemBuilder.setAmount(entryTransaction10, amount, currency);
            entryTransaction10.setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            entryTransaction10.setAdditionalTransactionInformation(paymentTypeCode);
            String camt053Entry = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);

            CashAccount16 debtorAccount = pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtrAcct();
            String debtorName = pacs004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getCdtr().getNm();

            String debtorIban = debtorAccount.getId().getIBAN();
            String creditorName = pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getCdtr().getNm();
            String debtorContactDetails = contactDetailsUtil.getId(pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtr().getCtctDtls());
            String unstructured = Optional.ofNullable(pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getRmtInf()).map(RemittanceInformation5::getUstrd).map(List::toString).orElse("");
            String endToEndId = pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getPmtId().getEndToEndId();

            // STEP 2 - deposit details
            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                var camt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, camt053Entry, debtorIban, paymentTypeCode, transactionGroupId, creditorName, creditorIban, null, debtorContactDetails, unstructured, transactionCategoryPurposeCode, paymentScheme, conversionAccountAmsId, disposalAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, "datatables/dt_savings_transaction_details/$.resourceId", camt053Body, true);
            } else {
                var camt053Body = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, moneyInOutWorker.getLocale(), paymentTypeId, currency, List.of(new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                        debtorIban,
                        camt053Entry,
                        internalCorrelationId,
                        creditorName,
                        creditorIban,
                        transactionGroupId,
                        transactionId,
                        endToEndId,
                        transactionCategoryPurposeCode,
                        paymentScheme,
                        unstructured,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        null,
                        debtorContactDetails,
                        null,
                        valueDated,
                        direction
                )), "dt_current_transaction_details"))));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, disposalAccountDepositRelativeUrl, camt053Body, false);
            }

            String conversionAccountWithdrawRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, conversionAccountAmsId, "withdrawal");
            String withdrawAmountOperation = "transferToDisposalAccountInRecall.ConversionAccount.WithdrawTransactionAmount";
            String withdrawAmountConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
            paymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, withdrawAmountConfigOperationKey);
            paymentTypeCode = tenantConfigs.findResourceCode(tenantIdentifier, withdrawAmountConfigOperationKey);
            entryTransaction10.setAdditionalTransactionInformation(paymentTypeCode);
            entryTransaction10.setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
            camt053Entry = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);

            // STEP 3 - withdraw
            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                var bodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, paymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, conversionAccountWithdrawRelativeUrl, bodyItem, false);

                var camt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, camt053Entry, debtorIban, paymentTypeCode, transactionGroupId, creditorName, creditorIban, null, debtorContactDetails, unstructured, transactionCategoryPurposeCode, paymentScheme, conversionAccountAmsId, disposalAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, "datatables/dt_savings_transaction_details/$.resourceId", camt053Body, true);
            } else {
                var bodyItem = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, moneyInOutWorker.getLocale(), paymentTypeId, currency, List.of(new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                        debtorIban,
                        camt053Entry,
                        internalCorrelationId,
                        creditorName,
                        creditorIban,
                        transactionGroupId,
                        transactionId,
                        endToEndId,
                        transactionCategoryPurposeCode,
                        paymentScheme,
                        unstructured,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        null,
                        debtorContactDetails,
                        null,
                        valueDated,
                        direction
                )), "dt_current_transaction_details"))));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, conversionAccountWithdrawRelativeUrl, bodyItem, false);
            }

            moneyInOutWorker.doBatch(items, tenantIdentifier, transactionGroupId, disposalAccountAmsId, conversionAccountAmsId, internalCorrelationId, "transferToDisposalAccountInRecall");
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
                                                  @Variable String transactionId,
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
        log.info("transferToDisposalAccountInReturn");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "transferToDisposalAccountInReturn", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> transferToDisposalAccountInReturn(pacs004,
                        internalCorrelationId,
                        paymentScheme,
                        transactionDate,
                        transactionGroupId,
                        transactionId,
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

    private Void transferToDisposalAccountInReturn(String pacs004,
                                                   String internalCorrelationId,
                                                   String paymentScheme,
                                                   String transactionDate,
                                                   String transactionGroupId,
                                                   String transactionId,
                                                   String transactionCategoryPurposeCode,
                                                   BigDecimal amount,
                                                   String currency,
                                                   String conversionAccountAmsId,
                                                   String disposalAccountAmsId,
                                                   String tenantIdentifier,
                                                   String creditorIban,
                                                   String accountProductType,
                                                   boolean valueDated
    ) {
        try {
            // STEP 0 - collect / extract information
            MDC.put("internalCorrelationId", internalCorrelationId);
            log.info("transfer to disposal account in return (pacs.004) {} started for {} on {} ", internalCorrelationId, paymentScheme, tenantIdentifier);
            String apiPath = accountProductType.equalsIgnoreCase("SAVINGS") ? incomingMoneyApi.substring(1) : currentAccountApi.substring(1);
            String disposalAccountDepositRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, disposalAccountAmsId, "deposit");
            String depositAmountOperation = "transferToDisposalAccountInReturn.DisposalAccount.DepositTransactionAmount";
            String depositAmountConfigOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
            var paymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, depositAmountConfigOperationKey);
            var paymentTypeCode = tenantConfigs.findResourceCode(tenantIdentifier, depositAmountConfigOperationKey);
            var direction = tenantConfigs.findDirection(tenantIdentifier, depositAmountConfigOperationKey);

            List<TransactionItem> items = new ArrayList<>();

            // STEP 1 - deposit transaction
            if (accountProductType.equals("SAVINGS")) {
                var bodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, paymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, disposalAccountDepositRelativeUrl, bodyItem, false);
            }

            // STEP 2 - deposit transaction details
            iso.std.iso._20022.tech.xsd.pacs_004_001.Document pacs_004 = jaxbUtils.unmarshalPacs004(pacs004);
            BankToCustomerStatementV08 camt053 = pacs004Camt053Mapper.convert(pacs_004,
                    new BankToCustomerStatementV08()
                            .withStatement(List.of(new AccountStatement9()
                                    .withEntry(List.of(new ReportEntry10()
                                            .withEntryDetails(List.of(new EntryDetails9()
                                                    .withTransactionDetails(List.of(new EntryTransaction10())))))))));
            ReportEntry10 convertedCamt053Entry = camt053.getStatement().get(0).getEntry().get(0);
            EntryTransaction10 entryTransaction10 = convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0);
            batchItemBuilder.setAmount(entryTransaction10, amount, currency);
            entryTransaction10.setAdditionalTransactionInformation(paymentTypeCode);
            entryTransaction10.setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            String camt053Entry = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);

            String creditorName = pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getCdtr().getNm();
            String debtorIban = pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getDbtrAcct().getId().getIBAN();
            String creditorContactDetails = contactDetailsUtil.getId(pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getCdtr().getCtctDtls());
            String unstructured = Optional.ofNullable(pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getRmtInf())
                    .map(iso.std.iso._20022.tech.xsd.pacs_004_001.RemittanceInformation5::getUstrd).map(List::toString).orElse("");
            String endToEndId = pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlEndToEndId();

            if (accountProductType.equals("SAVINGS")) {
                var camt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, camt053Entry, debtorIban, paymentTypeCode, transactionGroupId, creditorName, creditorIban, null, creditorContactDetails, unstructured, transactionCategoryPurposeCode, paymentScheme, conversionAccountAmsId, disposalAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, "datatables/dt_savings_transaction_details/$.resourceId", camt053Body, true);
            } else {
                var camt053Body = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, moneyInOutWorker.getLocale(), paymentTypeId, currency, List.of(new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(debtorIban,
                        camt053Entry,
                        internalCorrelationId,
                        creditorName,
                        creditorIban,
                        transactionGroupId,
                        transactionId,
                        endToEndId,
                        transactionCategoryPurposeCode,
                        paymentScheme,
                        unstructured,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        null,
                        creditorContactDetails,
                        null,
                        valueDated,
                        direction)), "dt_current_transaction_details"))));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, disposalAccountDepositRelativeUrl, camt053Body, false);
            }

            // STEP 3 - withdraw transaction
            String conversionAccountWithdrawRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, conversionAccountAmsId, "withdrawal");
            String withdrawAmountOperation = "transferToDisposalAccountInReturn.ConversionAccount.WithdrawTransactionAmount";
            String withdrawAmountConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
            paymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, withdrawAmountConfigOperationKey);
            paymentTypeCode = tenantConfigs.findResourceCode(tenantIdentifier, withdrawAmountConfigOperationKey);
            entryTransaction10.setAdditionalTransactionInformation(paymentTypeCode);
            entryTransaction10.setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
            camt053Entry = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);

            if (accountProductType.equals("SAVINGS")) {
                var bodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, paymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, conversionAccountWithdrawRelativeUrl, bodyItem, false);

                var camt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, camt053Entry, debtorIban, paymentTypeCode, transactionGroupId, creditorName, creditorIban, null, creditorContactDetails, unstructured, transactionCategoryPurposeCode, paymentScheme, conversionAccountAmsId, disposalAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, "datatables/dt_savings_transaction_details/$.resourceId", camt053Body, true);
            } else {
                var bodyItem = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, moneyInOutWorker.getLocale(), paymentTypeId, currency, List.of(new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                        debtorIban,
                        camt053Entry,
                        internalCorrelationId,
                        creditorName,
                        creditorIban,
                        transactionGroupId,
                        transactionId,
                        endToEndId,
                        transactionCategoryPurposeCode,
                        paymentScheme,
                        unstructured,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        null,
                        creditorContactDetails,
                        null,
                        valueDated,
                        direction
                )), "dt_current_transaction_details"))));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, conversionAccountWithdrawRelativeUrl, bodyItem, false);
            }

            moneyInOutWorker.doBatch(items, tenantIdentifier, transactionGroupId, disposalAccountAmsId, conversionAccountAmsId, internalCorrelationId, "transferToDisposalAccountInReturn");
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