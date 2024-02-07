package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.mifos.connector.ams.common.SerializationHelper;
import org.mifos.connector.ams.fineract.Config;
import org.mifos.connector.ams.fineract.ConfigFactory;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.log.LogInternalCorrelationId;
import org.mifos.connector.ams.log.TraceZeebeArguments;
import org.mifos.connector.ams.mapstruct.Pacs008Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.BatchItemBuilder;
import org.mifos.connector.ams.zeebe.workers.utils.ContactDetailsUtil;
import org.mifos.connector.ams.zeebe.workers.utils.DtSavingsTransactionDetails;
import org.mifos.connector.ams.zeebe.workers.utils.JAXBUtils;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionBody;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

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
import iso.std.iso._20022.tech.xsd.pacs_008_001.RemittanceInformation5;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class BookCreditedAmountToConversionAccountWorker extends AbstractMoneyInOutWorker {

    @Autowired
    private Pacs008Camt053Mapper pacs008Camt053Mapper;

    private Pacs004ToCamt053Converter pacs004Camt053Mapper = new Pacs004ToCamt053Converter();

    @Value("${fineract.incoming-money-api}")
    protected String incomingMoneyApi;

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
    private SerializationHelper serializationHelper;

    @Autowired
    @Qualifier("painMapper")
    private ObjectMapper painMapper;

    @JobWorker
    @LogInternalCorrelationId
    @TraceZeebeArguments
    public void bookCreditedAmountToConversionAccount(JobClient jobClient,
                                                      ActivatedJob activatedJob,
                                                      @Variable String originalPacs008,
                                                      @Variable String transactionDate,
                                                      @Variable String transactionCategoryPurposeCode,
                                                      @Variable String transactionGroupId,
                                                      @Variable String internalCorrelationId,
                                                      @Variable String tenantIdentifier,
                                                      @Variable String paymentScheme,
                                                      @Variable BigDecimal amount,
                                                      @Variable String conversionAccountAmsId,
                                                      @Variable String accountProductType) {
        log.info("bookCreditedAmountToConversionAccount");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "bookCreditedAmountToConversionAccount", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> bookCreditedAmountToConversionAccount(originalPacs008,
                        transactionDate,
                        transactionCategoryPurposeCode,
                        transactionGroupId,
                        internalCorrelationId,
                        tenantIdentifier,
                        paymentScheme,
                        amount,
                        conversionAccountAmsId,
                        accountProductType));
    }

    private Void bookCreditedAmountToConversionAccount(String originalPacs008,
                                                       String transactionDate,
                                                       String transactionCategoryPurposeCode,
                                                       String transactionGroupId,
                                                       String internalCorrelationId,
                                                       String tenantIdentifier,
                                                       String paymentScheme,
                                                       BigDecimal amount,
                                                       String conversionAccountAmsId,
                                                       String accountProductType) {
        try {
            MDC.put("internalCorrelationId", internalCorrelationId);
            log.info("book to conversion account in payment (pacs.008) {} started for {} on {} ", internalCorrelationId, paymentScheme, tenantIdentifier);

            iso.std.iso._20022.tech.xsd.pacs_008_001.Document pacs008 = jaxbUtils.unmarshalPacs008(originalPacs008);

            String conversionAccountWithdrawalRelativeUrl = String.format("%s%s/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "deposit");

            Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
            String depositAmountOperation = "bookCreditedAmountToConversionAccount.ConversionAccount.DepositTransactionAmount";
            String configOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
            Integer paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(configOperationKey);
            String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(configOperationKey);

            TransactionBody body = new TransactionBody(
                    transactionDate,
                    amount,
                    paymentTypeId,
                    "",
                    FORMAT,
                    locale);

            String bodyItem = painMapper.writeValueAsString(body);

            List<TransactionItem> items = new ArrayList<>();

            batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawalRelativeUrl, bodyItem, false);

            ReportEntry10 convertedCamt053Entry = pacs008Camt053Mapper.toCamt053Entry(pacs008).getStatement().get(0).getEntry().get(0);
            convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
            String camt053Entry = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);

            String camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";

            DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
                    internalCorrelationId,
                    camt053Entry,
                    pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getCdtrAcct().getId().getIBAN(),
                    paymentTypeCode,
                    transactionGroupId,
                    pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtr().getNm(),
                    pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtrAcct().getId().getIBAN(),
                    null,
                    contactDetailsUtil.getId(pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtr().getCtctDtls()),
                    Optional.ofNullable(pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getRmtInf()).map(RemittanceInformation5::getUstrd).map(List::toString).orElse(""),
                    transactionCategoryPurposeCode,
                    paymentScheme,
                    null,
                    conversionAccountAmsId,
                    pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getPmtId().getEndToEndId());

            String camt053Body = painMapper.writeValueAsString(td);

            batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);

            doBatch(items,
                    tenantIdentifier,
                    "-1",
                    conversionAccountAmsId,
                    internalCorrelationId,
                    "bookCreditedAmountToConversionAccount");
        } catch (Exception e) {
            log.error("Worker to book incoming money in AMS has failed, dispatching user task to handle conversion account deposit", e);
            throw new ZeebeBpmnError("Error_BookToConversionToBeHandledManually", e.getMessage());
        } finally {
            MDC.remove("internalCorrelationId");
        }
        return null;
    }

    @JobWorker
    @LogInternalCorrelationId
    @TraceZeebeArguments
    public void bookCreditedAmountToConversionAccountInRecall(JobClient jobClient,
                                                              ActivatedJob activatedJob,
                                                              @Variable String originalPacs008,
                                                              @Variable String transactionDate,
                                                              @Variable String transactionCategoryPurposeCode,
                                                              @Variable String transactionGroupId,
                                                              @Variable String internalCorrelationId,
                                                              @Variable String tenantIdentifier,
                                                              @Variable String paymentScheme,
                                                              @Variable BigDecimal amount,
                                                              @Variable String conversionAccountAmsId,
                                                              @Variable String pacs004,
                                                              @Variable String accountProductType) {
        log.info("bookCreditedAmountToConversionAccountInRecall");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "bookCreditedAmountToConversionAccountInRecall", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> bookCreditedAmountToConversionAccountInRecall(originalPacs008,
                        transactionDate,
                        transactionCategoryPurposeCode,
                        transactionGroupId,
                        internalCorrelationId,
                        tenantIdentifier,
                        paymentScheme,
                        amount,
                        conversionAccountAmsId,
                        pacs004,
                        accountProductType));
    }

    private Void bookCreditedAmountToConversionAccountInRecall(String originalPacs008,
                                                               String transactionDate,
                                                               String transactionCategoryPurposeCode,
                                                               String internalCorrelationId,
                                                               String transactionGroupId,
                                                               String tenantIdentifier,
                                                               String paymentScheme,
                                                               BigDecimal amount,
                                                               String conversionAccountAmsId,
                                                               String originalPacs004,
                                                               String accountProductType) {
        try {
            iso.std.iso._20022.tech.xsd.pacs_008_001.Document pacs008 = jaxbUtils.unmarshalPacs008(originalPacs008);

            iso.std.iso._20022.tech.xsd.pacs_004_001.Document pacs004 = jaxbUtils.unmarshalPacs004(originalPacs004);

            //TODO: IG2-nél egyáltalán nincs pacs.002; AFR-nél van, de nem tesszük be a flow-ba

            String conversionAccountWithdrawalRelativeUrl = String.format("%s%s/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "deposit");

            Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
            String configOperationKey = String.format("%s.%s", paymentScheme, "bookCreditedAmountToConversionAccountInRecall.ConversionAccount.DepositTransactionAmount");
            Integer paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(configOperationKey);
            String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(configOperationKey);

            TransactionBody body = new TransactionBody(
                    transactionDate,
                    amount,
                    paymentTypeId,
                    "",
                    FORMAT,
                    locale);

            String bodyItem = painMapper.writeValueAsString(body);

            List<TransactionItem> items = new ArrayList<>();

            batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawalRelativeUrl, bodyItem, false);

            BankToCustomerStatementV08 intermediateCamt053Entry = pacs008Camt053Mapper.toCamt053Entry(pacs008);
            ReportEntry10 convertedCamt053Entry = pacs004Camt053Mapper.convert(pacs004, intermediateCamt053Entry).getStatement().get(0).getEntry().get(0);
            convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
            if (convertedCamt053Entry.getValueDate().getAdditionalProperties().get("Date") == null) {
                convertedCamt053Entry.getValueDate().setAdditionalProperty("Date", transactionDate);
            }
            convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            String camt053Entry = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);

            String camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";

            DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
                    internalCorrelationId,
                    camt053Entry,
                    pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtrAcct().getId().getIBAN(),
                    paymentTypeCode,
                    transactionGroupId,
                    pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getCdtr().getNm(),
                    pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getCdtrAcct().getId().getIBAN(),
                    null,
                    contactDetailsUtil.getId(pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtr().getCtctDtls()),
                    Optional.ofNullable(pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getRmtInf()).map(RemittanceInformation5::getUstrd).map(List::toString).orElse(""),
                    transactionCategoryPurposeCode,
                    paymentScheme,
                    null,
                    conversionAccountAmsId,
                    pacs004.getPmtRtr().getTxInf().get(0).getOrgnlEndToEndId());

            String camt053Body = painMapper.writeValueAsString(td);

            batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);

            doBatch(items,
                    tenantIdentifier,
                    "-1",
                    conversionAccountAmsId,
                    internalCorrelationId,
                    "bookCreditedAmountToConversionAccountInRecall");

        } catch (Exception e) {
            // TODO technical error handling
            log.error("Worker to book incoming money in AMS has failed, dispatching user task to handle conversion account deposit", e);
            throw new ZeebeBpmnError("Error_BookToConversionToBeHandledManually", e.getMessage());
        }
        return null;
    }

    @JobWorker
    @LogInternalCorrelationId
    @TraceZeebeArguments
    public void bookCreditedAmountToConversionAccountInReturn(JobClient jobClient,
                                                              ActivatedJob activatedJob,
                                                              @Variable String pacs004,
                                                              @Variable String transactionDate,
                                                              @Variable String acceptanceDate,
                                                              @Variable String transactionCategoryPurposeCode,
                                                              @Variable String transactionGroupId,
                                                              @Variable String internalCorrelationId,
                                                              @Variable String tenantIdentifier,
                                                              @Variable String paymentScheme,
                                                              @Variable BigDecimal amount,
                                                              @Variable String conversionAccountAmsId,
                                                              @Variable String accountProductType) {
        log.info("bookCreditedAmountToConversionAccountInReturn");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "bookCreditedAmountToConversionAccountInReturn", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> bookCreditedAmountToConversionAccountInReturn(pacs004,
                        transactionDate,
                        transactionCategoryPurposeCode,
                        transactionGroupId,
                        internalCorrelationId,
                        tenantIdentifier,
                        paymentScheme,
                        amount,
                        conversionAccountAmsId,
                        accountProductType));
    }

    private Void bookCreditedAmountToConversionAccountInReturn(String pacs004,
                                                               String transactionDate,
                                                               String transactionCategoryPurposeCode,
                                                               String transactionGroupId,
                                                               String internalCorrelationId,
                                                               String tenantIdentifier,
                                                               String paymentScheme,
                                                               BigDecimal amount,
                                                               String conversionAccountAmsId,
                                                               String accountProductType) {
        try {
            MDC.put("internalCorrelationId", internalCorrelationId);
            log.info("book to conversion account in return (pacs.004) {} started for {} on {} ", internalCorrelationId, paymentScheme, tenantIdentifier);

            String conversionAccountWithdrawalRelativeUrl = String.format("%s%s/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "deposit");

            Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
            String depositAmountOperation = "bookCreditedAmountToConversionAccountInReturn.ConversionAccount.DepositTransactionAmount";
            String configOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
            Integer paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(configOperationKey);
            String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(configOperationKey);

            iso.std.iso._20022.tech.xsd.pacs_004_001.Document pacs_004 = jaxbUtils.unmarshalPacs004(pacs004);

            TransactionBody body = new TransactionBody(
                    transactionDate,
                    amount,
                    paymentTypeId,
                    "",
                    FORMAT,
                    locale);

            String bodyItem = painMapper.writeValueAsString(body);

            List<TransactionItem> items = new ArrayList<>();

            batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawalRelativeUrl, bodyItem, false);

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

            String camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";

            DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
                    internalCorrelationId,
                    camt053Entry,
                    pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getDbtrAcct().getId().getIBAN(),
                    paymentTypeCode,
                    transactionGroupId,
                    pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getCdtr().getNm(),
                    pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getCdtrAcct().getId().getIBAN(),
                    null,
                    contactDetailsUtil.getId(pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getCdtr().getCtctDtls()),
                    Optional.ofNullable(pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getRmtInf())
                            .map(iso.std.iso._20022.tech.xsd.pacs_004_001.RemittanceInformation5::getUstrd).map(List::toString).orElse(""),
                    transactionCategoryPurposeCode,
                    paymentScheme,
                    null,
                    conversionAccountAmsId,
                    pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlEndToEndId());

            String camt053Body = painMapper.writeValueAsString(td);

            batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);

            doBatch(items,
                    tenantIdentifier,
                    "-1",
                    conversionAccountAmsId,
                    internalCorrelationId,
                    "bookCreditedAmountToConversionAccountInReturn");
        } catch (Exception e) {
            log.error("Worker to book incoming money in AMS has failed, dispatching user task to handle conversion account deposit", e);
            throw new RuntimeException(e);
        } finally {
            MDC.remove("internalCorrelationId");
        }
        return null;
    }
}