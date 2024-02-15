package org.mifos.connector.ams.zeebe.workers.bookamount;

import com.baasflow.commons.events.EventService;
import com.fasterxml.jackson.databind.ObjectMapper;
import hu.dpc.rt.utils.converter.Pacs004ToCamt053Converter;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import iso.std.iso._20022.tech.json.camt_053_001.ActiveOrHistoricCurrencyAndAmountRange2.CreditDebitCode;
import iso.std.iso._20022.tech.json.camt_053_001.BankToCustomerStatementV08;
import iso.std.iso._20022.tech.json.camt_053_001.EntryStatus1Choice;
import iso.std.iso._20022.tech.json.camt_053_001.EntryTransaction10;
import iso.std.iso._20022.tech.json.camt_053_001.ReportEntry10;
import iso.std.iso._20022.tech.xsd.pacs_008_001.CreditTransferTransactionInformation11;
import iso.std.iso._20022.tech.xsd.pacs_008_001.RemittanceInformation5;
import jakarta.xml.bind.JAXBException;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Component
@Slf4j
public class BookCreditedAmountToTechnicalAccountWorker extends AbstractMoneyInOutWorker {

    @Value("${fineract.incoming-money-api}")
    private String incomingMoneyApi;

    @Value("${fineract.locale}")
    private String locale;

    @Autowired
    private BatchItemBuilder batchItemBuilder;

    @Autowired
    private ConfigFactory paymentTypeConfigFactory;

    @Autowired
    private ConfigFactory technicalAccountConfigFactory;

    @Autowired
    private JAXBUtils jaxbUtils;

    @Autowired
    private Pacs008Camt053Mapper pacs008Camt053Mapper;

    private Pacs004ToCamt053Converter pacs004Camt053Mapper = new Pacs004ToCamt053Converter();

    @Autowired
    private ContactDetailsUtil contactDetailsUtil;

    @Autowired
    private EventService eventService;

    @Autowired
    private SerializationHelper serializationHelper;

    @Autowired
    @Qualifier("painMapper")
    private ObjectMapper painMapper;

    private static final String FORMAT = "yyyyMMdd";

    @JobWorker
    @LogInternalCorrelationId
    @TraceZeebeArguments
    public void bookCreditedAmountToTechnicalAccount(JobClient jobClient,
                                                     ActivatedJob activatedJob,
                                                     @Variable String originalPacs008,
                                                     @Variable String amount,
                                                     @Variable String tenantIdentifier,
                                                     @Variable String paymentScheme,
                                                     @Variable String transactionDate,
                                                     @Variable String currency,
                                                     @Variable String internalCorrelationId,
                                                     @Variable String transactionGroupId,
                                                     @Variable String transactionCategoryPurposeCode,
                                                     @Variable String caseIdentifier,
                                                     @Variable String pacs004,
                                                     @Variable String accountProductType) {
        log.info("bookCreditedAmountToTechnicalAccount");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "bookCreditedAmountToTechnicalAccount", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> bookCreditedAmountToTechnicalAccount(originalPacs008,
                        amount,
                        tenantIdentifier,
                        paymentScheme,
                        transactionDate,
                        currency,
                        internalCorrelationId,
                        transactionGroupId,
                        transactionCategoryPurposeCode,
                        caseIdentifier,
                        pacs004,
                        accountProductType));
    }

    private Void bookCreditedAmountToTechnicalAccount(String originalPacs008,
                                                      String amount,
                                                      String tenantIdentifier,
                                                      String paymentScheme,
                                                      String transactionDate,
                                                      String currency,
                                                      String internalCorrelationId,
                                                      String transactionGroupId,
                                                      String transactionCategoryPurposeCode,
                                                      String caseIdentifier,
                                                      String originalPacs004,
                                                      String accountProductType) {
        try {
            log.info("Incoming money, bookCreditedAmountToTechnicalAccount worker started with variables");
            iso.std.iso._20022.tech.xsd.pacs_008_001.Document pacs008 = jaxbUtils.unmarshalPacs008(originalPacs008);
            MDC.put("internalCorrelationId", internalCorrelationId);

            Config technicalAccountConfig = technicalAccountConfigFactory.getConfig(tenantIdentifier);

            String taLookup = String.format("%s.%s", paymentScheme, caseIdentifier);
            log.debug("Looking up account id for {}", taLookup);
            Integer recallTechnicalAccountId = technicalAccountConfig.findPaymentTypeIdByOperation(taLookup);

            String conversionAccountWithdrawalRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), recallTechnicalAccountId, "deposit");

            Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
            String configOperationKey = String.format("%s.%s.%s", paymentScheme, "bookCreditedAmountToTechnicalAccount", caseIdentifier);
            Integer paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(configOperationKey);
            String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(configOperationKey);

            TransactionBody body = new TransactionBody(transactionDate, amount, paymentTypeId, "", FORMAT, locale);
            String bodyItem = painMapper.writeValueAsString(body);

            List<TransactionItem> items = new ArrayList<>();
            batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawalRelativeUrl, bodyItem, false);

            BankToCustomerStatementV08 intermediateCamt053 = pacs008Camt053Mapper.toCamt053Entry(pacs008);
            ReportEntry10 reportEntry10 = intermediateCamt053.getStatement().get(0).getEntry().get(0);
            EntryTransaction10 entryTransaction10 = reportEntry10.getEntryDetails().get(0).getTransactionDetails().get(0);
            entryTransaction10.setAdditionalTransactionInformation(paymentTypeCode);
            entryTransaction10.setCreditDebitIndicator(CreditDebitCode.CRDT);
            reportEntry10.setCreditDebitIndicator(CreditDebitCode.CRDT);
            reportEntry10.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            ReportEntry10 camt053 = originalPacs004 == null
                    ? reportEntry10
                    : enhanceFromPacs004(originalPacs004, paymentTypeCode, intermediateCamt053);
            String camt053Entry = serializationHelper.writeCamt053AsString(accountProductType, camt053);

            String camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";

            CreditTransferTransactionInformation11 creditTransferTransactionInformation11 = pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0);
            DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
                    internalCorrelationId,
                    camt053Entry,
                    creditTransferTransactionInformation11.getCdtrAcct().getId().getIBAN(),
                    paymentTypeCode,
                    transactionGroupId,
                    creditTransferTransactionInformation11.getDbtr().getNm(),
                    creditTransferTransactionInformation11.getDbtrAcct().getId().getIBAN(),
                    null,
                    contactDetailsUtil.getId(creditTransferTransactionInformation11.getDbtr().getCtctDtls()),
                    Optional.ofNullable(creditTransferTransactionInformation11.getRmtInf()).map(RemittanceInformation5::getUstrd).map(List::toString).orElse(""),
                    transactionCategoryPurposeCode,
                    paymentScheme,
                    null,
                    null,
                    creditTransferTransactionInformation11.getPmtId().getEndToEndId());

            String camt053Body = painMapper.writeValueAsString(td);

            batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);

            doBatch(items,
                    tenantIdentifier,
                    transactionGroupId,
                    "-1",
                    "-1",
                    internalCorrelationId,
                    "bookCreditedAmountToTechnicalAccount");
        } catch (Exception e) {
            log.error("Worker to book incoming money in AMS has failed, dispatching user task to handle conversion account deposit", e);
            throw new ZeebeBpmnError("Error_BookToConversionToBeHandledManually", e.getMessage());
        } finally {
            MDC.remove("internalCorrelationId");
        }
        return null;
    }

    private ReportEntry10 enhanceFromPacs004(String originalPacs004,
                                             String paymentTypeCode, BankToCustomerStatementV08 intermediateCamt053) throws JAXBException {
        iso.std.iso._20022.tech.xsd.pacs_004_001.Document pacs004 = jaxbUtils.unmarshalPacs004(originalPacs004);
        ReportEntry10 convertedCamt053Entry = pacs004Camt053Mapper.convert(pacs004, intermediateCamt053).getStatement().get(0).getEntry().get(0);
        convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
        convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.CRDT);
        convertedCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));

        return convertedCamt053Entry;
    }
}