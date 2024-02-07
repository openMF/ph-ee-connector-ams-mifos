package org.mifos.connector.ams.zeebe.workers.bookamount;

import com.baasflow.commons.events.EventService;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Component
@Slf4j
public class BookCreditedAmountFromTechnicalAccountWorker extends AbstractMoneyInOutWorker {

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
    public void bookCreditedAmountFromTechnicalAccount(JobClient jobClient,
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
                                                       @Variable String generatedPacs004Fragment,
                                                       @Variable String pacs002,
                                                       @Variable String caseIdentifier,
                                                       @Variable String accountProductType) {
        log.info("bookCreditedAmountFromTechnicalAccount");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "bookCreditedAmountFromTechnicalAccount", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> bookCreditedAmountFromTechnicalAccount(originalPacs008,
                        amount,
                        tenantIdentifier,
                        paymentScheme,
                        transactionDate,
                        currency,
                        internalCorrelationId,
                        transactionGroupId,
                        transactionCategoryPurposeCode,
                        generatedPacs004Fragment,
                        pacs002,
                        caseIdentifier,
                        accountProductType));
    }

    private Void bookCreditedAmountFromTechnicalAccount(String originalPacs008,
                                                        String amount,
                                                        String tenantIdentifier,
                                                        String paymentScheme,
                                                        String transactionDate,
                                                        String currency,
                                                        String internalCorrelationId,
                                                        String transactionGroupId,
                                                        String transactionCategoryPurposeCode,
                                                        String originalPacs004,
                                                        String originalPacs002,
                                                        String caseIdentifier,
                                                        String accountProductType) {
        try {
            List<TransactionItem> items = new ArrayList<>();

            iso.std.iso._20022.tech.xsd.pacs_008_001.Document pacs008 = jaxbUtils.unmarshalPacs008(originalPacs008);
            iso.std.iso._20022.tech.xsd.pacs_004_001.Document pacs004 = jaxbUtils.unmarshalPacs004(originalPacs004);
            iso.std.iso._20022.tech.xsd.pacs_002_001.Document pacs002 = jaxbUtils.unmarshalPacs002(originalPacs002);

            Config technicalAccountConfig = technicalAccountConfigFactory.getConfig(tenantIdentifier);

            String taLookup = String.format("%s.%s", paymentScheme, caseIdentifier);
            log.debug("Looking up account id for {}", taLookup);
            Integer recallTechnicalAccountId = technicalAccountConfig.findPaymentTypeIdByOperation(taLookup);

            String technicalAccountWithdrawalRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), recallTechnicalAccountId, "withdrawal");

            Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
            String configOperationKey = String.format("%s.%s.%s", paymentScheme, "bookCreditedAmountFromTechnicalAccount", caseIdentifier);
            Integer paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(String.format("%s.%s.%s", paymentScheme, "bookCreditedAmountFromTechnicalAccount", caseIdentifier));
            String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(configOperationKey);

            TransactionBody body = new TransactionBody(
                    transactionDate,
                    amount,
                    paymentTypeId,
                    "",
                    FORMAT,
                    locale);

            String bodyItem = painMapper.writeValueAsString(body);

            batchItemBuilder.add(tenantIdentifier, items, technicalAccountWithdrawalRelativeUrl, bodyItem, false);

            BankToCustomerStatementV08 intermediateCamt053 = pacs008Camt053Mapper.toCamt053Entry(pacs008);
            ReportEntry10 convertedCamt053Entry = pacs004Camt053Mapper.convert(pacs004, intermediateCamt053).getStatement().get(0).getEntry().get(0);
            EntryTransaction10 entryTransaction10 = convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0);
            entryTransaction10.setAdditionalTransactionInformation(paymentTypeCode);
            entryTransaction10.setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));

            XMLGregorianCalendar pacs002AccptncDtTm = pacs002.getFIToFIPmtStsRpt().getTxInfAndSts().get(0).getAccptncDtTm();
            if (pacs002AccptncDtTm == null) {
                convertedCamt053Entry.getValueDate().setAdditionalProperty("Date", hyphenateDate(transactionDate));
            } else {
                ZoneId zi = TimeZone.getTimeZone("Europe/Budapest").toZoneId();
                ZonedDateTime zdt = pacs002AccptncDtTm.toGregorianCalendar().toZonedDateTime().withZoneSameInstant(zi);
                var copy = DatatypeFactory.newDefaultInstance().newXMLGregorianCalendar(GregorianCalendar.from(zdt));
                convertedCamt053Entry.getValueDate().setAdditionalProperty("Date", copy.toGregorianCalendar().toZonedDateTime().format(DateTimeFormatter.ISO_LOCAL_DATE));
            }

            String camt053Entry = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);

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
                    pacs004.getPmtRtr().getTxInf().get(0).getOrgnlEndToEndId());

            String camt053Body = painMapper.writeValueAsString(td);

            batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);

            doBatch(items,
                    tenantIdentifier,
                    "-1",
                    "-1",
                    internalCorrelationId,
                    "bookCreditedAmountFromTechnicalAccount");

        } catch (JsonProcessingException | JAXBException e) {
            log.error("Worker to book incoming money in AMS has failed, dispatching user task to handle conversion account deposit", e);
            throw new ZeebeBpmnError("Error_BookToConversionToBeHandledManually", e.getMessage());
        }
        return null;
    }

    private String hyphenateDate(String date) {
        return date.substring(0, 4) + "-" + date.substring(4, 6) + "-" + date.substring(6);
    }
}