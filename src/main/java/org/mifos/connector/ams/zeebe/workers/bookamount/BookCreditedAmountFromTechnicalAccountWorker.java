package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.baasflow.commons.events.Event;
import com.baasflow.commons.events.EventService;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;	
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

import hu.dpc.rt.utils.converter.Pacs004ToCamt053Converter;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import iso.std.iso._20022.tech.json.camt_053_001.ActiveOrHistoricCurrencyAndAmountRange2.CreditDebitCode;
import iso.std.iso._20022.tech.json.camt_053_001.BankToCustomerStatementV08;
import iso.std.iso._20022.tech.json.camt_053_001.ReportEntry10;
import iso.std.iso._20022.tech.xsd.pacs_008_001.RemittanceInformation5;
import jakarta.xml.bind.JAXBException;
import lombok.extern.slf4j.Slf4j;

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
    
    private ObjectMapper objectMapper = new ObjectMapper() {
		private static final long serialVersionUID = 1L;

		{
    		registerModule(new AfterburnerModule());
    		registerModule(new JavaTimeModule());
    		configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    		setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
            .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    	}
    };

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
                                                       @Variable String caseIdentifier) {
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
                        eventBuilder));
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
                                                        Event.Builder eventBuilder) {
        try {
            objectMapper.setSerializationInclusion(Include.NON_NULL);

            List<TransactionItem> items = new ArrayList<>();

            iso.std.iso._20022.tech.xsd.pacs_008_001.Document pacs008 = jaxbUtils.unmarshalPacs008(originalPacs008);
            
            iso.std.iso._20022.tech.xsd.pacs_004_001.Document pacs004 = jaxbUtils.unmarshalPacs004(originalPacs004);
            
            iso.std.iso._20022.tech.xsd.pacs_002_001.Document pacs002 = jaxbUtils.unmarshalPacs002(originalPacs002);

            batchItemBuilder.tenantId(tenantIdentifier);

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

            String bodyItem = objectMapper.writeValueAsString(body);

            batchItemBuilder.add(items, technicalAccountWithdrawalRelativeUrl, bodyItem, false);

            BankToCustomerStatementV08 intermediateCamt053 = pacs008Camt053Mapper.toCamt053Entry(pacs008);
            ReportEntry10 convertedCamt053Entry = pacs004Camt053Mapper.convert(pacs004, intermediateCamt053).getStatement().get(0).getEntry().get(0);
            convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
            convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
            
            XMLGregorianCalendar pacs002AccptncDtTm = pacs002.getFIToFIPmtStsRpt().getTxInfAndSts().get(0).getAccptncDtTm();
            if (pacs002AccptncDtTm == null) {
            	convertedCamt053Entry.getValueDate().setAdditionalProperty("Date", transactionDate);
            } else {
	            ZoneId zi = TimeZone.getTimeZone("Europe/Budapest").toZoneId();
				ZonedDateTime zdt = pacs002AccptncDtTm.toGregorianCalendar().toZonedDateTime().withZoneSameInstant(zi);
		        var copy = DatatypeFactory.newDefaultInstance().newXMLGregorianCalendar(GregorianCalendar.from(zdt));
		        convertedCamt053Entry.getValueDate().setAdditionalProperty("Date", copy.toGregorianCalendar().toZonedDateTime().format(DateTimeFormatter.ISO_LOCAL_DATE));
            }
            
            String camt053Entry = objectMapper.writeValueAsString(convertedCamt053Entry);

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
    				null);

            String camt053Body = objectMapper.writeValueAsString(td);

            batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);

            doBatch(items,
                    tenantIdentifier,
                    -1,
                    -1,
                    internalCorrelationId,
                    "bookCreditedAmountFromTechnicalAccount");

        } catch (JsonProcessingException | JAXBException e) {
            log.error("Worker to book incoming money in AMS has failed, dispatching user task to handle conversion account deposit", e);
            throw new ZeebeBpmnError("Error_BookToConversionToBeHandledManually", e.getMessage());
        }
        return null;
    }
}