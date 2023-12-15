package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
import java.time.LocalDate;
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
import org.mifos.connector.ams.mapstruct.Pain001Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.BatchItemBuilder;
import org.mifos.connector.ams.zeebe.workers.utils.ContactDetailsUtil;
import org.mifos.connector.ams.zeebe.workers.utils.DtSavingsTransactionDetails;
import org.mifos.connector.ams.zeebe.workers.utils.JAXBUtils;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionBody;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;
import org.slf4j.MDC;
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
import iso.std.iso._20022.tech.json.camt_053_001.AccountStatement9;
import iso.std.iso._20022.tech.json.camt_053_001.ActiveOrHistoricCurrencyAndAmountRange2.CreditDebitCode;
import iso.std.iso._20022.tech.json.camt_053_001.BankToCustomerStatementV08;
import iso.std.iso._20022.tech.json.camt_053_001.EntryDetails9;
import iso.std.iso._20022.tech.json.camt_053_001.EntryStatus1Choice;
import iso.std.iso._20022.tech.json.camt_053_001.EntryTransaction10;
import iso.std.iso._20022.tech.json.camt_053_001.ReportEntry10;
import iso.std.iso._20022.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;
import iso.std.iso._20022.tech.xsd.pacs_004_001.PaymentTransactionInformation27;
import jakarta.xml.bind.JAXBException;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class BookOnConversionAccountInAmsWorker extends AbstractMoneyInOutWorker {

    @Autowired
    private Pain001Camt053Mapper camt053Mapper;

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

    @JobWorker
    @LogInternalCorrelationId
    @TraceZeebeArguments
    public void bookOnConversionAccountInAms(JobClient jobClient,
                                             ActivatedJob activatedJob,
                                             @Variable String originalPain001,
                                             @Variable String internalCorrelationId,
                                             @Variable String transactionFeeInternalCorrelationId,
                                             @Variable String paymentScheme,
                                             @Variable String transactionDate,
                                             @Variable Integer conversionAccountAmsId,
                                             @Variable String transactionGroupId,
                                             @Variable String transactionCategoryPurposeCode,
                                             @Variable String transactionFeeCategoryPurposeCode,
                                             @Variable BigDecimal amount,
                                             @Variable BigDecimal transactionFeeAmount,
                                             @Variable String tenantIdentifier) {
        log.info("bookOnConversionAccountInAms");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "bookOnConversionAccountInAms", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> bookOnConversionAccountInAms(originalPain001,
                        internalCorrelationId,
                        transactionFeeInternalCorrelationId,
                        paymentScheme,
                        transactionDate,
                        conversionAccountAmsId,
                        transactionGroupId,
                        transactionCategoryPurposeCode,
                        transactionFeeCategoryPurposeCode,
                        amount,
                        transactionFeeAmount,
                        tenantIdentifier,
                        eventBuilder));
    }

    private Void bookOnConversionAccountInAms(String originalPain001,
                                              String internalCorrelationId,
                                              String transactionFeeInternalCorrelationId,
                                              String paymentScheme,
                                              String transactionDate,
                                              Integer conversionAccountAmsId,
                                              String transactionGroupId,
                                              String transactionCategoryPurposeCode,
                                              String transactionFeeCategoryPurposeCode,
                                              BigDecimal amount,
                                              BigDecimal transactionFeeAmount,
                                              String tenantIdentifier,
                                              Event.Builder eventBuilder) {
    	try {
    		transactionDate = transactionDate.replaceAll("-", "");
		
    		objectMapper.setSerializationInclusion(Include.NON_NULL);
		
    		Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001;
			pain001 = objectMapper.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
		
			MDC.put("internalCorrelationId", internalCorrelationId);
			
			log.info("Starting book debit on conversion account worker");
			
			log.info("Withdrawing amount {} from conversion account {} of tenant {}", amount, conversionAccountAmsId, tenantIdentifier);
			
			batchItemBuilder.tenantId(tenantIdentifier);
			
			String conversionAccountWithdrawalRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");
			
			Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
			String withdrawAmountOperation = "bookOnConversionAccountInAms.ConversionAccount.WithdrawTransactionAmount";
			String configOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
			Integer paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(configOperationKey);
			String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(configOperationKey);
			
			TransactionBody body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			String bodyItem = objectMapper.writeValueAsString(body);
			
			List<TransactionItem> items = new ArrayList<>();
			
			batchItemBuilder.add(items, conversionAccountWithdrawalRelativeUrl, bodyItem, false);
		
			BankToCustomerStatementV08 convertedStatement = camt053Mapper.toCamt053Entry(pain001.getDocument());
			ReportEntry10 convertedcamt053Entry = convertedStatement.getStatement().get(0).getEntry().get(0);
			convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.DBIT);
			convertedcamt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
			convertedcamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
			convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
			String camt053Entry = objectMapper.writeValueAsString(convertedcamt053Entry);
			
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
					transactionCategoryPurposeCode,
					paymentScheme,
					conversionAccountAmsId,
					null,
					pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getPaymentIdentification().getEndToEndIdentification());
			
			String camt053Body = objectMapper.writeValueAsString(td);
	
			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
			if (!BigDecimal.ZERO.equals(transactionFeeAmount)) {
					
				log.info("Withdrawing fee {} from conversion account {}", transactionFeeAmount, conversionAccountAmsId);
				
				String withdrawFeeOperation = "bookOnConversionAccountInAms.ConversionAccount.WithdrawTransactionFee";
				String withdrawFeeConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawFeeOperation);
				paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawFeeConfigOperationKey);
				paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawFeeConfigOperationKey);
				convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
				convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getSupplementaryData().clear();
				camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001.getDocument(), convertedcamt053Entry, transactionFeeCategoryPurposeCode);
				camt053Mapper.refillOtherIdentification(pain001.getDocument(), convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0));

				camt053Entry = objectMapper.writeValueAsString(convertedcamt053Entry);
				
				body = new TransactionBody(
						transactionDate,
						transactionFeeAmount,
						paymentTypeId,
						"",
						FORMAT,
						locale);
				
				bodyItem = objectMapper.writeValueAsString(body);
				
				batchItemBuilder.add(items, conversionAccountWithdrawalRelativeUrl, bodyItem, false);
			
				td = new DtSavingsTransactionDetails(
						transactionFeeInternalCorrelationId,
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
						transactionFeeCategoryPurposeCode,
						paymentScheme,
						conversionAccountAmsId,
						null,
						pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getPaymentIdentification().getEndToEndIdentification());
				camt053Body = objectMapper.writeValueAsString(td);
				batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			}
			
			doBatch(items,
	                tenantIdentifier,
	                -1,
	                conversionAccountAmsId,
	                internalCorrelationId,
	                "bookOnConversionAccountInAms");
			
			log.info("Book debit on conversion account has finished  successfully");
			
			MDC.remove("internalCorrelationId");
    	} catch (JsonProcessingException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}

        return null;
    }

    @JobWorker
    @TraceZeebeArguments
    @LogInternalCorrelationId
    public void withdrawTheAmountFromConversionAccountInAms(JobClient client,
                                                            ActivatedJob activatedJob,
                                                            @Variable BigDecimal amount,
                                                            @Variable Integer conversionAccountAmsId,
                                                            @Variable String tenantIdentifier,
                                                            @Variable String paymentScheme,
                                                            @Variable String transactionCategoryPurposeCode,
                                                            @Variable String camt056,
                                                            @Variable String debtorIban,
                                                            @Variable String generatedPacs004,
                                                            @Variable String pacs002,
                                                            @Variable String transactionDate) {
        log.info("withdrawTheAmountFromConversionAccountInAms");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob,
                        "withdrawTheAmountFromConversionAccountInAms",
                        null,
                        null,
                        eventBuilder),
                eventBuilder -> withdrawTheAmountFromConversionAccountInAms(amount,
                        conversionAccountAmsId,
                        tenantIdentifier,
                        paymentScheme,
                        transactionCategoryPurposeCode,
                        camt056,
                        debtorIban,
                        generatedPacs004,
                        pacs002,
                        transactionDate,
                        eventBuilder));
    }

    private Void withdrawTheAmountFromConversionAccountInAms(BigDecimal amount,
                                                             Integer conversionAccountAmsId,
                                                             String tenantIdentifier,
                                                             String paymentScheme,
                                                             String transactionCategoryPurposeCode,
                                                             String camt056,
                                                             String debtorIban,
                                                             String originalPacs004,
                                                             String originalPacs002,
                                                             String transactionDate,
                                                             Event.Builder eventBuilder) {
    	try {
			log.info("Withdrawing amount {} from conversion account {} of tenant {}", amount, conversionAccountAmsId, tenantIdentifier);
			
			if (transactionDate == null) {
				transactionDate = LocalDate.now().format(DateTimeFormatter.ofPattern(FORMAT));
			}
			
			iso.std.iso._20022.tech.xsd.pacs_004_001.Document pacs004 = jaxbUtils.unmarshalPacs004(originalPacs004);
			
			iso.std.iso._20022.tech.xsd.pacs_002_001.Document pacs002 = jaxbUtils.unmarshalPacs002(originalPacs002);
			
			batchItemBuilder.tenantId(tenantIdentifier);
			
			String conversionAccountWithdrawalRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");
			
			Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
			String withdrawAmountOperation = "withdrawTheAmountFromConversionAccountInAms.ConversionAccount.WithdrawTransactionAmount";
			String configOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
			Integer paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(configOperationKey);
			String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(configOperationKey);
			
			TransactionBody body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			objectMapper.setSerializationInclusion(Include.NON_NULL);
			
			String bodyItem = objectMapper.writeValueAsString(body);
			
			List<TransactionItem> items = new ArrayList<>();
			
			batchItemBuilder.add(items, conversionAccountWithdrawalRelativeUrl, bodyItem, false);
		
			Pacs004ToCamt053Converter converter = new Pacs004ToCamt053Converter();
			ReportEntry10 camt053Entry = converter.convert(pacs004, 
            		new BankToCustomerStatementV08()
    				.withStatement(List.of(new AccountStatement9()
    						.withEntry(List.of(new ReportEntry10()
    								.withEntryDetails(List.of(new EntryDetails9()
    										.withTransactionDetails(List.of(new EntryTransaction10()))))))))).getStatement().get(0).getEntry().get(0);
			camt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
			camt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
			
			PaymentTransactionInformation27 paymentTransactionInformation = pacs004
					.getPmtRtr()
					.getTxInf().get(0);
			
			String originalDebtorBic = paymentTransactionInformation
					.getOrgnlTxRef()
					.getDbtrAgt()
					.getFinInstnId()
					.getBIC();
			
			pacs004.getPmtRtr().getGrpHdr().getIntrBkSttlmDt();
			String originalCreationDate = paymentTransactionInformation
					.getOrgnlTxRef()
					.getIntrBkSttlmDt()
					.toGregorianCalendar()
					.toZonedDateTime()
					.toLocalDate()
					.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
			String originalEndToEndId = paymentTransactionInformation
					.getOrgnlEndToEndId();
			
			String internalCorrelationId = String.format("%s_%s_%s", originalDebtorBic, originalCreationDate, originalEndToEndId);
			
			XMLGregorianCalendar orgnlCreDtTm = pacs002.getFIToFIPmtStsRpt().getOrgnlGrpInfAndSts().getOrgnlCreDtTm();
			if (orgnlCreDtTm == null) {
				camt053Entry.getValueDate().setAdditionalProperty("Date", transactionDate);
			} else {
				ZoneId zi = TimeZone.getTimeZone("Europe/Budapest").toZoneId();
				ZonedDateTime zdt = orgnlCreDtTm.toGregorianCalendar().toZonedDateTime().withZoneSameInstant(zi);
				var copy = DatatypeFactory.newDefaultInstance().newXMLGregorianCalendar(GregorianCalendar.from(zdt));
				String date = copy.toGregorianCalendar().toZonedDateTime().format(DateTimeFormatter.ISO_LOCAL_DATE);
				camt053Entry.getValueDate().setAdditionalProperty("Date", date);
			}
			
			camt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
			
			String camt053 = objectMapper.writeValueAsString(camt053Entry);
			
			String camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";
			
			DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
					internalCorrelationId,
					camt053,
					paymentTransactionInformation.getOrgnlTxRef().getCdtrAcct().getId().getIBAN(),
					paymentTypeCode,
					internalCorrelationId,
					paymentTransactionInformation.getOrgnlTxRef().getDbtr().getNm(),
					paymentTransactionInformation.getOrgnlTxRef().getDbtrAcct().getId().getIBAN(),
					null,
					contactDetailsUtil.getId(paymentTransactionInformation.getOrgnlTxRef().getDbtr().getCtctDtls()),
					Optional.ofNullable(paymentTransactionInformation.getOrgnlTxRef().getRmtInf())
							.map(iso.std.iso._20022.tech.xsd.pacs_004_001.RemittanceInformation5::getUstrd).map(List::toString).orElse(""),
					transactionCategoryPurposeCode,
					paymentScheme,
					conversionAccountAmsId,
					null,
					paymentTransactionInformation.getOrgnlEndToEndId());
			
			String camt053Body = objectMapper.writeValueAsString(td);
	
			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
			doBatch(items,
                    tenantIdentifier,
                    -1,
                    conversionAccountAmsId,
                    internalCorrelationId,
                    "withdrawTheAmountFromConversionAccountInAms");
		} catch (JAXBException | JsonProcessingException e) {
			log.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}
        return null;
    }
}