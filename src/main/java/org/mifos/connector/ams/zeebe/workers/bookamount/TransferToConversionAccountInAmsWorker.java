package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

import org.mifos.connector.ams.fineract.Config;
import org.mifos.connector.ams.fineract.ConfigFactory;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.log.LogInternalCorrelationId;
import org.mifos.connector.ams.log.TraceZeebeArguments;
import org.mifos.connector.ams.mapstruct.Pain001Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.AuthTokenHelper;
import org.mifos.connector.ams.zeebe.workers.utils.BatchItemBuilder;
import org.mifos.connector.ams.zeebe.workers.utils.ContactDetailsUtil;
import org.mifos.connector.ams.zeebe.workers.utils.DtSavingsTransactionDetails;
import org.mifos.connector.ams.zeebe.workers.utils.HoldAmountBody;
import org.mifos.connector.ams.zeebe.workers.utils.JAXBUtils;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionBody;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
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

import hu.dpc.rt.utils.converter.Camt056ToCamt053Converter;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import iso.std.iso._20022.tech.json.camt_053_001.ActiveOrHistoricCurrencyAndAmountRange2.CreditDebitCode;
import iso.std.iso._20022.tech.json.camt_053_001.BankToCustomerStatementV08;
import iso.std.iso._20022.tech.json.camt_053_001.EntryStatus1Choice;
import iso.std.iso._20022.tech.json.camt_053_001.ReportEntry10;
import iso.std.iso._20022.tech.json.pain_001_001.CustomerCreditTransferInitiationV10;
import iso.std.iso._20022.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;
import iso.std.iso._20022.tech.xsd.camt_056_001.PaymentTransactionInformation31;
import jakarta.xml.bind.JAXBException;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class TransferToConversionAccountInAmsWorker extends AbstractMoneyInOutWorker {

    @Autowired
    private Pain001Camt053Mapper camt053Mapper;

    @Value("${fineract.incoming-money-api}")
    protected String incomingMoneyApi;

    @Autowired
    private ConfigFactory paymentTypeConfigFactory;

//    @Autowired
//    private JsonSchemaValidator validator;

    @Autowired
    private JAXBUtils jaxbUtils;

    @Autowired
    private AuthTokenHelper authTokenHelper;

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

    private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);

    @JobWorker
    @LogInternalCorrelationId
    @TraceZeebeArguments
    public void transferToConversionAccountInAms(JobClient jobClient,
                                                 ActivatedJob activatedJob,
                                                 @Variable String transactionGroupId,
                                                 @Variable String transactionCategoryPurposeCode,
                                                 @Variable String transactionFeeCategoryPurposeCode,
                                                 @Variable String originalPain001,
                                                 @Variable String internalCorrelationId,
                                                 @Variable BigDecimal amount,
                                                 @Variable BigDecimal transactionFeeAmount,
                                                 @Variable String paymentScheme,
                                                 @Variable Integer disposalAccountAmsId,
                                                 @Variable Integer conversionAccountAmsId,
                                                 @Variable String tenantIdentifier,
                                                 @Variable String iban,
                                                 @Variable String transactionFeeInternalCorrelationId) {
        log.info("transferToConversionAccountInAms");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "transferToConversionAccountInAms", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> transferToConversionAccountInAms(transactionGroupId,
                        transactionCategoryPurposeCode,
                        transactionFeeCategoryPurposeCode,
                        originalPain001,
                        internalCorrelationId,
                        amount,
                        transactionFeeAmount,
                        paymentScheme,
                        disposalAccountAmsId,
                        conversionAccountAmsId,
                        tenantIdentifier,
                        iban,
                        transactionFeeInternalCorrelationId,
                        eventBuilder));
    }

    @SuppressWarnings("unchecked")
	private Void transferToConversionAccountInAms(String transactionGroupId,
                                                  String transactionCategoryPurposeCode,
                                                  String transactionFeeCategoryPurposeCode,
                                                  String originalPain001,
                                                  String internalCorrelationId,
                                                  BigDecimal amount,
                                                  BigDecimal transactionFeeAmount,
                                                  String paymentScheme,
                                                  Integer disposalAccountAmsId,
                                                  Integer conversionAccountAmsId,
                                                  String tenantIdentifier,
                                                  String iban,
                                                  String transactionFeeInternalCorrelationId,
                                                  Event.Builder eventBuilder) {
        try {
        	String transactionDate = LocalDate.now().format(PATTERN);
    		log.debug("Debtor exchange worker starting");
    		MDC.put("internalCorrelationId", internalCorrelationId);
			objectMapper.setSerializationInclusion(Include.NON_NULL);
			Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = objectMapper.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
			
//			try {
//				log.info(">>>>>>>>>>>>>>>>>> Validating incoming pain.001 <<<<<<<<<<<<<<<<");
//
//				Set<ValidationMessage> validationResult = validator.validate(originalPain001);
//
//				if (validationResult.isEmpty()) {
//					log.info(">>>>>>>>>>>>>>>> pain.001 validation successful <<<<<<<<<<<<<<<");
//				} else {
//					log.error(validationResult.toString());
//				}
//			} catch (JsonProcessingException e) {
//				log.warn("Unable to validate pain.001: {}", e.getMessage());
//			}
			
			
			log.debug("Debtor exchange worker incoming variables:");
		
			log.debug("Withdrawing amount {} from disposal account {}", amount, disposalAccountAmsId);
			
			boolean hasFee = !BigDecimal.ZERO.equals(transactionFeeAmount);

			Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
			
			List<TransactionItem> items = new ArrayList<>();
			batchItemBuilder.tenantId(tenantIdentifier);
			
			String holdTransactionUrl = String.format("%s%d/transactions?command=holdAmount", incomingMoneyApi.substring(1), disposalAccountAmsId);

			Integer outHoldReasonId = paymentTypeConfig.findPaymentTypeIdByOperation(String.format("%s.%s", paymentScheme, "outHoldReasonId"));
			HoldAmountBody body = new HoldAmountBody(
	                transactionDate,
	                hasFee ? amount.add(transactionFeeAmount) : amount,
	                outHoldReasonId,
	                locale,
	                FORMAT
	        );
			
			objectMapper.setSerializationInclusion(Include.NON_NULL);
    		String bodyItem = objectMapper.writeValueAsString(body);
    		
    		batchItemBuilder.add(items, holdTransactionUrl, bodyItem, false);
    		
    		BankToCustomerStatementV08 convertedStatement = camt053Mapper.toCamt053Entry(pain001.getDocument());
			ReportEntry10 convertedCamt053Entry = convertedStatement.getStatement().get(0).getEntry().get(0);
			convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getAmountDetails().getTransactionAmount().getAmount().setAmount(hasFee ? amount.add(transactionFeeAmount) : amount);
			String camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";

			String partnerName = pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getName();
			String partnerAccountIban = pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditorAccount().getIdentification().getIban();
			String partnerAccountSecondaryIdentifier = contactDetailsUtil.getId(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getContactDetails());
			String unstructured = Optional.ofNullable(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getRemittanceInformation())
					.map(iso.std.iso._20022.tech.json.pain_001_001.RemittanceInformation16::getUnstructured).map(List::toString).orElse("");
    		
			String holdAmountOperation = "transferToConversionAccountInAms.DisposalAccount.HoldTransactionAmount";
    		addDetails(pain001.getDocument(), transactionGroupId, transactionCategoryPurposeCode, internalCorrelationId, 
					objectMapper, batchItemBuilder, items, convertedCamt053Entry, camt053RelativeUrl, iban, 
					paymentTypeConfig, paymentScheme, holdAmountOperation, partnerName, partnerAccountIban, 
					partnerAccountSecondaryIdentifier, unstructured, null, null, false);
    		
    		
			Long lastHoldTransactionId = holdBatch(items, tenantIdentifier, disposalAccountAmsId, conversionAccountAmsId, internalCorrelationId, "transferToConversionAccountInAms");
			
			HttpHeaders httpHeaders = new HttpHeaders();
			httpHeaders.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
			httpHeaders.set("Authorization", authTokenHelper.generateAuthToken());
			httpHeaders.set("Fineract-Platform-TenantId", tenantIdentifier);
			LinkedHashMap<String, Object> accountDetails = restTemplate.exchange(
					String.format("%s/%s%d", fineractApiUrl, incomingMoneyApi.substring(1), disposalAccountAmsId), 
					HttpMethod.GET, 
					new HttpEntity<>(httpHeaders), 
					LinkedHashMap.class)
				.getBody();
			LinkedHashMap<String, Object> summary = (LinkedHashMap<String, Object>) accountDetails.get("summary");
			BigDecimal availableBalance = new BigDecimal(summary.get("availableBalance").toString());
			if (availableBalance.signum() < 0) {
				restTemplate.exchange(
					String.format("%s/%ssavingsaccounts/%d/transactions/%d?command=releaseAmount", fineractApiUrl, incomingMoneyApi.substring(1), disposalAccountAmsId, lastHoldTransactionId),
					HttpMethod.POST,
					new HttpEntity<>(httpHeaders),
					Object.class
				);
				throw new ZeebeBpmnError("Error_InsufficientFunds", "Insufficient funds");
			}
			
			items.clear();
			
			String releaseTransactionUrl = String.format("%s%d/transactions/%d?command=releaseAmount", incomingMoneyApi.substring(1), disposalAccountAmsId, lastHoldTransactionId);
			batchItemBuilder.add(items, releaseTransactionUrl, null, false);
			String releaseAmountOperation = "transferToConversionAccountInAms.DisposalAccount.ReleaseTransactionAmount";
			addDetails(pain001.getDocument(), transactionGroupId, transactionCategoryPurposeCode, internalCorrelationId, 
					objectMapper, batchItemBuilder, items, convertedCamt053Entry, camt053RelativeUrl, iban, 
					paymentTypeConfig, paymentScheme, releaseAmountOperation, partnerName, partnerAccountIban, 
					partnerAccountSecondaryIdentifier, unstructured, null, null, false);
			
			convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.DBIT);
			convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
			convertedCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "PENDING"));
			
			String disposalAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "withdrawal");

			
			String withdrawAmountOperation = "transferToConversionAccountInAms.DisposalAccount.WithdrawTransactionAmount";
			addExchange(amount, paymentScheme, transactionDate, objectMapper, paymentTypeConfig, batchItemBuilder, items, disposalAccountWithdrawRelativeUrl, withdrawAmountOperation);
			
			iban = pain001.getDocument().getPaymentInformation().get(0).getDebtorAccount().getIdentification().getIban();
			
			convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getAmountDetails().getTransactionAmount().getAmount().setAmount(amount);
			convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.DBIT);
			convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
			convertedCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "PENDING"));
			addDetails(pain001.getDocument(), transactionGroupId, transactionCategoryPurposeCode, internalCorrelationId, 
					objectMapper, batchItemBuilder, items, convertedCamt053Entry, camt053RelativeUrl, iban, 
					paymentTypeConfig, paymentScheme, withdrawAmountOperation, partnerName, partnerAccountIban, 
					partnerAccountSecondaryIdentifier, unstructured, disposalAccountAmsId, conversionAccountAmsId, true);

			if (hasFee) {
				log.debug("Withdrawing fee {} from disposal account {}", transactionFeeAmount, disposalAccountAmsId);
					String withdrawFeeOperation = "transferToConversionAccountInAms.DisposalAccount.WithdrawTransactionFee";
					addExchange(transactionFeeAmount, paymentScheme, transactionDate, objectMapper, paymentTypeConfig, batchItemBuilder, items, disposalAccountWithdrawRelativeUrl, withdrawFeeOperation);
					
					convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getSupplementaryData().get(0).getEnvelope().setAdditionalProperty("InternalCorrelationId", transactionFeeInternalCorrelationId);
					
					convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getAmountDetails().getTransactionAmount().getAmount().setAmount(transactionFeeAmount);
					
					addDetails(pain001.getDocument(), transactionGroupId, transactionFeeCategoryPurposeCode, transactionFeeInternalCorrelationId, objectMapper,
							batchItemBuilder, items, convertedCamt053Entry, camt053RelativeUrl, iban, paymentTypeConfig, paymentScheme, 
							withdrawFeeOperation, partnerName, partnerAccountIban, partnerAccountSecondaryIdentifier, unstructured,
							disposalAccountAmsId, conversionAccountAmsId, true);
			}
			
			log.info("Depositing amount {} to conversion account {}", amount, conversionAccountAmsId);
			
			String conversionAccountDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "deposit");
			
			String depositAmountOperation = "transferToConversionAccountInAms.ConversionAccount.DepositTransactionAmount";
			addExchange(amount, paymentScheme, transactionDate, objectMapper, paymentTypeConfig, batchItemBuilder, items, conversionAccountDepositRelativeUrl, depositAmountOperation);
			
			convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getSupplementaryData().get(0).getEnvelope().setAdditionalProperty("InternalCorrelationId", internalCorrelationId);
			
			convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getAmountDetails().getTransactionAmount().getAmount().setAmount(amount);
			convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.CRDT);
			convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.CRDT);
			convertedCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "PENDING"));
			addDetails(pain001.getDocument(), transactionGroupId, transactionCategoryPurposeCode, internalCorrelationId, objectMapper, batchItemBuilder, items,
					convertedCamt053Entry, camt053RelativeUrl, iban, paymentTypeConfig, paymentScheme, depositAmountOperation, partnerName, 
					partnerAccountIban, partnerAccountSecondaryIdentifier, unstructured, disposalAccountAmsId, conversionAccountAmsId, true);
		
			
			if (hasFee) {
				log.debug("Depositing fee {} to conversion account {}", transactionFeeAmount, conversionAccountAmsId);
				String depositFeeOperation = "transferToConversionAccountInAms.ConversionAccount.DepositTransactionFee";
				addExchange(transactionFeeAmount, paymentScheme, transactionDate, objectMapper, paymentTypeConfig, batchItemBuilder, items, conversionAccountDepositRelativeUrl, depositFeeOperation);
				
				convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getSupplementaryData().get(0).getEnvelope().setAdditionalProperty("InternalCorrelationId", transactionFeeInternalCorrelationId);
				
				convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getAmountDetails().getTransactionAmount().getAmount().setAmount(transactionFeeAmount);
				
				addDetails(pain001.getDocument(), transactionGroupId, transactionFeeCategoryPurposeCode, transactionFeeInternalCorrelationId, objectMapper, batchItemBuilder,
						items, convertedCamt053Entry, camt053RelativeUrl, iban, paymentTypeConfig, paymentScheme, depositFeeOperation, partnerName, 
						partnerAccountIban, partnerAccountSecondaryIdentifier, unstructured, disposalAccountAmsId, conversionAccountAmsId, true);
			}
    			
    		doBatch(items,
                    tenantIdentifier,
                    disposalAccountAmsId,
                    conversionAccountAmsId,
                    internalCorrelationId,
                    "transferToConversionAccountInAms");

        } catch (ZeebeBpmnError z) {
        	throw z;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
        	MDC.remove("internalCorrelationId");
        }
        return null;
    }

    private void addExchange(BigDecimal amount,
                             String paymentScheme,
                             String transactionDate,
                             ObjectMapper om,
                             Config paymentTypeConfig,
                             BatchItemBuilder batchItemBuilder,
                             List<TransactionItem> items,
                             String relativeUrl,
                             String paymentTypeOperation) throws JsonProcessingException {
    	Integer paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(String.format("%s.%s", paymentScheme, paymentTypeOperation));
		
		TransactionBody transactionBody = new TransactionBody(
				transactionDate,
				amount,
				paymentTypeId,
				"",
				FORMAT,
				locale);
		
		String bodyItem = om.writeValueAsString(transactionBody);
		batchItemBuilder.add(items, relativeUrl, bodyItem, false);
    }

    private void addDetails(CustomerCreditTransferInitiationV10 pain001,
    		String transactionGroupId, 
			String transactionFeeCategoryPurposeCode,
			String internalCorrelationId, 
			ObjectMapper om, 
			BatchItemBuilder batchItemBuilder, 
			List<TransactionItem> items,
			ReportEntry10 convertedCamt053Entry, 
			String camt053RelativeUrl,
			String accountIban,
			Config paymentTypeConfig,
			String paymentScheme,
			String paymentTypeOperation,
			String partnerName,
			String partnerAccountIban,
			String partnerAccountSecondaryIdentifier,
			String unstructured,
			Integer sourceAmsAccountId,
			Integer targetAmsAccountId,
			boolean includeSupplementary) throws JsonProcessingException {
    	String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(String.format("%s.%s", paymentScheme, paymentTypeOperation));
    	if (paymentTypeCode == null) {
    		paymentTypeCode = "";
    	}
    	convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
    	convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getSupplementaryData().clear();
    	if (pain001 != null) {
    		if (includeSupplementary) {
    			camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001, convertedCamt053Entry, transactionFeeCategoryPurposeCode);
    			camt053Mapper.refillOtherIdentification(pain001, convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0));
    		}
    	}
    	String camt053 = objectMapper.writeValueAsString(convertedCamt053Entry);
		DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
				internalCorrelationId,
				camt053,
				accountIban,
				paymentTypeCode,
				transactionGroupId,
				partnerName,
				partnerAccountIban,
				null,
				partnerAccountSecondaryIdentifier,
				unstructured,
				transactionFeeCategoryPurposeCode,
				paymentScheme,
				sourceAmsAccountId,
				targetAmsAccountId);
		
		String camt053Body = om.writeValueAsString(td);
		batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
    }

    @JobWorker
    @TraceZeebeArguments
    @LogInternalCorrelationId
    public void withdrawTheAmountFromDisposalAccountInAMS(JobClient client,
                                                          ActivatedJob activatedJob,
                                                          @Variable BigDecimal amount,
                                                          @Variable Integer conversionAccountAmsId,
                                                          @Variable Integer disposalAccountAmsId,
                                                          @Variable String tenantIdentifier,
                                                          @Variable String paymentScheme,
                                                          @Variable String transactionCategoryPurposeCode,
                                                          @Variable String camt056,
                                                          @Variable String iban) {
        log.info("withdrawTheAmountFromDisposalAccountInAMS");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "withdrawTheAmountFromDisposalAccountInAMS",
                        null,
                        null,
                        eventBuilder),
                eventBuilder -> withdrawTheAmountFromDisposalAccountInAMS(amount,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        tenantIdentifier,
                        paymentScheme,
                        transactionCategoryPurposeCode,
                        camt056,
                        iban,
                        eventBuilder));
    }

    @SuppressWarnings("unchecked")
	private Void withdrawTheAmountFromDisposalAccountInAMS(BigDecimal amount,
                                                           Integer conversionAccountAmsId,
                                                           Integer disposalAccountAmsId,
                                                           String tenantIdentifier,
                                                           String paymentScheme,
                                                           String transactionCategoryPurposeCode,
                                                           String camt056,
                                                           String iban,
                                                           Event.Builder eventBuilder) {
    	try {
			String transactionDate = LocalDate.now().format(PATTERN);
			
			log.debug("Withdrawing amount {} from disposal account {}", amount, disposalAccountAmsId);
			
			iso.std.iso._20022.tech.xsd.camt_056_001.Document document = jaxbUtils.unmarshalCamt056(camt056);
    		Camt056ToCamt053Converter converter = new Camt056ToCamt053Converter();
    		BankToCustomerStatementV08 statement = converter.convert(document, new BankToCustomerStatementV08());
    		
    		PaymentTransactionInformation31 paymentTransactionInformation = document
    				.getFIToFIPmtCxlReq()
    				.getUndrlyg().get(0)
    				.getTxInf().get(0);
			
			String originalDebtorBic = paymentTransactionInformation
					.getOrgnlTxRef()
					.getDbtrAgt()
					.getFinInstnId()
					.getBIC();
    		String originalCreationDate = paymentTransactionInformation
    				.getOrgnlIntrBkSttlmDt()
    				.toGregorianCalendar()
    				.toZonedDateTime()
    				.toLocalDate()
    				.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
    		String originalTxId = paymentTransactionInformation
    				.getOrgnlTxId();
    		
    		String internalCorrelationId = String.format("%s_%s_%s", originalDebtorBic, originalCreationDate, originalTxId);
			
			batchItemBuilder.tenantId(tenantIdentifier);
			List<TransactionItem> items = new ArrayList<>();
			Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
			
			String holdTransactionUrl = String.format("%s%d/transactions?command=holdAmount", incomingMoneyApi.substring(1), disposalAccountAmsId);

			Integer outHoldReasonId = paymentTypeConfig.findPaymentTypeIdByOperation(String.format("%s.%s", paymentScheme, "outHoldReasonId"));
			HoldAmountBody holdAmountBody = new HoldAmountBody(
	                transactionDate,
	                amount,
	                outHoldReasonId,
	                locale,
	                FORMAT
	        );
			
			objectMapper.setSerializationInclusion(Include.NON_NULL);
    		String bodyItem = objectMapper.writeValueAsString(holdAmountBody);
    		
    		batchItemBuilder.add(items, holdTransactionUrl, bodyItem, false);
    		
    		ReportEntry10 convertedCamt053Entry = statement.getStatement().get(0).getEntry().get(0);
			convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.DBIT);
			convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
			convertedCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "PENDING"));
			String camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";

			
			String partnerName = document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getCdtr().getNm();
			String partnerAccountIban = document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getCdtrAcct().getId().getIBAN();
			String partnerAccountSecondaryIdentifier = contactDetailsUtil.getId(document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getCdtr().getCtctDtls());
			String unstructured = Optional.ofNullable(document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getRmtInf())
					.map(iso.std.iso._20022.tech.xsd.camt_056_001.RemittanceInformation5::getUstrd).map(List::toString).orElse("");
    		
			String holdAmountOperation = "withdrawTheAmountFromDisposalAccountInAMS.DisposalAccount.HoldTransactionAmount";
			
    		addDetails(null, internalCorrelationId, transactionCategoryPurposeCode, internalCorrelationId, 
					objectMapper, batchItemBuilder, items, convertedCamt053Entry, camt053RelativeUrl, iban, 
					paymentTypeConfig, paymentScheme, holdAmountOperation, partnerName, partnerAccountIban, 
					partnerAccountSecondaryIdentifier, unstructured, disposalAccountAmsId, conversionAccountAmsId, false);
    		
			Long lastHoldTransactionId = holdBatch(items, tenantIdentifier, disposalAccountAmsId, conversionAccountAmsId, internalCorrelationId, "transferToConversionAccountInAms");
			
			HttpHeaders httpHeaders = new HttpHeaders();
			httpHeaders.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
			httpHeaders.set("Authorization", authTokenHelper.generateAuthToken());
			httpHeaders.set("Fineract-Platform-TenantId", tenantIdentifier);
			LinkedHashMap<String, Object> accountDetails = restTemplate.exchange(
					String.format("%s/%s%d", fineractApiUrl, incomingMoneyApi.substring(1), disposalAccountAmsId), 
					HttpMethod.GET, 
					new HttpEntity<>(httpHeaders), 
					LinkedHashMap.class)
				.getBody();
			LinkedHashMap<String, Object> summary = (LinkedHashMap<String, Object>) accountDetails.get("summary");
			BigDecimal availableBalance = new BigDecimal(summary.get("availableBalance").toString());
			if (availableBalance.signum() < 0) {
				restTemplate.exchange(
					String.format("%s/%ssavingsaccounts/%d/transactions/%d?command=releaseAmount", fineractApiUrl, incomingMoneyApi.substring(1), disposalAccountAmsId, lastHoldTransactionId),
					HttpMethod.POST,
					new HttpEntity<>(httpHeaders),
					Object.class
				);
				throw new ZeebeBpmnError("Error_InsufficientFunds", "Insufficient funds");
			}
			
			items.clear();
			
			String releaseTransactionUrl = String.format("%s%d/transactions/%d?command=releaseAmount", incomingMoneyApi.substring(1), disposalAccountAmsId, lastHoldTransactionId);
			batchItemBuilder.add(items, releaseTransactionUrl, null, false);
			String releaseAmountOperation = "withdrawTheAmountFromDisposalAccountInAMS.DisposalAccount.ReleaseTransactionAmount";
			addDetails(null, internalCorrelationId, transactionCategoryPurposeCode, internalCorrelationId, 
					objectMapper, batchItemBuilder, items, convertedCamt053Entry, camt053RelativeUrl, iban, 
					paymentTypeConfig, paymentScheme, releaseAmountOperation, partnerName, partnerAccountIban, 
					partnerAccountSecondaryIdentifier, unstructured, disposalAccountAmsId, conversionAccountAmsId, false);
			
			String disposalAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "withdrawal");
			
			String withdrawAmountOperation = "withdrawTheAmountFromDisposalAccountInAMS.DisposalAccount.WithdrawTransactionAmount";
			String configOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
			Integer paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(configOperationKey);
			String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(configOperationKey);
			
			TransactionBody transactionBody = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			objectMapper.setSerializationInclusion(Include.NON_NULL);
			
			bodyItem = objectMapper.writeValueAsString(transactionBody);
			
			batchItemBuilder.add(items, disposalAccountWithdrawRelativeUrl, bodyItem, false);
			
			convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
			convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.DBIT);
			convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
			convertedCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "PENDING"));
		
			String camt053 = objectMapper.writeValueAsString(statement.getStatement().get(0));
			
			camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";
			
			DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
					internalCorrelationId,
					camt053,
					document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getCdtrAcct().getId().getIBAN(),
					paymentTypeCode,
					internalCorrelationId,
					document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getDbtr().getNm(),
					document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getDbtrAcct().getId().getIBAN(),
					null,
					contactDetailsUtil.getId(document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getDbtr().getCtctDtls()),
					Optional.ofNullable(document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getRmtInf())
							.map(iso.std.iso._20022.tech.xsd.camt_056_001.RemittanceInformation5::getUstrd).map(List::toString).orElse(""),
					transactionCategoryPurposeCode,
					paymentScheme,
					disposalAccountAmsId,
					null);
			
			String camt053Body = objectMapper.writeValueAsString(td);
	
			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
			String conversionAccountDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "deposit");
			
			addExchange(amount, paymentScheme, transactionDate, objectMapper, paymentTypeConfig, batchItemBuilder, items, conversionAccountDepositRelativeUrl, "withdrawTheAmountFromDisposalAccountInAMS.ConversionAccount.DepositTransactionAmount");
			
			camt053Body = objectMapper.writeValueAsString(td);
			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
			doBatch(items,
                    tenantIdentifier,
                    disposalAccountAmsId,
                    conversionAccountAmsId,
                    internalCorrelationId,
                    "withdrawTheAmountFromDisposalAccountInAMS");
		} catch (JAXBException | JsonProcessingException e) {
			log.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}

        return null;
    }
}