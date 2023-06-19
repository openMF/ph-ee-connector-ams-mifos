package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import jakarta.xml.bind.JAXBException;

import org.mifos.connector.ams.fineract.PaymentTypeConfig;
import org.mifos.connector.ams.fineract.PaymentTypeConfigFactory;
import org.mifos.connector.ams.mapstruct.Pain001Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.accountdetails.AbstractAmsWorker;
import org.mifos.connector.ams.zeebe.workers.utils.AuthTokenHelper;
import org.mifos.connector.ams.zeebe.workers.utils.BatchItemBuilder;
import org.mifos.connector.ams.zeebe.workers.utils.JAXBUtils;
import org.mifos.connector.ams.zeebe.workers.utils.JsonSchemaValidator;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionBody;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionDetails;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.ValidationMessage;

import hu.dpc.rt.utils.converter.Camt056ToCamt053Converter;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import iso.std.iso._20022.tech.json.camt_053_001.BankToCustomerStatementV08;
import iso.std.iso._20022.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;
import iso.std.iso._20022.tech.xsd.camt_056_001.PaymentTransactionInformation31;

@Component
public class TransferToConversionAccountInAmsWorker extends AbstractMoneyInOutWorker {
	
	@Autowired
	private Pain001Camt053Mapper camt053Mapper;
	
	@Value("${fineract.incoming-money-api}")
	protected String incomingMoneyApi;
	
	@Autowired
    private PaymentTypeConfigFactory paymentTypeConfigFactory;
	
	@Autowired
	private JsonSchemaValidator validator;
	
	@Autowired
	private JAXBUtils jaxbUtils;
	
	@Autowired
	private AuthTokenHelper authTokenHelper;
	
	@Autowired
	private BatchItemBuilder batchItemBuilder;
	
	private final ObjectMapper objectMapper = new ObjectMapper();
	
	private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);

	@SuppressWarnings("unchecked")
	@JobWorker
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
			@Variable String iban) {
		String transactionDate = LocalDate.now().format(PATTERN);
		logger.debug("Debtor exchange worker starting");
		MDC.put("internalCorrelationId", internalCorrelationId);
		try {
			Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = objectMapper.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
			
			try {
				logger.info(">>>>>>>>>>>>>>>>>> Validating incoming pain.001 <<<<<<<<<<<<<<<<");
				
				Set<ValidationMessage> validationResult = validator.validate(originalPain001);
			
				if (validationResult.isEmpty()) {
					logger.info(">>>>>>>>>>>>>>>> pain.001 validation successful <<<<<<<<<<<<<<<");
				} else {
					logger.error(validationResult.toString());
				}
			} catch (JsonProcessingException e) {
				logger.warn("Unable to validate pain.001: {}", e.getMessage());
			}
			
			
			logger.debug("Debtor exchange worker incoming variables:");
		
			logger.debug("Withdrawing amount {} from disposal account {}", amount, disposalAccountAmsId);
			
			boolean hasFee = !BigDecimal.ZERO.equals(transactionFeeAmount);

			PaymentTypeConfig paymentTypeConfig = paymentTypeConfigFactory.getPaymentTypeConfig(tenantIdentifier);
			
			Integer outHoldReasonId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "outHoldReasonId"));
			var holdResponse = hold(outHoldReasonId, transactionDate, hasFee ? amount.add(transactionFeeAmount) : amount, disposalAccountAmsId, tenantIdentifier).getBody();
			Integer lastHoldTransactionId = (Integer) ((LinkedHashMap<String, Object>) holdResponse).get("resourceId");
			
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
			
			batchItemBuilder.tenantId(tenantIdentifier);
			
			List<TransactionItem> items = new ArrayList<>();
			
			String releaseTransactionUrl = String.format("%s%d/transactions/%d?command=releaseAmount", incomingMoneyApi.substring(1), disposalAccountAmsId, lastHoldTransactionId);
			batchItemBuilder.add(items, releaseTransactionUrl, "", false);
			
			String disposalAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "withdrawal");
			
			withdrawAmount(amount, paymentScheme, transactionDate, objectMapper, paymentTypeConfig, batchItemBuilder, items,
					disposalAccountWithdrawRelativeUrl);
			
			BankToCustomerStatementV08 convertedCamt053 = camt053Mapper.toCamt053(pain001.getDocument());
			String camt053 = objectMapper.writeValueAsString(convertedCamt053);
			
			String camt053RelativeUrl = String.format("datatables/transaction_details/%d", disposalAccountAmsId);
			
			addDetails(transactionGroupId, transactionCategoryPurposeCode,
					internalCorrelationId, objectMapper, batchItemBuilder, items, camt053, camt053RelativeUrl);

			if (hasFee) {
				logger.debug("Withdrawing fee {} from disposal account {}", transactionFeeAmount, disposalAccountAmsId);
					withdrawFee(transactionFeeAmount, paymentScheme, transactionDate, objectMapper, paymentTypeConfig, batchItemBuilder,
							items, disposalAccountWithdrawRelativeUrl);
					
					addDetails(transactionGroupId, transactionFeeCategoryPurposeCode, internalCorrelationId, objectMapper,
							batchItemBuilder, items, camt053, camt053RelativeUrl);
			}
			
			logger.info("Depositing amount {} to conversion account {}", amount, conversionAccountAmsId);
			
			String conversionAccountDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "deposit");
			camt053RelativeUrl = String.format("datatables/transaction_details/%d", conversionAccountAmsId);
			
			depositAmount(amount, paymentScheme, transactionDate, objectMapper, paymentTypeConfig, batchItemBuilder, items,
					conversionAccountDepositRelativeUrl);
			
			addDetails(transactionGroupId, transactionCategoryPurposeCode, internalCorrelationId, objectMapper, batchItemBuilder, items,
					camt053, camt053RelativeUrl);
		
			
			if (hasFee) {
				logger.debug("Depositing fee {} to conversion account {}", transactionFeeAmount, conversionAccountAmsId);
				depositFee(transactionFeeAmount, paymentScheme, transactionDate, objectMapper, paymentTypeConfig, batchItemBuilder, items,
						conversionAccountDepositRelativeUrl);
				
				addDetails(transactionGroupId, transactionFeeCategoryPurposeCode, internalCorrelationId, objectMapper, batchItemBuilder,
						items, camt053, camt053RelativeUrl);
			}
			
			doBatch(items, tenantIdentifier, internalCorrelationId);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new ZeebeBpmnError("Error_InsufficientFunds", e.getMessage());
		} finally {
			MDC.remove("internalCorrelationId");
		}
	}

	private void withdrawAmount(BigDecimal amount, 
			String paymentScheme, 
			String transactionDate, 
			ObjectMapper om,
			PaymentTypeConfig paymentTypeConfig, 
			BatchItemBuilder batchItemBuilder, 
			List<TransactionItem> items,
			String disposalAccountWithdrawRelativeUrl) throws JsonProcessingException {
		addExchange(amount, paymentScheme, transactionDate, om, paymentTypeConfig, batchItemBuilder, items, disposalAccountWithdrawRelativeUrl, "transferToConversionAccountInAms.DisposalAccount.WithdrawTransactionAmount");
	}

	private void withdrawFee(BigDecimal transactionFeeAmount, 
			String paymentScheme, 
			String transactionDate,
			ObjectMapper om, 
			PaymentTypeConfig paymentTypeConfig, 
			BatchItemBuilder batchItemBuilder,
			List<TransactionItem> items, 
			String disposalAccountWithdrawRelativeUrl) throws JsonProcessingException {
		addExchange(transactionFeeAmount, paymentScheme, transactionDate, om, paymentTypeConfig, batchItemBuilder, items, disposalAccountWithdrawRelativeUrl, "transferToConversionAccountInAms.DisposalAccount.WithdrawTransactionFee");
	}
	
	private void addExchange(BigDecimal amount, 
			String paymentScheme, 
			String transactionDate, 
			ObjectMapper om, 
			PaymentTypeConfig paymentTypeConfig, 
			BatchItemBuilder batchItemBuilder, 
			List<TransactionItem> items, 
			String relativeUrl, 
			String paymentTypeOperation) throws JsonProcessingException {
		Integer paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, paymentTypeOperation));
		
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

	private void depositAmount(BigDecimal amount, 
			String paymentScheme, 
			String transactionDate, 
			ObjectMapper om,
			PaymentTypeConfig paymentTypeConfig, 
			BatchItemBuilder batchItemBuilder, 
			List<TransactionItem> items,
			String conversionAccountDepositRelativeUrl) throws JsonProcessingException {
		addExchange(amount, paymentScheme, transactionDate, om, paymentTypeConfig, batchItemBuilder, items, conversionAccountDepositRelativeUrl, "transferToConversionAccountInAms.ConversionAccount.DepositTransactionAmount");
	}

	private void depositFee(BigDecimal transactionFeeAmount, 
			String paymentScheme, 
			String transactionDate, 
			ObjectMapper om,
			PaymentTypeConfig paymentTypeConfig, 
			BatchItemBuilder batchItemBuilder, 
			List<TransactionItem> items,
			String conversionAccountDepositRelativeUrl) throws JsonProcessingException {
		addExchange(transactionFeeAmount, paymentScheme, transactionDate, om, paymentTypeConfig, batchItemBuilder, items, conversionAccountDepositRelativeUrl, "transferToConversionAccountInAms.ConversionAccount.DepositTransactionFee");
	}

	private void addDetails(String transactionGroupId, 
			String transactionFeeCategoryPurposeCode,
			String internalCorrelationId, 
			ObjectMapper om, 
			BatchItemBuilder batchItemBuilder, 
			List<TransactionItem> items,
			String camt053, 
			String camt053RelativeUrl) throws JsonProcessingException {
		TransactionDetails td = new TransactionDetails(
				"$.resourceId",
				internalCorrelationId,
				camt053,
				transactionGroupId,
				transactionFeeCategoryPurposeCode);
		
		String camt053Body = om.writeValueAsString(td);
		batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
	}
	
	@JobWorker
	public void withdrawTheAmountFromDisposalAccountInAMS(JobClient client,
			ActivatedJob job,
			@Variable BigDecimal amount,
			@Variable Integer conversionAccountAmsId,
			@Variable Integer disposalAccountAmsId,
			@Variable String tenantIdentifier,
			@Variable String paymentScheme,
			@Variable String transactionCategoryPurposeCode,
			@Variable String camt056) {
		
		try {
			String transactionDate = LocalDate.now().format(PATTERN);
			
			logger.debug("Withdrawing amount {} from disposal account {}", amount, disposalAccountAmsId);
			
			batchItemBuilder.tenantId(tenantIdentifier);
			
			String disposalAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "withdrawal");
			
			PaymentTypeConfig paymentTypeConfig = paymentTypeConfigFactory.getPaymentTypeConfig(tenantIdentifier);
			Integer paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "transferToConversionAccountInAms.DisposalAccount.WithdrawTransactionAmount"));
			
			TransactionBody body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			String bodyItem = objectMapper.writeValueAsString(body);
			
			List<TransactionItem> items = new ArrayList<>();
			
			batchItemBuilder.add(items, disposalAccountWithdrawRelativeUrl, bodyItem, false);
		
			iso.std.iso._20022.tech.xsd.camt_056_001.Document document = jaxbUtils.unmarshalCamt056(camt056);
    		Camt056ToCamt053Converter converter = new Camt056ToCamt053Converter();
    		BankToCustomerStatementV08 statement = converter.convert(document);
    		
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
    				.getOrgnlGrpInf()
    				.getOrgnlCreDtTm()
    				.toGregorianCalendar()
    				.toZonedDateTime()
    				.toLocalDate()
    				.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
    		String originalTxId = paymentTransactionInformation
    				.getOrgnlTxId();
    		
    		String internalCorrelationId = String.format("%s_%s_%s", originalDebtorBic, originalCreationDate, originalTxId);
		
			String camt053 = objectMapper.writeValueAsString(statement);
			
			String camt053RelativeUrl = String.format("datatables/transaction_details/%d", disposalAccountAmsId);
			
			TransactionDetails td = new TransactionDetails(
					"$.resourceId",
					internalCorrelationId,
					camt053,
					internalCorrelationId,
					transactionCategoryPurposeCode);
			
			String camt053Body = objectMapper.writeValueAsString(td);
	
			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
			String conversionAccountDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "deposit");
			
			depositAmount(amount, paymentScheme, transactionDate, objectMapper, paymentTypeConfig, batchItemBuilder, items,
					conversionAccountDepositRelativeUrl);
			
			camt053RelativeUrl = String.format("datatables/transaction_details/%d", conversionAccountAmsId);
			
			camt053Body = objectMapper.writeValueAsString(td);
			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
			doBatch(items, tenantIdentifier, internalCorrelationId);
		} catch (JAXBException | JsonProcessingException e) {
			logger.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}
}
