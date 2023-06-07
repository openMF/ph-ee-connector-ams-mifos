package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.mifos.connector.ams.fineract.PaymentTypeConfig;
import org.mifos.connector.ams.fineract.PaymentTypeConfigFactory;
import org.mifos.connector.ams.mapstruct.Pain001Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.BatchItemBuilder;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionBody;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionDetails;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import iso.std.iso._20022.tech.json.camt_053_001.BankToCustomerStatementV08;
import iso.std.iso._20022.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;

@Component
public class TransferNonTransactionalFeeInAmsWorker extends AbstractMoneyInOutWorker {
	
	@Autowired
	private Pain001Camt053Mapper camt053Mapper;
	
	@Value("${fineract.incoming-money-api}")
	protected String incomingMoneyApi;
	
	@Value("${fineract.auth-token}")
	private String authToken;
	
	@Autowired
    private PaymentTypeConfigFactory paymentTypeConfigFactory;
	
	private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);

	@JobWorker
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
			@Variable String originalPain001) throws Exception {
		String disposalAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "withdrawal");
		PaymentTypeConfig paymentTypeConfig = paymentTypeConfigFactory.getPaymentTypeConfig(tenantIdentifier);
		logger.debug("Got payment scheme {}", paymentScheme);
		String transactionDate = LocalDate.now().format(PATTERN);
		ObjectMapper objectMapper = new ObjectMapper();
		BatchItemBuilder biBuilder = new BatchItemBuilder(tenantIdentifier);
		logger.debug("Got category purpose code {}", categoryPurpose);
		
		try {
			MDC.put("internalCorrelationId", internalCorrelationId);
			Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = objectMapper.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
			
			Integer paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s.%s", paymentScheme, categoryPurpose, "transferToConversionAccountInAms.DisposalAccount.WithdrawNonTransactionalFee"));
			logger.debug("Looking up {}, got payment type id {}", String.format("%s.%s.%s", paymentScheme, categoryPurpose, "transferToConversionAccountInAms.DisposalAccount.WithdrawNonTransactionalFee"), paymentTypeId);
			TransactionBody body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			String bodyItem = objectMapper.writeValueAsString(body);
			
			List<TransactionItem> items = new ArrayList<>();
			
			biBuilder.add(items, disposalAccountWithdrawRelativeUrl, bodyItem, false);
			
			BankToCustomerStatementV08 convertedcamt053 = camt053Mapper.toCamt053(pain001.getDocument());
			String camt053 = objectMapper.writeValueAsString(convertedcamt053);
			
			String camt053RelativeUrl = String.format("datatables/transaction_details/%d", disposalAccountAmsId);
			
			TransactionDetails td = new TransactionDetails(
					"$.resourceId",
					internalCorrelationId,
					camt053,
					transactionGroupId,
					categoryPurpose);
			
			String camt053Body = objectMapper.writeValueAsString(td);

			biBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
			
			
			paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s.%s", paymentScheme, categoryPurpose, "transferToConversionAccountInAms.ConversionAccount.DepositNonTransactionalFee"));
			logger.debug("Looking up {}, got payment type id {}", String.format("%s.%s.%s", paymentScheme, categoryPurpose, "transferToConversionAccountInAms.ConversionAccount.DepositNonTransactionalFee"), paymentTypeId);
			body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			bodyItem = objectMapper.writeValueAsString(body);
			
			String conversionAccountDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "deposit");
			
			biBuilder.add(items, conversionAccountDepositRelativeUrl, bodyItem, false);
			
			camt053RelativeUrl = String.format("datatables/transaction_details/%d", conversionAccountAmsId);
			
			td = new TransactionDetails(
					"$.resourceId",
					internalCorrelationId,
					camt053,
					transactionGroupId,
					categoryPurpose);
			
			camt053Body = objectMapper.writeValueAsString(td);

			biBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
			
			
			paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s.%s", paymentScheme, categoryPurpose, "transferToConversionAccountInAms.ConversionAccount.WithdrawNonTransactionalFee"));
			logger.debug("Looking up {}, got payment type id {}", String.format("%s.%s.%s", paymentScheme, categoryPurpose, "transferToConversionAccountInAms.ConversionAccount.WithdrawNonTransactionalFee"), paymentTypeId);
			body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			bodyItem = objectMapper.writeValueAsString(body);
			
			String conversionAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");
			
			biBuilder.add(items, conversionAccountWithdrawRelativeUrl, bodyItem, false);
			
			camt053RelativeUrl = String.format("datatables/transaction_details/%d", conversionAccountAmsId);
			
			td = new TransactionDetails(
					"$.resourceId",
					internalCorrelationId,
					camt053,
					transactionGroupId,
					categoryPurpose);
			
			camt053Body = objectMapper.writeValueAsString(td);

			biBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
			logger.debug("Attempting to send {}", objectMapper.writeValueAsString(items));
			
			doBatch(items, tenantIdentifier, internalCorrelationId);
			
			return Map.of("transactionDate", transactionDate);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new ZeebeBpmnError("Error_InsufficientFunds", e.getMessage());
		} finally {
			MDC.remove("internalCorrelationId");
		}
	}
}
