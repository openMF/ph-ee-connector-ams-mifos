package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.mifos.connector.ams.fineract.Config;
import org.mifos.connector.ams.fineract.ConfigFactory;
import org.mifos.connector.ams.mapstruct.Pain001Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.BatchItemBuilder;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionBody;
import org.mifos.connector.ams.zeebe.workers.utils.DtSavingsTransactionDetails;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import iso.std.iso._20022.tech.json.camt_053_001.ReportEntry10;
import iso.std.iso._20022.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;

@Component
public class TransferNonTransactionalFeeInAmsWorker extends AbstractMoneyInOutWorker {
	
	@Autowired
	private Pain001Camt053Mapper camt053Mapper;
	
	@Value("${fineract.incoming-money-api}")
	protected String incomingMoneyApi;
	
	@Autowired
    private ConfigFactory paymentTypeConfigFactory;
	
	@Autowired
	private BatchItemBuilder batchItemBuilder;
	
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
			@Variable String originalPain001,
			@Variable String debtorIban) throws Exception {
		String disposalAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "withdrawal");
		Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
		logger.debug("Got payment scheme {}", paymentScheme);
		String transactionDate = LocalDate.now().format(PATTERN);
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.setSerializationInclusion(Include.NON_NULL);
		batchItemBuilder.tenantId(tenantIdentifier);
		logger.debug("Got category purpose code {}", categoryPurpose);
		
		try {
			MDC.put("internalCorrelationId", internalCorrelationId);
			Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = objectMapper.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
			
			String withdrawNonTxFeeDisposalOperation = "transferToConversionAccountInAms.DisposalAccount.WithdrawNonTransactionalFee";
			String withdrawNonTxDisposalConfigOperationKey = String.format("%s.%s.%s", paymentScheme, categoryPurpose, withdrawNonTxFeeDisposalOperation);
			Integer paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawNonTxDisposalConfigOperationKey);
			String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawNonTxDisposalConfigOperationKey);
			logger.debug("Looking up {}, got payment type id {}", withdrawNonTxDisposalConfigOperationKey, paymentTypeId);
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
			
			ReportEntry10 convertedcamt053 = camt053Mapper.toCamt053Entry(pain001.getDocument());
			String camt053Entry = objectMapper.writeValueAsString(convertedcamt053);
			
			String camt053RelativeUrl = "datatables/transaction_details/$.resourceId";
			
			DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
					internalCorrelationId,
					camt053Entry,
					debtorIban,
					paymentTypeCode,
					transactionGroupId,
					categoryPurpose);
			
			String camt053Body = objectMapper.writeValueAsString(td);

			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
			
			
			String depositNonTxFeeOperation = "transferToConversionAccountInAms.ConversionAccount.DepositNonTransactionalFee";
			String depositNonTxFeeConfigOperationKey = String.format("%s.%s.%s", paymentScheme, categoryPurpose, depositNonTxFeeOperation);
			paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(depositNonTxFeeConfigOperationKey);
			paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(depositNonTxFeeConfigOperationKey);
			logger.debug("Looking up {}, got payment type id {}", depositNonTxFeeConfigOperationKey, paymentTypeId);
			body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			bodyItem = objectMapper.writeValueAsString(body);
			
			String conversionAccountDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "deposit");
			
			batchItemBuilder.add(items, conversionAccountDepositRelativeUrl, bodyItem, false);
			
			td = new DtSavingsTransactionDetails(
					internalCorrelationId,
					camt053Entry,
					debtorIban,
					paymentTypeCode,
					transactionGroupId,
					categoryPurpose);
			
			camt053Body = objectMapper.writeValueAsString(td);

			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
			
			
			String withdrawNonTxFeeConversionOperation = "transferToConversionAccountInAms.ConversionAccount.WithdrawNonTransactionalFee";
			String withdrawNonTxFeeConversionConfigOperationKey = String.format("%s.%s.%s", paymentScheme, categoryPurpose, withdrawNonTxFeeConversionOperation);
			paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawNonTxFeeConversionConfigOperationKey);
			paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawNonTxFeeConversionConfigOperationKey);
			logger.debug("Looking up {}, got payment type id {}", withdrawNonTxFeeConversionConfigOperationKey, paymentTypeId);
			body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			bodyItem = objectMapper.writeValueAsString(body);
			
			String conversionAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");
			
			batchItemBuilder.add(items, conversionAccountWithdrawRelativeUrl, bodyItem, false);
			
			td = new DtSavingsTransactionDetails(
					internalCorrelationId,
					camt053Entry,
					debtorIban,
					paymentTypeCode,
					transactionGroupId,
					categoryPurpose);
			
			camt053Body = objectMapper.writeValueAsString(td);

			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
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
