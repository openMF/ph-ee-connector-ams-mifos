package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.io.InputStream;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import iso.std.iso._20022.tech.json.camt_053_001.BankToCustomerStatementV08;
import iso.std.iso._20022.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;

@Component
public class TransferToConversionAccountInAmsWorker extends AbstractMoneyInOutWorker {
	
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
			@Variable String tenantIdentifier) throws Exception {
		String transactionDate = LocalDate.now().format(PATTERN);
		logger.debug("Debtor exchange worker starting");
		MDC.put("internalCorrelationId", internalCorrelationId);
		try {
			ObjectMapper om = new ObjectMapper();
			Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = om.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
			
			try {
				logger.info(">>>>>>>>>>>>>>>>>> Validating incoming pain.001 <<<<<<<<<<<<<<<<");
				InputStream resource = new ClassPathResource("/services/json-schema/pain.001.001/pain.001.001.10-CustomerCreditTransferInitiationV10.Message.schema.json").getInputStream();
				JsonSchemaFactory sf = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
			
				JsonNode json = om.readTree(originalPain001);
				JsonSchema schema = sf.getSchema(resource);
				Set<ValidationMessage> validationResult = schema.validate(json);
			
				if (validationResult.isEmpty()) {
					logger.info(">>>>>>>>>>>>>>>> pain.001 validation successful <<<<<<<<<<<<<<<");
				} else {
					logger.error(validationResult.toString());
				}
			} catch (Exception e) {
				logger.warn("Unable to validate pain.001: {}", e.getMessage());
			}
			
			
			logger.debug("Debtor exchange worker incoming variables:");
		
			logger.debug("Withdrawing amount {} from disposal account {}", amount, disposalAccountAmsId);
			
			BatchItemBuilder biBuilder = new BatchItemBuilder(tenantIdentifier);
			
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
			
			String bodyItem = om.writeValueAsString(body);
			
			List<TransactionItem> items = new ArrayList<>();
			
			biBuilder.add(items, disposalAccountWithdrawRelativeUrl, bodyItem, false);
			
			BankToCustomerStatementV08 convertedCamt053 = camt053Mapper.toCamt053(pain001.getDocument());
			String camt053 = om.writeValueAsString(convertedCamt053);
			
			String camt053RelativeUrl = String.format("datatables/transaction_details/%d", disposalAccountAmsId);
			
			TransactionDetails td = new TransactionDetails(
					"$.resourceId",
					internalCorrelationId,
					camt053,
					transactionGroupId,
					transactionCategoryPurposeCode);
			
			String camt053Body = om.writeValueAsString(td);

			biBuilder.add(items, camt053RelativeUrl, camt053Body, true);

			if (!BigDecimal.ZERO.equals(transactionFeeAmount)) {
				logger.debug("Withdrawing fee {} from disposal account {}", transactionFeeAmount, disposalAccountAmsId);
					Integer withdrawTransactionFeePaymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "transferToConversionAccountInAms.DisposalAccount.WithdrawTransactionFee"));
					
					TransactionBody withdrawTransactionFeeBody = new TransactionBody(
							transactionDate,
							transactionFeeAmount,
							withdrawTransactionFeePaymentTypeId,
							"",
							FORMAT,
							locale);
					
					String withdrawTransactionFeeBodyItem = om.writeValueAsString(withdrawTransactionFeeBody);
					biBuilder.add(items, disposalAccountWithdrawRelativeUrl, withdrawTransactionFeeBodyItem, false);
					
					td = new TransactionDetails(
							"$.resourceId",
							internalCorrelationId,
							camt053,
							transactionGroupId,
							transactionFeeCategoryPurposeCode);
					
					camt053Body = om.writeValueAsString(td);
					biBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			}
			
			logger.info("Depositing amount {} to conversion account {}", amount, conversionAccountAmsId);
			
			String conversionAccountDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "deposit");
			
			Integer conversionAccountDepositAmountPaymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "transferToConversionAccountInAms.ConversionAccount.DepositTransactionAmount"));
			
			TransactionBody depositAmountBody = new TransactionBody(
					transactionDate,
					amount,
					conversionAccountDepositAmountPaymentTypeId,
					"",
					FORMAT,
					locale);
			
			String depositAmountBodyItem = om.writeValueAsString(depositAmountBody);
			
			biBuilder.add(items, conversionAccountDepositRelativeUrl, depositAmountBodyItem, false);
			
			camt053RelativeUrl = String.format("datatables/transaction_details/%d", conversionAccountAmsId);
			td = new TransactionDetails(
					"$.resourceId",
					internalCorrelationId,
					camt053,
					transactionGroupId,
					transactionCategoryPurposeCode);
			
			camt053Body = om.writeValueAsString(td);
			biBuilder.add(items, camt053RelativeUrl, camt053Body, true);
		
			
			if (!BigDecimal.ZERO.equals(transactionFeeAmount)) {
				logger.debug("Depositing fee {} to conversion account {}", transactionFeeAmount, conversionAccountAmsId);
				Integer depositTransactionFeePaymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "transferToConversionAccountInAms.ConversionAccount.DepositTransactionFee"));
				
				TransactionBody depositTransactionFeeBody = new TransactionBody(
						transactionDate,
						transactionFeeAmount,
						depositTransactionFeePaymentTypeId,
						"",
						FORMAT,
						locale);
				
				String depositTransactionFeeBodyItem = om.writeValueAsString(depositTransactionFeeBody);
				biBuilder.add(items, conversionAccountDepositRelativeUrl, depositTransactionFeeBodyItem, false);
				
				td = new TransactionDetails(
						"$.resourceId",
						internalCorrelationId,
						camt053,
						transactionGroupId,
						transactionFeeCategoryPurposeCode);
				
				camt053Body = om.writeValueAsString(td);
				biBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			}
			
			doBatch(items, tenantIdentifier, internalCorrelationId);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new ZeebeBpmnError("Error_InsufficientFunds", e.getMessage());
		} finally {
			MDC.remove("internalCorrelationId");
		}
	}
}
