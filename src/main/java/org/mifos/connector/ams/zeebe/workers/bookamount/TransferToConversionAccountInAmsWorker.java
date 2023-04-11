package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.io.InputStream;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mifos.connector.ams.fineract.PaymentTypeConfig;
import org.mifos.connector.ams.fineract.PaymentTypeConfigFactory;
import org.mifos.connector.ams.mapstruct.Pain001Camt052Mapper;
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
import iso.std.iso._20022.tech.json.camt_052_001.BankToCustomerAccountReportV08;
import iso.std.iso._20022.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;

@Component
public class TransferToConversionAccountInAmsWorker extends AbstractMoneyInOutWorker {
	
	@Autowired
	private Pain001Camt052Mapper camt052Mapper;
	
	@Value("${fineract.incoming-money-api}")
	protected String incomingMoneyApi;
	
	@Value("${fineract.auth-token}")
	private String authToken;
	
	@Autowired
    private PaymentTypeConfigFactory paymentTypeConfigFactory;
	
	private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);

	@Override
	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		String transactionDate = LocalDate.now().format(PATTERN);
		logger.error("Debtor exchange worker starting");
		Object amount = new Object();
		Integer disposalAccountAmsId = null;
		try {
			Map<String, Object> variables = activatedJob.getVariablesAsMap();
			
			String originalPain001 = (String) variables.get("originalPain001");
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
			
			
			String internalCorrelationId = (String) variables.get("internalCorrelationId");
			MDC.put("internalCorrelationId", internalCorrelationId);
			
			String paymentScheme = (String) variables.get("paymentScheme");
			
			amount = variables.get("amount");
			
			Object feeAmount = variables.get("transactionFeeAmount");
			BigDecimal fee = null;
			if (feeAmount != null) {
				fee = new BigDecimal(feeAmount.toString());
			}
			
			logger.error("Debtor exchange worker incoming variables:");
			variables.entrySet().forEach(e -> logger.error("{}: {} of type {}", e.getKey(), e.getValue(), e.getValue().getClass()));
		
			disposalAccountAmsId = (Integer) variables.get("disposalAccountAmsId");
			Integer conversionAccountAmsId = (Integer) variables.get("conversionAccountAmsId");
			
			logger.info("Withdrawing amount {} from disposal account {}", amount, disposalAccountAmsId);
			
			String tenantId = (String) variables.get("tenantIdentifier");
			
			BatchItemBuilder biBuilder = new BatchItemBuilder(tenantId);
			
			String disposalAccountWithdrawRelativeUrl = String.format("%s/%d/transactions?command=%s", incomingMoneyApi, disposalAccountAmsId, "withdrawal");
			
			PaymentTypeConfig paymentTypeConfig = paymentTypeConfigFactory.getPaymentTypeConfig(tenantId);
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
			
			BankToCustomerAccountReportV08 convertedCamt052 = camt052Mapper.toCamt052(pain001.getDocument());
			String camt052 = om.writeValueAsString(convertedCamt052);
			
			String camt052RelativeUrl = String.format("datatables/transaction_details/%d", disposalAccountAmsId);
			
			TransactionDetails td = new TransactionDetails(
					"$.resourceId",
					internalCorrelationId,
					camt052);
			
			String camt052Body = om.writeValueAsString(td);

			biBuilder.add(items, camt052RelativeUrl, camt052Body, true);

			if (!fee.equals(BigDecimal.ZERO)) {
				logger.info("Withdrawing fee {} from disposal account {}", fee, disposalAccountAmsId);
					Integer withdrawTransactionFeePaymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "transferToConversionAccountInAms.DisposalAccount.WithdrawTransactionFee"));
					
					TransactionBody withdrawTransactionFeeBody = new TransactionBody(
							transactionDate,
							fee,
							withdrawTransactionFeePaymentTypeId,
							"",
							FORMAT,
							locale);
					
					String withdrawTransactionFeeBodyItem = om.writeValueAsString(withdrawTransactionFeeBody);
					biBuilder.add(items, disposalAccountWithdrawRelativeUrl, withdrawTransactionFeeBodyItem, false);
					
					biBuilder.add(items, camt052RelativeUrl, camt052Body, true);
			}
			
			logger.info("Depositing amount {} to conversion account {}", amount, conversionAccountAmsId);
			
			String conversionAccountDepositRelativeUrl = String.format("%s/%d/transactions?command=%s", incomingMoneyApi, conversionAccountAmsId, "deposit");
			
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
			biBuilder.add(items, camt052RelativeUrl, camt052Body, true);
		
			logger.info("Depositing fee {} to conversion account {}", fee, conversionAccountAmsId);
			
			if (!fee.equals(BigDecimal.ZERO)) {
				Integer depositTransactionFeePaymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "transferToConversionAccountInAms.ConversionAccount.DepositTransactionFee"));
				
				TransactionBody depositTransactionFeeBody = new TransactionBody(
						transactionDate,
						fee,
						depositTransactionFeePaymentTypeId,
						"",
						FORMAT,
						locale);
				
				String depositTransactionFeeBodyItem = om.writeValueAsString(depositTransactionFeeBody);
				biBuilder.add(items, conversionAccountDepositRelativeUrl, depositTransactionFeeBodyItem, false);
				
				biBuilder.add(items, camt052RelativeUrl, camt052Body, true);
			}
			
			doBatch(items, tenantId, internalCorrelationId);
		
			jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
		} catch (Exception e) {
			logger.warn("Fee to conversion account failed");
			logger.error(e.getMessage(), e);
			jobClient
					.newThrowErrorCommand(activatedJob.getKey())
					.errorCode("Error_InsufficientFunds")
					.errorMessage(e.getMessage())
					.send();
		} finally {
			MDC.remove("internalCorrelationId");
		}
	}
}
