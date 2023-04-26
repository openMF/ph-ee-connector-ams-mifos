package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import iso.std.iso._20022.tech.json.camt_052_001.BankToCustomerAccountReportV08;
import iso.std.iso._20022.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;

@Component
public class TransferNonTransactionalFeeInAmsWorker extends AbstractMoneyInOutWorker {
	
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
		Map<String, Object> variables = activatedJob.getVariablesAsMap();
		Integer conversionAccountAmsId = (Integer) variables.get("conversionAccountAmsId");
		Integer disposalAccountAmsId = (Integer) variables.get("disposalAccountAmsId");
		String disposalAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "withdrawal");
		String tenantId = (String) variables.get("tenantIdentifier");
		PaymentTypeConfig paymentTypeConfig = paymentTypeConfigFactory.getPaymentTypeConfig(tenantId);
		String paymentScheme = (String) variables.get("paymentScheme");
		logger.debug("Got payment scheme {}", paymentScheme);
		String transactionDate = LocalDate.now().format(PATTERN);
		BigDecimal amount = new BigDecimal(variables.get("amount").toString());
		ObjectMapper om = new ObjectMapper();
		BatchItemBuilder biBuilder = new BatchItemBuilder(tenantId);
		String internalCorrelationId = (String) variables.get("internalCorrelationId");
		String transactionGroupId = (String) variables.get("transactionGroupId");
		String categoryPurpose = (String) variables.get("categoryPurpose");
		logger.debug("Got category purpose code {}", categoryPurpose);
		
		try {
			MDC.put("internalCorrelationId", internalCorrelationId);
			String originalPain001 = (String) variables.get("originalPain001");
			Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = om.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
			
			Integer paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s.%s", paymentScheme, categoryPurpose, "transferToConversionAccountInAms.DisposalAccount.WithdrawNonTransactionalFee"));
			logger.debug("Looking up {}, got payment type id {}", String.format("%s.%s.%s", paymentScheme, categoryPurpose, "transferToConversionAccountInAms.DisposalAccount.WithdrawNonTransactionalFee"), paymentTypeId);
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
					camt052,
					transactionGroupId,
					categoryPurpose);
			
			String camt052Body = om.writeValueAsString(td);

			biBuilder.add(items, camt052RelativeUrl, camt052Body, true);
			
			
			
			paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s.%s", paymentScheme, categoryPurpose, "transferToConversionAccountInAms.ConversionAccount.DepositNonTransactionalFee"));
			logger.debug("Looking up {}, got payment type id {}", String.format("%s.%s.%s", paymentScheme, categoryPurpose, "transferToConversionAccountInAms.ConversionAccount.DepositNonTransactionalFee"), paymentTypeId);
			body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			bodyItem = om.writeValueAsString(body);
			
			String conversionAccountDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "deposit");
			
			biBuilder.add(items, conversionAccountDepositRelativeUrl, bodyItem, false);
			
			camt052RelativeUrl = String.format("datatables/transaction_details/%d", conversionAccountAmsId);
			
			td = new TransactionDetails(
					"$.resourceId",
					internalCorrelationId,
					camt052,
					transactionGroupId,
					categoryPurpose);
			
			camt052Body = om.writeValueAsString(td);

			biBuilder.add(items, camt052RelativeUrl, camt052Body, true);
			
			
			
			paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s.%s", paymentScheme, categoryPurpose, "transferToConversionAccountInAms.ConversionAccount.WithdrawNonTransactionalFee"));
			logger.debug("Looking up {}, got payment type id {}", String.format("%s.%s.%s", paymentScheme, categoryPurpose, "transferToConversionAccountInAms.ConversionAccount.WithdrawNonTransactionalFee"), paymentTypeId);
			body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			bodyItem = om.writeValueAsString(body);
			
			String conversionAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");
			
			biBuilder.add(items, conversionAccountWithdrawRelativeUrl, bodyItem, false);
			
			camt052RelativeUrl = String.format("datatables/transaction_details/%d", conversionAccountAmsId);
			
			td = new TransactionDetails(
					"$.resourceId",
					internalCorrelationId,
					camt052,
					transactionGroupId,
					categoryPurpose);
			
			camt052Body = om.writeValueAsString(td);

			biBuilder.add(items, camt052RelativeUrl, camt052Body, true);
			
			logger.debug("Attempting to send {}", om.writeValueAsString(items));
			
			doBatch(items, tenantId, internalCorrelationId);
			
			jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
		} catch (Exception e) {
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
