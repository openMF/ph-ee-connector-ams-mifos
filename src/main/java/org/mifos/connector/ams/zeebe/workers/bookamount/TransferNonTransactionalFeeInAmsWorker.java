package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.mifos.connector.ams.fineract.PaymentTypeConfig;
import org.mifos.connector.ams.fineract.PaymentTypeConfigFactory;
import org.mifos.connector.ams.zeebe.workers.utils.BatchItemBuilder;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionBody;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionDetails;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;

public class TransferNonTransactionalFeeInAmsWorker extends AbstractMoneyInOutWorker {
	
//	@Autowired
//	private Pacs008Camt052Mapper camt052Mapper;
	
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
		String transactionDate = LocalDate.now().format(PATTERN);
		BigDecimal amount = new BigDecimal(variables.get("amount").toString());
		ObjectMapper om = new ObjectMapper();
		BatchItemBuilder biBuilder = new BatchItemBuilder(tenantId);
		String internalCorrelationId = (String) variables.get("internalCorrelationId");
		String categoryPurposeCode = (String) variables.get("categoryPurposeCode");
		
		try {
			MDC.put("internalCorrelationId", internalCorrelationId);
			Integer paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s.%s", paymentScheme, categoryPurposeCode, "transferToConversionAccountInAms.DisposalAccount.WithdrawNonTransactionalFee"));
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
			
			// BankToCustomerAccountReportV08 convertedCamt052 = camt052Mapper.toCamt052(pain001.getDocument());
			// String camt052 = om.writeValueAsString(convertedCamt052);
			
			String camt052RelativeUrl = String.format("datatables/transaction_details/%d", disposalAccountAmsId);
			
			TransactionDetails td = new TransactionDetails(
					"$.resourceId",
					internalCorrelationId,
					// camt052
					null);
			
			String camt052Body = om.writeValueAsString(td);

			biBuilder.add(items, camt052RelativeUrl, camt052Body, true);
			
			
			
			paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s.%s", paymentScheme, categoryPurposeCode, "transferToConversionAccountInAms.ConversionAccount.DepositNonTransactionalFee"));
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
					//camt052
					null);
			
			camt052Body = om.writeValueAsString(td);

			biBuilder.add(items, camt052RelativeUrl, camt052Body, true);
			
			
			
			paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s.%s", paymentScheme, categoryPurposeCode, "transferToConversionAccountInAms.ConversionAccount.WithdrawNonTransactionalFee"));
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
					//camt052
					null);
			
			camt052Body = om.writeValueAsString(td);

			biBuilder.add(items, camt052RelativeUrl, camt052Body, true);
			
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
