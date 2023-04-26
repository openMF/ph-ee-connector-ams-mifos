package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
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
public class BookOnConversionAccountInAmsWorker extends AbstractMoneyInOutWorker {
	
	@Autowired
	private Pain001Camt052Mapper camt052Mapper;
	
	@Value("${fineract.incoming-money-api}")
	protected String incomingMoneyApi;
	
	@Value("${fineract.auth-token}")
	private String authToken;
	
	@Autowired
    private PaymentTypeConfigFactory paymentTypeConfigFactory;
	
	@Override
	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		Map<String, Object> variables = activatedJob.getVariablesAsMap();
		
		String originalPain001 = (String) variables.get("originalPain001");
		ObjectMapper om = new ObjectMapper();
		Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = om.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
		
		String internalCorrelationId = (String) variables.get("internalCorrelationId");
		MDC.put("internalCorrelationId", internalCorrelationId);
		
		String paymentScheme = (String) variables.get("paymentScheme");
		
		String transactionDate = (String) variables.get("transactionDate");
		
		Integer conversionAccountAmsId = (Integer) variables.get("conversionAccountAmsId");
		
		String transactionGroupId = (String) variables.get("transactionGroupId");
		String transactionCategoryPurposeCode = (String) variables.get("transactionCategoryPurposeCode");
		String transactionFeeCategoryPurposeCode = (String) variables.get("transactionFeeCategoryPurposeCode");
		
		logger.info("Starting book debit on fiat account worker");
		
		Object amount = variables.get("amount");
		Object feeAmount = variables.get("transactionFeeAmount");
		BigDecimal fee = null;
		if (feeAmount != null) {
			fee = new BigDecimal(feeAmount.toString());
		}
		
		String tenantId = (String) variables.get("tenantIdentifier");
		logger.info("Withdrawing amount {} from conversion account {} of tenant {}", amount, conversionAccountAmsId, tenantId);
		
		BatchItemBuilder biBuilder = new BatchItemBuilder(tenantId);
		
		String conversionAccountWithdrawalRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");
		
		PaymentTypeConfig paymentTypeConfig = paymentTypeConfigFactory.getPaymentTypeConfig(tenantId);
		Integer paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "bookOnConversionAccountInAms.ConversionAccount.WithdrawTransactionAmount"));
		
		TransactionBody body = new TransactionBody(
				transactionDate,
				amount,
				paymentTypeId,
				"",
				FORMAT,
				locale);
		
		String bodyItem = om.writeValueAsString(body);
		
		List<TransactionItem> items = new ArrayList<>();
		
		biBuilder.add(items, conversionAccountWithdrawalRelativeUrl, bodyItem, false);
	
		BankToCustomerAccountReportV08 convertedCamt052 = camt052Mapper.toCamt052(pain001.getDocument());
		String camt052 = om.writeValueAsString(convertedCamt052);
		
		String camt052RelativeUrl = String.format("datatables/transaction_details/%d", conversionAccountAmsId);
		
		TransactionDetails td = new TransactionDetails(
				"$.resourceId",
				internalCorrelationId,
				camt052,
				transactionGroupId,
				transactionCategoryPurposeCode);
		
		String camt052Body = om.writeValueAsString(td);

		biBuilder.add(items, camt052RelativeUrl, camt052Body, true);
		
		if (!BigDecimal.ZERO.equals(fee)) {
				
			logger.info("Withdrawing fee {} from conversion account {}", fee, conversionAccountAmsId);
			
			paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "bookOnConversionAccountInAms.ConversionAccount.WithdrawTransactionFee"));
			
			body = new TransactionBody(
					transactionDate,
					fee,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			bodyItem = om.writeValueAsString(body);
			
			biBuilder.add(items, conversionAccountWithdrawalRelativeUrl, bodyItem, false);
		
			td = new TransactionDetails(
					"$.resourceId",
					internalCorrelationId,
					camt052,
					transactionGroupId,
					transactionFeeCategoryPurposeCode);
			camt052Body = om.writeValueAsString(td);
			biBuilder.add(items, camt052RelativeUrl, camt052Body, true);
		}
		
		doBatch(items, tenantId, internalCorrelationId);
		
		jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
			
		logger.info("Book debit on fiat account has finished  successfully");
		
		MDC.remove("internalCorrelationId");
	}
}
