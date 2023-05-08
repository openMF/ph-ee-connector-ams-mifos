package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
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
import iso.std.iso._20022.tech.json.camt_053_001.BankToCustomerStatementV08;
import iso.std.iso._20022.tech.json.pain_001_001.CreditTransferTransaction40;
import iso.std.iso._20022.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;

@Component
public class RevertInAmsWorker extends AbstractMoneyInOutWorker {
	
	@Autowired
	private Pain001Camt053Mapper camt053Mapper;
	
	@Value("${fineract.incoming-money-api}")
	protected String incomingMoneyApi;
	
	@Value("${fineract.auth-token}")
	private String authToken;
	
	@Autowired
    private PaymentTypeConfigFactory paymentTypeConfigFactory;
	
	@Override
	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		Map<String, Object> variables = activatedJob.getVariablesAsMap();
		
		String internalCorrelationId = (String) variables.get("internalCorrelationId");
		MDC.put("internalCorrelationId", internalCorrelationId);
		
		String originalPain001 = (String) variables.get("originalPain001");
		
		Integer conversionAccountAmsId = (Integer) variables.get("conversionAccountAmsId");
		
		String transactionDate = (String) variables.get("transactionDate");

		Integer disposalAccountAmsId = (Integer) variables.get("disposalAccountAmsId");
		
		String paymentScheme = (String) variables.get("paymentScheme");
		
		String transactionGroupId = (String) variables.get("transactionGroupId");
		String transactionCategoryPurposeCode = (String) variables.get("transactionCategoryPurposeCode");
		String transactionFeeCategoryPurposeCode = (String) variables.get("transactionFeeCategoryPurposeCode");
		
		ObjectMapper om = new ObjectMapper();
		Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = om.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
		
		BigDecimal amount = BigDecimal.ZERO;
		BigDecimal fee = variables.get("transactionFeeAmount") == null
						? null
						: new BigDecimal(variables.get("transactionFeeAmount").toString());
		
		List<CreditTransferTransaction40> creditTransferTransactionInformation = pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation();
		
		for (CreditTransferTransaction40 ctti : creditTransferTransactionInformation) {
			amount = new BigDecimal(ctti.getAmount().getInstructedAmount().getAmount().toString());
		}
		
		logger.debug("Withdrawing amount {} from conversion account {}", amount, conversionAccountAmsId);
		
		String tenantId = (String) variables.get("tenantIdentifier");
		
		
		
		
		
		
		BatchItemBuilder biBuilder = new BatchItemBuilder(tenantId);
		
		String conversionAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");
		
		PaymentTypeConfig paymentTypeConfig = paymentTypeConfigFactory.getPaymentTypeConfig(tenantId);
		Integer paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "revertInAms.ConversionAccount.WithdrawTransactionAmount"));
		
		TransactionBody body = new TransactionBody(
				transactionDate,
				amount,
				paymentTypeId,
				"",
				FORMAT,
				locale);
		
		String bodyItem = om.writeValueAsString(body);
		
		List<TransactionItem> items = new ArrayList<>();
		
		biBuilder.add(items, conversionAccountWithdrawRelativeUrl, bodyItem, false);
		
		BankToCustomerStatementV08 convertedcamt053 = camt053Mapper.toCamt053(pain001.getDocument());
		String camt053 = om.writeValueAsString(convertedcamt053);
		
		String camt053RelativeUrl = String.format("datatables/transaction_details/%d", conversionAccountAmsId);
		
		TransactionDetails td = new TransactionDetails(
				"$.resourceId",
				internalCorrelationId,
				camt053,
				transactionGroupId,
				transactionCategoryPurposeCode);
		
		String camt053Body = om.writeValueAsString(td);

		biBuilder.add(items, camt053RelativeUrl, camt053Body, true);
		
		if (!BigDecimal.ZERO.equals(fee)) {
			logger.debug("Withdrawing fee {} from conversion account {}", fee, conversionAccountAmsId);
			
			paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "revertInAms.ConversionAccount.WithdrawTransactionFee"));
			
			body = new TransactionBody(
					transactionDate,
					fee,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			bodyItem = om.writeValueAsString(body);
			
			biBuilder.add(items, conversionAccountWithdrawRelativeUrl, bodyItem, false);
			
			td = new TransactionDetails(
					"$.resourceId",
					internalCorrelationId,
					camt053,
					transactionGroupId,
					transactionFeeCategoryPurposeCode);
			camt053Body = om.writeValueAsString(td);
			biBuilder.add(items, camt053RelativeUrl, camt053Body, true);
		}

		logger.debug("Re-depositing amount {} in disposal account {}", amount, disposalAccountAmsId);
		
		String disposalAccountDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "deposit");
		
		paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "revertInAms.DisposalAccount.DepositTransactionAmount"));
		
		body = new TransactionBody(
				transactionDate,
				amount,
				paymentTypeId,
				"",
				FORMAT,
				locale);
		
		bodyItem = om.writeValueAsString(body);
		
		biBuilder.add(items, disposalAccountDepositRelativeUrl, bodyItem, false);
		
		camt053RelativeUrl = String.format("datatables/transaction_details/%d", disposalAccountAmsId);
		
		td = new TransactionDetails(
				"$.resourceId",
				internalCorrelationId,
				camt053,
				transactionGroupId,
				transactionCategoryPurposeCode);
		
		camt053Body = om.writeValueAsString(td);
		
		biBuilder.add(items, camt053RelativeUrl, camt053Body, true);
		
		if (!BigDecimal.ZERO.equals(fee)) {
			logger.debug("Re-depositing fee {} in disposal account {}", fee, disposalAccountAmsId);
			
			paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "revertInAms.DisposalAccount.DepositTransactionFee"));
			
			body = new TransactionBody(
					transactionDate,
					fee,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			bodyItem = om.writeValueAsString(body);
			
			biBuilder.add(items, disposalAccountDepositRelativeUrl, bodyItem, false);
			
			td = new TransactionDetails(
					"$.resourceId",
					internalCorrelationId,
					camt053,
					transactionGroupId,
					transactionFeeCategoryPurposeCode);
			camt053Body = om.writeValueAsString(td);
			biBuilder.add(items, camt053RelativeUrl, camt053Body, true);
		}
		
		doBatch(items, tenantId, internalCorrelationId);
		
		jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
		MDC.remove("internalCorrelationId");
	}

}
