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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import iso.std.iso._20022.tech.json.camt_052_001.BankToCustomerAccountReportV08;
import iso.std.iso._20022.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;

@Component
public class OnUsTransferWorker extends AbstractMoneyInOutWorker {
	
	private static final String ERROR_FAILED_CREDIT_TRANSFER = "Error_FailedCreditTransfer";

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
	public void handle(JobClient jobClient, ActivatedJob activatedJob) {
		try {
			Map<String, Object> variables = activatedJob.getVariablesAsMap();
			
			logger.info("Starting onUs transfer worker with variables {}", variables);
			
			String internalCorrelationId = variables.get("internalCorrelationId").toString();
			
			String paymentScheme = (String) variables.get("paymentScheme");
			
			String originalPain001 = (String) variables.get("originalPain001");
			ObjectMapper om = new ObjectMapper();
			Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = om.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
			
			BankToCustomerAccountReportV08 convertedCamt052 = camt052Mapper.toCamt052(pain001.getDocument());
			String camt052 = om.writeValueAsString(convertedCamt052);
			
			BigDecimal amount = new BigDecimal(variables.get("amount").toString());
			Integer creditorDisposalAccountAmsId = Integer.parseInt(variables.get("creditorDisposalAccountAmsId").toString());
			Integer debtorDisposalAccountAmsId = Integer.parseInt(variables.get("debtorDisposalAccountAmsId").toString());
			Integer debtorConversionAccountAmsId = Integer.parseInt(variables.get("debtorConversionAccountAmsId").toString());
			BigDecimal feeAmount = new BigDecimal(variables.get("transactionFeeAmount").toString());
			String tenantIdentifier = variables.get("tenantIdentifier").toString();
			
			String transactionGroupId = (String) variables.get("transactionGroupId");
			String transactionCategoryPurposeCode = (String) variables.get("transactionCategoryPurposeCode");
			String transactionFeeCategoryPurposeCode = (String) variables.get("transactionFeeCategoryPurposeCode");
			
			String interbankSettlementDate = LocalDate.now().format(PATTERN);
			
            BatchItemBuilder biBuilder = new BatchItemBuilder(tenantIdentifier);
    		
    		String debtorDisposalWithdrawalRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), debtorDisposalAccountAmsId, "withdrawal");
    		
    		PaymentTypeConfig paymentTypeConfig = paymentTypeConfigFactory.getPaymentTypeConfig(tenantIdentifier);
    		Integer paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "transferTheAmountBetweenDisposalAccounts.Debtor.DisposalAccount.WithdrawTransactionAmount"));
    		
    		TransactionBody body = new TransactionBody(
    				interbankSettlementDate,
    				amount,
    				paymentTypeId,
    				"",
    				FORMAT,
    				locale);
    		
    		String bodyItem = om.writeValueAsString(body);
    		
    		List<TransactionItem> items = new ArrayList<>();
    		
    		biBuilder.add(items, debtorDisposalWithdrawalRelativeUrl, bodyItem, false);
    	
    		String camt052RelativeUrl = String.format("datatables/transaction_details/%d", debtorDisposalAccountAmsId);
    		
    		TransactionDetails td = new TransactionDetails(
    				"$.resourceId",
    				internalCorrelationId,
    				camt052,
    				transactionGroupId,
    				transactionCategoryPurposeCode);
    		
    		String camt052Body = om.writeValueAsString(td);

    		biBuilder.add(items, camt052RelativeUrl, camt052Body, true);
    		
			
			if (!BigDecimal.ZERO.equals(feeAmount)) {
				paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "transferTheAmountBetweenDisposalAccounts.Debtor.DisposalAccount.WithdrawTransactionFee"));
	    		
	    		body = new TransactionBody(
	    				interbankSettlementDate,
	    				feeAmount,
	    				paymentTypeId,
	    				"",
	    				FORMAT,
	    				locale);
	    		
	    		bodyItem = om.writeValueAsString(body);
	    		
	    		biBuilder.add(items, debtorDisposalWithdrawalRelativeUrl, bodyItem, false);
	    	
	    		camt052RelativeUrl = String.format("datatables/transaction_details/%d", debtorDisposalAccountAmsId);
	    		
	    		td = new TransactionDetails(
	    				"$.resourceId",
	    				internalCorrelationId,
	    				camt052,
	    				transactionGroupId,
	    				transactionFeeCategoryPurposeCode);
	    		
	    		camt052Body = om.writeValueAsString(td);
	    		biBuilder.add(items, camt052RelativeUrl, camt052Body, true);

	    		
	    		
				paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "transferTheAmountBetweenDisposalAccounts.Debtor.ConversionAccount.DepositTransactionFee"));
	    		
	    		body = new TransactionBody(
	    				interbankSettlementDate,
	    				feeAmount,
	    				paymentTypeId,
	    				"",
	    				FORMAT,
	    				locale);
			    		
	    		bodyItem = om.writeValueAsString(body);
	    		
	    		String debtorConversionDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), debtorConversionAccountAmsId, "deposit");
		    		
	    		biBuilder.add(items, debtorConversionDepositRelativeUrl, bodyItem, false);
		    	
	    		camt052RelativeUrl = String.format("datatables/transaction_details/%d", debtorConversionAccountAmsId);
	    		
	    		td = new TransactionDetails(
	    				"$.resourceId",
	    				internalCorrelationId,
	    				camt052,
	    				transactionGroupId,
	    				transactionFeeCategoryPurposeCode);
	    		
	    		camt052Body = om.writeValueAsString(td);
	    		biBuilder.add(items, camt052RelativeUrl, camt052Body, true);
			}
			
			paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "transferTheAmountBetweenDisposalAccounts.Creditor.DisposalAccount.DepositTransactionAmount"));
    		
    		body = new TransactionBody(
    				interbankSettlementDate,
    				amount,
    				paymentTypeId,
    				"",
    				FORMAT,
    				locale);
		    		
    		bodyItem = om.writeValueAsString(body);
    		
    		String creditorDisposalDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), creditorDisposalAccountAmsId, "deposit");
	    		
    		biBuilder.add(items, creditorDisposalDepositRelativeUrl, bodyItem, false);
	    	
    		camt052RelativeUrl = String.format("datatables/transaction_details/%d", creditorDisposalAccountAmsId);
    		td = new TransactionDetails(
    				"$.resourceId",
    				internalCorrelationId,
    				camt052,
    				transactionGroupId,
    				transactionCategoryPurposeCode);
    		
    		camt052Body = om.writeValueAsString(td);
    		biBuilder.add(items, camt052RelativeUrl, camt052Body, true);
			
	    		
			if (!BigDecimal.ZERO.equals(feeAmount)) {
	    		paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "transferTheAmountBetweenDisposalAccounts.Debtor.ConversionAccount.WithdrawTransactionFee"));
	    		
	    		body = new TransactionBody(
	    				interbankSettlementDate,
	    				feeAmount,
	    				paymentTypeId,
	    				"",
	    				FORMAT,
	    				locale);
			    		
	    		bodyItem = om.writeValueAsString(body);
	    		
	    		String debtorConversionWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), debtorConversionAccountAmsId, "withdrawal");
		    		
	    		biBuilder.add(items, debtorConversionWithdrawRelativeUrl, bodyItem, false);
		    	
	    		camt052RelativeUrl = String.format("datatables/transaction_details/%d", debtorConversionAccountAmsId);
			    		
	    		biBuilder.add(items, camt052RelativeUrl, camt052Body, true);
			}
			
			doBatch(items, tenantIdentifier, internalCorrelationId);
			
			jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
		} catch (JsonProcessingException e) {
			logger.error(e.getMessage(), e);
			jobClient.newThrowErrorCommand(activatedJob.getKey()).errorCode(ERROR_FAILED_CREDIT_TRANSFER).errorMessage(e.getMessage()).send();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).errorMessage(e.getMessage()).send();
		}
	}
}
