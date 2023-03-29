package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.mifos.connector.ams.mapstruct.Pain001Camt052Mapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
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
			
			Object amount = variables.get("amount");
			Integer creditorDisposalAccountAmsId = Integer.parseInt(variables.get("creditorDisposalAccountAmsId").toString());
			Integer debtorDisposalAccountAmsId = Integer.parseInt(variables.get("debtorDisposalAccountAmsId").toString());
			Integer debtorConversionAccountAmsId = Integer.parseInt(variables.get("debtorConversionAccountAmsId").toString());
			Object feeAmount = variables.get("transactionFeeAmount");
			String tenantIdentifier = variables.get("tenantIdentifier").toString();
			
			String interbankSettlementDate = LocalDate.now().format(PATTERN);
			
			FineractOperationExecutor opExecutor = new FineractOperationExecutor(jobClient, activatedJob, internalCorrelationId, camt052, tenantIdentifier);
			
			opExecutor.execute(
					withdraw(
							interbankSettlementDate, 
							amount, 
							debtorDisposalAccountAmsId, 
							paymentScheme,
							"transferTheAmountBetweenDisposalAccounts.Debtor.DisposalAccount.WithdrawTransactionAmount",
							tenantIdentifier, 
							internalCorrelationId), 
					ERROR_FAILED_CREDIT_TRANSFER
			);
			
			opExecutor.execute(
					withdraw(
							interbankSettlementDate, 
							feeAmount, 
							debtorDisposalAccountAmsId, 
							paymentScheme,
							"transferTheAmountBetweenDisposalAccounts.Debtor.DisposalAccount.WithdrawTransactionFee",
							tenantIdentifier, 
							internalCorrelationId), 
					ERROR_FAILED_CREDIT_TRANSFER
			);
			
			opExecutor.execute(
					deposit(
							interbankSettlementDate, 
							amount, 
							creditorDisposalAccountAmsId, 
							paymentScheme,
							"transferTheAmountBetweenDisposalAccounts.Creditor.DisposalAccount.DepositTransactionAmount",
							tenantIdentifier, 
							internalCorrelationId), 
					ERROR_FAILED_CREDIT_TRANSFER
			);
			
			opExecutor.execute(
					deposit(
							interbankSettlementDate, 
							feeAmount, 
							debtorConversionAccountAmsId, 
							paymentScheme,
							"transferTheAmountBetweenDisposalAccounts.Debtor.ConversionAccount.DepositTransactionFee",
							tenantIdentifier, 
							internalCorrelationId), 
					ERROR_FAILED_CREDIT_TRANSFER
			);
			
			opExecutor.execute(
					withdraw(
							interbankSettlementDate, 
							feeAmount, 
							debtorConversionAccountAmsId, 
							paymentScheme,
							"transferTheAmountBetweenDisposalAccounts.Debtor.ConversionAccount.WithdrawTransactionFee",
							tenantIdentifier, 
							internalCorrelationId), 
					ERROR_FAILED_CREDIT_TRANSFER
			);
			
			jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
		} catch (JsonProcessingException e) {
			logger.error(e.getMessage(), e);
			jobClient.newThrowErrorCommand(activatedJob.getKey()).errorCode(ERROR_FAILED_CREDIT_TRANSFER).errorMessage(e.getMessage()).send();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).errorMessage(e.getMessage()).send();
		}
	}

	private class FineractOperationExecutor {

		private JobClient jobClient;
		private ActivatedJob activatedJob;
		private String internalCorrelationId;
		private String camt052;
		private String tenantIdentifier;
		
		FineractOperationExecutor(JobClient jobClient, ActivatedJob activatedJob, String internalCorrelationId,
				String camt052, String tenantIdentifier) {
			super();
			this.jobClient = jobClient;
			this.activatedJob = activatedJob;
			this.internalCorrelationId = internalCorrelationId;
			this.camt052 = camt052;
			this.tenantIdentifier = tenantIdentifier;
		}
		
		void execute(ResponseEntity<Object> responseObject, String errorCode) throws JsonProcessingException {
			if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
				jobClient.newThrowErrorCommand(activatedJob.getKey()).errorCode(errorCode).errorMessage(responseObject.getBody().toString()).send();
				return;
			}
			
			postCamt052(tenantIdentifier, camt052, internalCorrelationId, responseObject);
		}
	}
}
