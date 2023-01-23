package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import iso.std.iso20022plus.tech.json.pain_001_001.CreditTransferTransaction40;
import iso.std.iso20022plus.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;

@Component
public class BookOnConversionAccountInAmsWorker extends AbstractMoneyInOutWorker {
	
	@Override
	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		Map<String, Object> variables = activatedJob.getVariablesAsMap();
		
		Integer conversionAccountAmsId = (Integer) variables.get("conversionAccountAmsId");
		
		String originalPain001 = (String) variables.get("originalPain001");
		
		logger.info("Starting book debit on fiat account worker with currency account Id {}", conversionAccountAmsId);
		
		String interbankSettlementDate = (String) variables.get("interbankSettlementDate");
		
		if (interbankSettlementDate == null) {
			logger.error("Book debit on fiat account has failed due to Interbank settlement date not being present in pacs.002");
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
			return;
		}
		
		ObjectMapper om = new ObjectMapper();
		Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = om.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
		
		BigDecimal amount = BigDecimal.ZERO;
		BigDecimal fee = new BigDecimal(variables.get("transactionFeeAmount").toString());
		
		List<CreditTransferTransaction40> creditTransferTransactionInformation = pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation();
		
		for (CreditTransferTransaction40 ctti : creditTransferTransactionInformation) {
			amount = new BigDecimal(ctti.getAmount().getInstructedAmount().getAmount().toString());
		}
		
		logger.error("Debtor exchange worker incoming variables:");
		variables.entrySet().forEach(e -> logger.error("{}: {}", e.getKey(), e.getValue()));
	
		logger.info("Attempting to deposit the amount of {}", amount);
	
		ResponseEntity<Object> responseObject = withdraw(interbankSettlementDate, amount, conversionAccountAmsId, 1);
			
		if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
			return;
		}
			
		responseObject = withdraw(interbankSettlementDate, fee, conversionAccountAmsId, 1);
			
		if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
			ResponseEntity<Object> sagaResponseObject = deposit(interbankSettlementDate, amount, conversionAccountAmsId, 1);
			if (!HttpStatus.OK.equals(sagaResponseObject.getStatusCode())) {
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
			}
			return;
		}
		
		jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
			
		logger.info("Book debit on fiat account has finished  successfully");
	}
}
