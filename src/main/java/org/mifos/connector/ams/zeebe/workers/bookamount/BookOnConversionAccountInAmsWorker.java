package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;

@Component
public class BookOnConversionAccountInAmsWorker extends AbstractMoneyInOutWorker {
	
	private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);

	@Override
	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		Map<String, Object> variables = activatedJob.getVariablesAsMap();
		
		Integer conversionAccountAmsId = (Integer) variables.get("conversionAccountAmsId");
		
		logger.info("Starting book debit on fiat account worker with currency account Id {}", conversionAccountAmsId);
		
		String interbankSettlementDate = (String) variables.get("interbankSettlementDate");
		
		Object amount = variables.get("amount");
			
		ResponseEntity<Object> responseObject = withdraw(Optional.ofNullable(interbankSettlementDate).orElse(LocalDateTime.now().format(PATTERN)), amount, conversionAccountAmsId, 1);
			
		if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
			logger.error("Debtor exchange and hold worker fails with status code {}", responseObject.getStatusCodeValue());
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
			return;
		}
		
		jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
			
		logger.info("Book debit on fiat account has finished  successfully");
	}
}
