package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;

public class IncomingMoneyWorker extends AbstractMoneyInWorker {
	
	Logger logger = LoggerFactory.getLogger(IncomingMoneyWorker.class);
	
	private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);

	@Override
	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		Map<String, Object> variables = activatedJob.getVariablesAsMap();
		
		String transactionDate = LocalDate.now().format(PATTERN);
		Object amount = variables.get("amount");
		
		Integer fiatCurrencyAccountAmsId = (Integer) variables.get("fiatCurrencyAccountAmsId");
		
		ResponseEntity<Object> responseObject = deposit(transactionDate, amount, fiatCurrencyAccountAmsId);
		
		if (HttpStatus.OK.equals(responseObject.getStatusCode())) {
			jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
		} else {
			jobClient.newFailCommand(activatedJob.getKey()).retries(3).send();
		}
	}
}
