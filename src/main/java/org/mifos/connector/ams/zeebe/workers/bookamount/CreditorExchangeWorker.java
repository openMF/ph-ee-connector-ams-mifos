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

public class CreditorExchangeWorker extends AbstractMoneyInWorker {

	Logger logger = LoggerFactory.getLogger(CreditorExchangeWorker.class);
	
	private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);
	
	@Override
	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		Map<String, Object> variables = activatedJob.getVariablesAsMap();
		
		String transactionDate = LocalDate.now().format(PATTERN);
		Object amount = variables.get("amount");
		
		Integer fiatCurrencyAccountAmsId = (Integer) variables.get("fiatCurrencyAccountAmsId");
		Integer eCurrencyAccountAmsId = (Integer) variables.get("eCurrencyAccountAmsId");
		
		ResponseEntity<Object> responseObject = withdraw(transactionDate, amount, fiatCurrencyAccountAmsId);
		
		if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
			return;
		}
		
		responseObject = deposit(transactionDate, amount, eCurrencyAccountAmsId);
		
		if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
			return;
		}
		
		jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
	}
}
