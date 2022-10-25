package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;

@Component
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
		
		ResponseEntity<Object> responseObject = exchange(transactionDate, amount, fiatCurrencyAccountAmsId, "withdrawal");
		
		if (HttpStatus.OK.equals(responseObject.getStatusCode())) {
			jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
		} else {
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
		}
		
		responseObject = exchange(transactionDate, amount, eCurrencyAccountAmsId, "deposit");
		
		if (HttpStatus.OK.equals(responseObject.getStatusCode())) {
			jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
		} else {
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
		}
	}
}
