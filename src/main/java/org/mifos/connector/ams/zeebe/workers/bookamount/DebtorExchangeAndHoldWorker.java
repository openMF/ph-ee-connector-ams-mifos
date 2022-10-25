package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.util.UriComponentsBuilder;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;

public class DebtorExchangeAndHoldWorker extends AbstractMoneyInOutWorker {
	
	Logger logger = LoggerFactory.getLogger(DebtorExchangeAndHoldWorker.class);
	
	private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);

	@Override
	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		Map<String, Object> variables = activatedJob.getVariablesAsMap();
		
		String transactionDate = LocalDate.now().format(PATTERN);
		Object amount = variables.get("amount");
		
		Integer eCurrencyAccountAmsId = (Integer) variables.get("eCurrencyAccountAmsId");
		Integer fiatCurrencyAccountAmsId = (Integer) variables.get("fiatCurrencyAccountAmsId");
		
		ResponseEntity<Object> responseObject = withdraw(transactionDate, amount, eCurrencyAccountAmsId);
		
		if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
			return;
		}
		
		responseObject = deposit(transactionDate, amount, fiatCurrencyAccountAmsId);
		
		if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
			return;
		}
		
		responseObject = hold(transactionDate, amount, fiatCurrencyAccountAmsId);
		
		if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
			return;
		}
		
		// TODO: create journal entry
		AccountIdAmountPair[] debits = new AccountIdAmountPair[] { new AccountIdAmountPair(10, amount) };
		AccountIdAmountPair[] credits = new AccountIdAmountPair[] { new AccountIdAmountPair(14, amount) };
		
		JournalEntry entry = new JournalEntry(
				"1",
				(String) variables.get("currency"),
				debits,
				credits,
				"",
				LocalDate.now().format(PATTERN),
				"",
				String.format("%d", fiatCurrencyAccountAmsId),
				"",
				"",
				"",
				"",
				"",
				locale,
				FORMAT
				);
		var entity = new HttpEntity<>(entry, httpHeaders);
		
		var urlTemplate = UriComponentsBuilder.fromHttpUrl(fineractApiUrl)
				.path(incomingMoneyApi)
				.path(String.format("%s", fiatCurrencyAccountAmsId))
				.path("/journalentries")
				.encode()
				.toUriString();
		
		responseObject = restTemplate.exchange(urlTemplate, HttpMethod.POST, entity, Object.class);
		
		if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
			return;
		}
		
		jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
	}

}
