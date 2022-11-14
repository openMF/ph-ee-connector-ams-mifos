package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponentsBuilder;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;

@Component
public class DebtorExchangeAndHoldWorker extends AbstractMoneyInOutWorker {
	
	@Value("${fineract.paymentType.paymentTypeExchangeECurrencyId}")
	private Integer paymentTypeExchangeECurrencyId;
	
	@Value("${fineract.paymentType.paymentTypeExchangeToFiatCurrencyId}")
	private Integer paymentTypeExchangeToFiatCurrencyId;
	
	private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);

	@Override
	@SuppressWarnings("unchecked")
	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		logger.error("Debtor exchange worker starting");
		try {
			Map<String, Object> variables = activatedJob.getVariablesAsMap();
			
			logger.error("Debtor exchange worker incoming variables:");
			variables.entrySet().forEach(e -> logger.error("{}: {}", e.getKey(), e.getValue()));
		
			String transactionDate = LocalDate.now().format(PATTERN);
			Object amount = variables.get("amount");
		
			Integer eCurrencyAccountAmsId = (Integer) variables.get("eCurrencyAccountAmsId");
			Integer fiatCurrencyAccountAmsId = (Integer) variables.get("fiatCurrencyAccountAmsId");
		
		
			ResponseEntity<Object> responseObject = withdraw(transactionDate, amount, eCurrencyAccountAmsId, paymentTypeExchangeECurrencyId);
		
			if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
				return;
			}
		
			responseObject = deposit(transactionDate, amount, fiatCurrencyAccountAmsId, paymentTypeExchangeToFiatCurrencyId);
		
			if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
				return;
			}
		
			responseObject = hold(transactionDate, amount, fiatCurrencyAccountAmsId);
		
			if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
				return;
			}
		
			LinkedHashMap<String, Object> holdBody = ((LinkedHashMap<String, Object>) responseObject.getBody());
			Integer resourceId = (Integer) holdBody.get("resourceId");
			logger.info("{} put on hold with holdAmountId {}", amount, resourceId);
			variables.put("holdAmountId", resourceId);
		
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
				.path("/journalentries")
				.encode()
				.toUriString();
		
			responseObject = restTemplate.exchange(urlTemplate, HttpMethod.POST, entity, Object.class);
		
			if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
				logger.error("Debtor exchange and hold worker fails with status code {}", responseObject.getStatusCodeValue());
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
				return;
			}
		
			jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			jobClient.newThrowErrorCommand(activatedJob.getKey()).errorCode("Error_InsufficientFunds").send();
		}
	}

}
