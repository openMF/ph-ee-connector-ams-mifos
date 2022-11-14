package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
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
public class RevertDebitOnFiatAccountWorker extends AbstractMoneyInOutWorker {
	
	private static final String ERROR_MESSAGE_PATTERN = "Revert debit worker fails with status code {}";

	@Value("${fineract.paymentType.paymentTypeExchangeFiatCurrencyId}")
	private Integer paymentTypeExchangeFiatCurrencyId;
	
	@Value("${fineract.paymentType.paymentTypeIssuingECurrencyId}")
	private Integer paymentTypeIssuingECurrencyId;
	
	@Value("${fineract.generalLedger.glLiabilityAmountOnHoldId}")
	private Integer glLiabilityAmountOnHoldId;
	
	@Value("${fineract.generalLedger.glLiabilityToCustomersInFiatCurrencyId}")
	private Integer glLiabilityToCustomersInFiatCurrencyId;
	
	private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);

	@Override
	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		Map<String, Object> variables = activatedJob.getVariablesAsMap();
		
		Integer fiatCurrencyAccountAmsId = (Integer) variables.get("fiatCurrencyAccountAmsId");
		Integer holdAmountId = (Integer) variables.get("holdAmountId");
		
		
		ResponseEntity<Object> responseObject = release(fiatCurrencyAccountAmsId, holdAmountId);
		
		if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
			logger.error(ERROR_MESSAGE_PATTERN, responseObject.getStatusCodeValue());
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
			return;
		}
		
		
		Object amount = variables.get("amount");
		AccountIdAmountPair[] debits = new AccountIdAmountPair[] { new AccountIdAmountPair(glLiabilityAmountOnHoldId, amount) };
		AccountIdAmountPair[] credits = new AccountIdAmountPair[] { new AccountIdAmountPair(glLiabilityToCustomersInFiatCurrencyId, amount) };
		
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
			logger.error(ERROR_MESSAGE_PATTERN, responseObject.getStatusCodeValue());
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
			return;
		}

			
		responseObject = withdraw(LocalDate.now().format(PATTERN), amount, fiatCurrencyAccountAmsId, paymentTypeExchangeFiatCurrencyId);
		
		if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
			logger.error(ERROR_MESSAGE_PATTERN, responseObject.getStatusCodeValue());
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
			return;
		}
		
		Integer eCurrencyAccountAmsId = (Integer) variables.get("eCurrencyAccountAmsId");
		responseObject = deposit(LocalDate.now().format(PATTERN), amount, eCurrencyAccountAmsId, paymentTypeIssuingECurrencyId);
		
		if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
			logger.error(ERROR_MESSAGE_PATTERN, responseObject.getStatusCodeValue());
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
			return;
		}
		
		
		jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
	}

}
