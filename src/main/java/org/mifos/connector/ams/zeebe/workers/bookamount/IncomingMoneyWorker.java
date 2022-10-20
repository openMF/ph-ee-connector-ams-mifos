package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.client.api.worker.JobHandler;

public class IncomingMoneyWorker implements JobHandler {

	@Autowired
	private RestTemplate restTemplate;

	@Value("${fineract.api-url}")
	private String fineractApiUrl;

	@Value("${fineract.incoming-money-api}")
	private String incomingMoneyApi;
	
	@Value("${fineract.locale}")
	private String locale;

	Logger logger = LoggerFactory.getLogger(IncomingMoneyWorker.class);
	
	private static final String FORMAT = "yyyyMMdd";
	private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);

	@Override
	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		logger.error("IncomingMoneyWorker has started");
		Map<String, Object> variables = activatedJob.getVariablesAsMap();
		
		String transactionDate = LocalDate.now().format(PATTERN);
		BigDecimal amount = (BigDecimal) variables.get("amount");
		Integer paymentTypeId = (Integer) variables.get("fiatCurrencyAccountAmsId");
		
		HttpHeaders headers = new HttpHeaders();
		headers.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);

		DepositBody body = new DepositBody(
				transactionDate,
				amount,
				paymentTypeId,
				"",
				FORMAT,
				locale);
		HttpEntity<DepositBody> entity = new HttpEntity<>(body, headers);

		String urlTemplate = UriComponentsBuilder.fromHttpUrl(fineractApiUrl)
				.path(incomingMoneyApi)
				.path("" + paymentTypeId)
				.path("transactions")
				.encode()
				.toUriString();
		
		Map<String, Object> params = new HashMap<>();
		params.put("command", "deposit");

		ResponseEntity<Object> responseObject = restTemplate.exchange(urlTemplate, HttpMethod.POST, entity, Object.class, params);

		if (HttpStatus.OK.equals(responseObject.getStatusCode())) {
			jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
		} else {
			jobClient.newFailCommand(activatedJob.getKey()).retries(3).send();
		}
	}
}
