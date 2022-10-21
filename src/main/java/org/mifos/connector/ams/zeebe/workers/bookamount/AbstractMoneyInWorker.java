package org.mifos.connector.ams.zeebe.workers.bookamount;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import io.camunda.zeebe.client.api.worker.JobHandler;

public abstract class AbstractMoneyInWorker implements JobHandler {
	
	@Autowired
	private RestTemplate restTemplate;
	
	@Autowired
	private HttpHeaders httpHeaders;
	
	@Value("${fineract.api-url}")
	private String fineractApiUrl;

	@Value("${fineract.incoming-money-api}")
	private String incomingMoneyApi;
	
	@Value("${fineract.locale}")
	private String locale;

	Logger logger = LoggerFactory.getLogger(IncomingMoneyWorker.class);
	
	protected static final String FORMAT = "yyyyMMdd";

	protected ResponseEntity<Object> exchange(String transactionDate, Object amount, Integer currencyAccountAmsId, String command) {
		var body = new TransactionBody(
				transactionDate,
				amount,
				currencyAccountAmsId,
				"",
				FORMAT,
				locale);
		var entity = new HttpEntity<>(body, httpHeaders);

		var urlTemplate = UriComponentsBuilder.fromHttpUrl(fineractApiUrl)
				.path(incomingMoneyApi)
				.path(String.format("%s", currencyAccountAmsId))
				.path("/transactions")
				.queryParam("command", command)
				.encode()
				.toUriString();
		
		return restTemplate.exchange(urlTemplate, HttpMethod.POST, entity, Object.class);
	}
}
