package org.mifos.connector.ams.zeebe.workers.bookamount;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import io.camunda.zeebe.client.api.worker.JobHandler;

@Component
public abstract class AbstractMoneyInOutWorker implements JobHandler {
	
	@Autowired
	protected RestTemplate restTemplate;
	
	@Autowired
	protected HttpHeaders httpHeaders;
	
	@Value("${fineract.api-url}")
	protected String fineractApiUrl;

	@Value("${fineract.incoming-money-api}")
	protected String incomingMoneyApi;
	
	@Value("${fineract.locale}")
	protected String locale;

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	protected static final String FORMAT = "yyyyMMdd";
	
	protected ResponseEntity<Object> release(Integer currencyAccountAmsId, Integer holdAmountId, String tenantId) {
		httpHeaders.remove("Fineract-Platform-TenantId");
		httpHeaders.add("Fineract-Platform-TenantId", tenantId);
		var entity = new HttpEntity<>(null, httpHeaders);
		
		var urlTemplate = UriComponentsBuilder.fromHttpUrl(fineractApiUrl)
				.path(incomingMoneyApi)
				.path(String.format("%s", currencyAccountAmsId))
				.path("/transactions")
				.path(String.format("/%s", holdAmountId))
				.queryParam("command", "releaseAmount")
				.encode()
				.toUriString();
		
		logger.info(">> Sending {} to {} with headers {}", null, urlTemplate, httpHeaders);
		
		ResponseEntity<Object> response = restTemplate.exchange(urlTemplate, HttpMethod.POST, entity, Object.class);
		
		logger.info("<< Received HTTP {}", response.getStatusCode());
		
		return response;
	}
	
	protected ResponseEntity<Object> hold(String transactionDate, Object amount, Integer currencyAccountAmsId, String tenantId) {
		var body = new HoldAmountBody(
				transactionDate,
				amount,
				"Transfer out - on Hold - pending checks",
				locale,
				FORMAT
				);
		return doExchange(body, currencyAccountAmsId, "holdAmount", tenantId);
	}
	
	protected ResponseEntity<Object> deposit(String transactionDate, Object amount, Integer currencyAccountAmsId, Integer paymentTypeId, String tenantId) {
		var body = new TransactionBody(
				transactionDate,
				amount,
				paymentTypeId,
				"",
				FORMAT,
				locale);
		return doExchange(body, currencyAccountAmsId, "deposit", tenantId);
	}

	protected ResponseEntity<Object> withdraw(String transactionDate, Object amount, Integer currencyAccountAmsId, Integer paymentTypeId, String tenantId) {
		var body = new TransactionBody(
				transactionDate,
				amount,
				paymentTypeId,
				"",
				FORMAT,
				locale);
		return doExchange(body, currencyAccountAmsId, "withdrawal", tenantId);
	}
	
	private <T> ResponseEntity<Object> doExchange(T body, Integer currencyAccountAmsId, String command, String tenantId) {
		httpHeaders.remove("Fineract-Platform-TenantId");
		httpHeaders.add("Fineract-Platform-TenantId", tenantId);
		var entity = new HttpEntity<>(body, httpHeaders);
		
		var urlTemplate = UriComponentsBuilder.fromHttpUrl(fineractApiUrl)
				.path(incomingMoneyApi)
				.path(String.format("%s", currencyAccountAmsId))
				.path("/transactions")
				.queryParam("command", command)
				.encode()
				.toUriString();
		
		logger.info(">> Sending {} to {} with headers {}", body, urlTemplate, httpHeaders);
		
		try {
			ResponseEntity<Object> response = restTemplate.exchange(urlTemplate, HttpMethod.POST, entity, Object.class);
			logger.info("<< Received HTTP {}", response.getStatusCode());
			return response;
		} catch (HttpClientErrorException e) {
			logger.error(e.getMessage(), e);
			return ResponseEntity.status(e.getRawStatusCode()).headers(e.getResponseHeaders())
	                .body(e.getResponseBodyAsString());
		}
	}
}
