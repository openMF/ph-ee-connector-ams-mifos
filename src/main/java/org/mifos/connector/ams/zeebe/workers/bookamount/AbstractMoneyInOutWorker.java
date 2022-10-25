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

@InheritingComponent
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

	Logger logger = LoggerFactory.getLogger(IncomingMoneyWorker.class);
	
	protected static final String FORMAT = "yyyyMMdd";
	
	protected ResponseEntity<Object> hold(String transactionDate, Object amount, Integer currencyAccountAmsId) {
		var body = new HoldAmountBody(
				transactionDate,
				amount,
				"Transfer out - on Hold - pending checks",
				locale,
				FORMAT
				);
		return doExchange(body, transactionDate, amount, currencyAccountAmsId, "holdAmount");
	}
	
	protected ResponseEntity<Object> deposit(String transactionDate, Object amount, Integer currencyAccountAmsId) {
		var body = new TransactionBody(
				transactionDate,
				amount,
				currencyAccountAmsId,
				"",
				FORMAT,
				locale);
		return doExchange(body, transactionDate, amount, currencyAccountAmsId, "deposit");
	}

	protected ResponseEntity<Object> withdraw(String transactionDate, Object amount, Integer currencyAccountAmsId) {
		var body = new TransactionBody(
				transactionDate,
				amount,
				currencyAccountAmsId,
				"",
				FORMAT,
				locale);
		return doExchange(body, transactionDate, amount, currencyAccountAmsId, "withdrawal");
	}
	
	private <T> ResponseEntity<Object> doExchange(T body, String transactionDate, Object amount, Integer currencyAccountAmsId, String command) {
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
