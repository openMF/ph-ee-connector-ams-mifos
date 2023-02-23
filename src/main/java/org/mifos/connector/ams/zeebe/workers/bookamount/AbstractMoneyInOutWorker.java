package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.net.SocketTimeoutException;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

import org.mifos.connector.ams.log.IOTxLogger;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.camunda.zeebe.client.api.worker.JobHandler;

@Component
public abstract class AbstractMoneyInOutWorker implements JobHandler {
	
	@Autowired
	protected RestTemplate restTemplate;
	
	@Autowired
	protected HttpHeaders httpHeaders;
	
	@Autowired
	private IOTxLogger wireLogger;
	
	@Value("${fineract.api-url}")
	protected String fineractApiUrl;

	@Value("${fineract.incoming-money-api}")
	protected String incomingMoneyApi;
	
	@Value("${fineract.locale}")
	protected String locale;
	
	@Value("${fineract.idempotency.count}")
	private int idempotencyRetryCount;
	
	@Value("${fineract.idempotency.interval}")
	private int idempotencyRetryInterval;
	
	@Value("${fineract.idempotency.key-header-name}")
	private String idempotencyKeyHeaderName;

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
	
	@SuppressWarnings("unchecked")
	protected void postCamt052(String tenantId, String camt052, String internalCorrelationId,
			ResponseEntity<Object> responseObject) throws JsonProcessingException {
		logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>  camt.052  <<<<<<<<<<<<<<<<<<<<<<<<");
		
		Map<String, Object> body = (Map<String, Object>) responseObject.getBody();
		logger.info("Generating camt.052 based on the following reponse body: {}", body);
		
		Object txId = body.get("resourceId");
		logger.info("Setting amsTransactionId to {}", txId);
		
		Object clientId = body.get("clientId");
		logger.info("Setting clientId to {}", clientId);
		
		LocalDateTime now = LocalDateTime.now();
		
		TransactionDetails td = new TransactionDetails(
				clientId, 
				txId,
				internalCorrelationId,
				camt052,
				now,
				now);
		
		logger.info("The following camt.052 will be inserted into the data table: {}", camt052);
		
		httpHeaders.remove("Fineract-Platform-TenantId");
		httpHeaders.add("Fineract-Platform-TenantId", tenantId);
		var entity = new HttpEntity<>(td, httpHeaders);
		
		var urlTemplate = UriComponentsBuilder.fromHttpUrl(fineractApiUrl)
				.path("/datatables")
				.path("/transaction_details")
				.path("/" + clientId)
				.queryParam("genericResultSet", true)
				.encode()
				.toUriString();
		
		logger.info(">> Sending {} to {} with headers {}", td, urlTemplate, httpHeaders);
		
		try {
			ResponseEntity<Object> response = restTemplate.exchange(urlTemplate, HttpMethod.POST, entity, Object.class);
			logger.info("<< Received HTTP {}", response.getStatusCode());
		} catch (HttpClientErrorException e) {
			logger.error(e.getMessage(), e);
			logger.warn("Cam052 insert returned with status code {}", e.getRawStatusCode());
			throw new RuntimeException(e);
		}
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
		
		int retryCount = idempotencyRetryCount;
		String idempotencyHeaderValue = UUID.randomUUID().toString();
		httpHeaders.remove(idempotencyKeyHeaderName);
		httpHeaders.set(idempotencyKeyHeaderName, idempotencyHeaderValue);
		
		while (retryCount > 0) {
			try {
				wireLogger.sending(body.toString());
				ResponseEntity<Object> response = restTemplate.exchange(urlTemplate, HttpMethod.POST, entity, Object.class);
				wireLogger.receiving(response.toString());
				
				logger.info("<< Received {}", response);
				return response;
			} catch (HttpClientErrorException e) {
				logger.error(e.getMessage(), e);
				if (HttpStatus.CONFLICT.equals(e.getStatusCode())) {
					logger.warn("Transaction is already executing, but not completed yet");
					break;
				}
				logger.warn("Transaction returned with status code {}, rolling back any previous transactions", e.getRawStatusCode());
				return ResponseEntity.status(e.getRawStatusCode()).headers(e.getResponseHeaders())
		                .body(e.getResponseBodyAsString());
			} catch (Exception e) {
				if (e instanceof InterruptedException
						|| (e instanceof ResourceAccessException
								&& e.getCause() instanceof SocketTimeoutException)) {
					retryCount--;
					try {
						Thread.sleep(idempotencyRetryInterval);
					} catch (InterruptedException ie) {
						logger.error(ie.getMessage(), ie);
					}
				}
			}
		}
		
		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("An unexpected error occurred");
	}
}
