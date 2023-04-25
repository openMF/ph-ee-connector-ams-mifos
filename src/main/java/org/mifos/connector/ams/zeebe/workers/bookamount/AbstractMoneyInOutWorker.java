package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.LinkedHashMap;
import java.util.List;

import org.mifos.connector.ams.fineract.PaymentTypeConfig;
import org.mifos.connector.ams.fineract.PaymentTypeConfigFactory;
import org.mifos.connector.ams.log.IOTxLogger;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionBody;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;
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
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.camunda.zeebe.client.api.worker.JobHandler;

@Component
public abstract class AbstractMoneyInOutWorker implements JobHandler {
	
	@Autowired
	protected RestTemplate restTemplate;
	
	@Autowired
	private IOTxLogger wireLogger;
	
	@Autowired
    private PaymentTypeConfigFactory paymentTypeConfigFactory;

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
	
	@Value("${fineract.auth-token}")
	private String authToken;

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	protected static final String FORMAT = "yyyyMMdd";
	
	protected ResponseEntity<Object> release(Integer currencyAccountAmsId, Integer holdAmountId, String tenantId) {
		HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
		httpHeaders.set("Authorization", "Basic " + authToken);
		httpHeaders.set("Fineract-Platform-TenantId", tenantId);
		var entity = new HttpEntity<>(null, httpHeaders);
		
		var urlTemplate = UriComponentsBuilder.fromHttpUrl(fineractApiUrl)
				.path(incomingMoneyApi)
				.path(String.format("%s", currencyAccountAmsId))
				.path("/transactions")
				.path(String.format("/%s", holdAmountId))
				.queryParam("command", "releaseAmount")
				.encode()
				.toUriString();
		
		logger.debug(">> Sending {} to {} with headers {}", null, urlTemplate, httpHeaders);
		
		ResponseEntity<Object> response = restTemplate.exchange(urlTemplate, HttpMethod.POST, entity, Object.class);
		
		logger.debug("<< Received HTTP {}", response.getStatusCode());
		
		return response;
	}

	protected ResponseEntity<Object> deposit(String transactionDate, Object amount, Integer currencyAccountAmsId, String paymentScheme, String paymentTypeName, String tenantId, String internalCorrelationId) {
		logger.info("Looking up {}.{}", paymentScheme, paymentTypeName);
		PaymentTypeConfig paymentTypeConfig = paymentTypeConfigFactory.getPaymentTypeConfig(tenantId);
		
		Integer paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, paymentTypeName));
		var body = new TransactionBody(
				transactionDate,
				amount,
				paymentTypeId,
				"",
				FORMAT,
				locale);
		return doExchange(body, currencyAccountAmsId, "deposit", tenantId, internalCorrelationId);
	}

	protected ResponseEntity<Object> withdraw(String transactionDate, Object amount, Integer currencyAccountAmsId, String paymentScheme, String paymentTypeName, String tenantId, String internalCorrelationId) {
		logger.info("Looking up {}.{}", paymentScheme, paymentTypeName);
		PaymentTypeConfig paymentTypeConfig = paymentTypeConfigFactory.getPaymentTypeConfig(tenantId);
		
		Integer paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, paymentTypeName));
		var body = new TransactionBody(
				transactionDate,
				amount,
				paymentTypeId,
				"",
				FORMAT,
				locale);
		return doExchange(body, currencyAccountAmsId, "withdrawal", tenantId, internalCorrelationId);
	}
	
	private <T> ResponseEntity<Object> doExchange(T body, Integer currencyAccountAmsId, String command, String tenantId, String internalCorrelationId) {
		HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
		httpHeaders.set("Authorization", "Basic " + authToken);
		httpHeaders.set("Fineract-Platform-TenantId", tenantId);
		var entity = new HttpEntity<>(body, httpHeaders);
		
		var urlTemplate = UriComponentsBuilder.fromHttpUrl(fineractApiUrl)
				.path(incomingMoneyApi)
				.path(String.format("%s", currencyAccountAmsId))
				.path("/transactions")
				.queryParam("command", command)
				.encode()
				.toUriString();
		
		logger.debug(">> Sending {} to {} with headers {}", body, urlTemplate, httpHeaders);
		
		int retryCount = idempotencyRetryCount;
		httpHeaders.remove(idempotencyKeyHeaderName);
		httpHeaders.set(idempotencyKeyHeaderName, internalCorrelationId);
		
		while (retryCount > 0) {
			try {
				wireLogger.sending(body.toString());
				ResponseEntity<Object> response = restTemplate.exchange(urlTemplate, HttpMethod.POST, entity, Object.class);
				wireLogger.receiving(response.toString());
				
				logger.debug("<< Received {}", response);
				return response;
			} catch (HttpClientErrorException e) {
				logger.error(e.getMessage(), e);
				if (HttpStatus.CONFLICT.equals(e.getStatusCode())) {
					logger.warn("Transaction is already executing, has not completed yet");
					break;
				} else {
					logger.warn("Transaction returned with status code {}", e.getRawStatusCode());
				}
				logger.warn(e.getMessage(), e);
				throw new RuntimeException(e);
			} catch (Exception e) {
				if (e instanceof InterruptedException
						|| (e instanceof ResourceAccessException
								&& e.getCause() instanceof SocketTimeoutException)) {
					logger.warn("Communication with Fineract timed out, retrying transaction {} more times with idempotency header value {}", retryCount, internalCorrelationId);
				} else {
					logger.error(e.getMessage(), e);
				}
				try {
					Thread.sleep(idempotencyRetryInterval);
				} catch (InterruptedException ie) {
					logger.error(ie.getMessage(), ie);
					throw new RuntimeException(ie);
				}
			}
			retryCount--;
		}
		
		throw new RuntimeException("An unexpected error occurred");
	}
	
	@SuppressWarnings("unchecked")
	protected void doBatch(List<TransactionItem> items, String tenantId, String internalCorrelationId) throws JsonProcessingException {
		HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
		httpHeaders.set("Authorization", "Basic " + authToken);
		httpHeaders.set("Fineract-Platform-TenantId", tenantId);
		var entity = new HttpEntity<>(items, httpHeaders);
		
		var urlTemplate = UriComponentsBuilder.fromHttpUrl(fineractApiUrl)
				.path("/batches")
				.queryParam("enclosingTransaction", true)
				.encode()
				.toUriString();
		
		logger.debug(">> Sending {} to {} with headers {}", items, urlTemplate, httpHeaders);
		
		ObjectMapper om = new ObjectMapper();
		String body = om.writeValueAsString(items);
		
		int retryCount = idempotencyRetryCount;
		httpHeaders.remove(idempotencyKeyHeaderName);
		httpHeaders.set(idempotencyKeyHeaderName, internalCorrelationId);
		
		while (retryCount > 0) {
			try {
				wireLogger.sending(body.toString());
				ResponseEntity<Object> response = restTemplate.exchange(urlTemplate, HttpMethod.POST, entity, Object.class);
				wireLogger.receiving(response.toString());
				
				logger.debug("<< Received {} with body type {}", response, response.getBody().getClass());
				List<LinkedHashMap<String, Object>> responseBody = (List<LinkedHashMap<String, Object>>) response.getBody();
				for (LinkedHashMap<String, Object> responseItem : responseBody) {
					Integer statusCode = (Integer) responseItem.get("statusCode");
					if (statusCode != 200) {
						retryCount = 0;
						throw new RuntimeException("An unexpected error occurred");
					}
				}
				return;
			} catch (HttpClientErrorException e) {
				logger.error(e.getMessage(), e);
				if (HttpStatus.CONFLICT.equals(e.getStatusCode())) {
					logger.warn("Transaction is already executing, has not completed yet");
					break;
				} else {
					logger.warn("Transaction returned with status code {}", e.getRawStatusCode());
				}
				logger.warn(e.getMessage(), e);
				throw new RuntimeException(e);
			} catch (RuntimeException e) {
				throw e;
			} catch (Exception e) {
				if (e instanceof InterruptedException
						|| (e instanceof ResourceAccessException
								&& (e.getCause() instanceof SocketTimeoutException
										|| e.getCause() instanceof ConnectException))) {
					logger.warn("Communication with Fineract timed out, retrying transaction {} more times with idempotency header value {}", retryCount, internalCorrelationId);
				} else {
					logger.error(e.getMessage(), e);
				}
				try {
					Thread.sleep(idempotencyRetryInterval);
				} catch (InterruptedException ie) {
					logger.error(ie.getMessage(), ie);
					throw new RuntimeException(ie);
				}
			}
			retryCount--;
		}
		
		throw new RuntimeException("An unexpected error occurred");
	}
}
