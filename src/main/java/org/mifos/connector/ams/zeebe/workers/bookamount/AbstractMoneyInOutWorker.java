package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.LinkedHashMap;
import java.util.List;

import org.mifos.connector.ams.log.IOTxLogger;
import org.mifos.connector.ams.zeebe.workers.utils.AuthTokenHelper;
import org.mifos.connector.ams.zeebe.workers.utils.HoldAmountBody;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public abstract class AbstractMoneyInOutWorker {
	
	@Autowired
	protected RestTemplate restTemplate;
	
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

	@Autowired
	private AuthTokenHelper authTokenHelper;

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	protected static final String FORMAT = "yyyyMMdd";
	
	protected ResponseEntity<Object> release(Integer currencyAccountAmsId, Integer holdAmountId, String tenantId) {
		HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
		httpHeaders.set("Authorization", authTokenHelper.generateAuthToken());
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
		
		logger.info(">> Sending {} to {} with headers {}", null, urlTemplate, httpHeaders);
		
		ResponseEntity<Object> response = restTemplate.exchange(urlTemplate, HttpMethod.POST, entity, Object.class);
		
		logger.info("<< Received HTTP {}", response.getStatusCode());
		
		return response;
	}
	
	protected ResponseEntity<Object> hold(Integer holdReasonId, String transactionDate, Object amount, Integer currencyAccountAmsId, String tenantId) {
		var body = new HoldAmountBody(
				transactionDate,
				amount,
				holdReasonId,
				locale,
				FORMAT
				);
		return doExchange(body, currencyAccountAmsId, "holdAmount", tenantId);
	}
	
	protected <T> ResponseEntity<Object> doExchange(T body, Integer currencyAccountAmsId, String command, String tenantId) {
		HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
		httpHeaders.set("Authorization", authTokenHelper.generateAuthToken());
		httpHeaders.set("Fineract-Platform-TenantId", tenantId);
		var entity = new HttpEntity<>(body, httpHeaders);
		
		var urlTemplate = UriComponentsBuilder.fromHttpUrl(fineractApiUrl)
				.path(incomingMoneyApi)
				.path(String.format("%s", currencyAccountAmsId))
				.path("/transactions")
				.queryParam("command", command)
				.encode()
				.toUriString();
		
		logger.info(">> Sending {} to {} with headers {}", body, urlTemplate, httpHeaders);
		
		ResponseEntity<Object> response = restTemplate.exchange(urlTemplate, HttpMethod.POST, entity, Object.class);
		logger.info("<< Received HTTP {}", response.getStatusCode());
		return response;
	}
	
	@SuppressWarnings("unchecked")
	protected void doBatch(List<TransactionItem> items, String tenantId, String internalCorrelationId) throws JsonProcessingException {
		HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
		httpHeaders.set("Authorization", authTokenHelper.generateAuthToken());
		httpHeaders.set("Fineract-Platform-TenantId", tenantId);
		int idempotencyPostfix = 0;
		var entity = new HttpEntity<>(items, httpHeaders);
		
		var urlTemplate = UriComponentsBuilder.fromHttpUrl(fineractApiUrl)
				.path("/batches")
				.queryParam("enclosingTransaction", true)
				.encode()
				.toUriString();
		
		logger.debug(">> Sending {} to {} with headers {}", items, urlTemplate, httpHeaders);
		
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.setSerializationInclusion(Include.NON_NULL);
		String body = objectMapper.writeValueAsString(items);
		
		int retryCount = idempotencyRetryCount;
		
		retry:
		while (retryCount > 0) {
			httpHeaders.remove(idempotencyKeyHeaderName);
			httpHeaders.set(idempotencyKeyHeaderName, String.format("%s_%d", internalCorrelationId, idempotencyPostfix));
			wireLogger.sending(body);
			ResponseEntity<Object> response = null;
			try {
				response = restTemplate.exchange(urlTemplate, HttpMethod.POST, entity, Object.class);
				wireLogger.receiving(response.toString());
				logger.debug("Response is " + (response == null ? "" : "not ") + "null");
			} catch (ResourceAccessException e) {
				if (e.getCause() instanceof SocketTimeoutException
						|| e.getCause() instanceof ConnectException) {
					logger.warn("Communication with Fineract timed out");
					retryCount--;
					if (retryCount > 0) {
						logger.warn("Retrying transaction {} more times with idempotency header value {}", retryCount, internalCorrelationId);
					}
				} else {
					logger.error(e.getMessage(), e);
					throw e;
				}
				try {
					Thread.sleep(idempotencyRetryInterval);
				} catch (InterruptedException ie) {
					logger.error(ie.getMessage(), ie);
					throw new RuntimeException(ie);
				}
				continue retry;
			} catch (RestClientException e) {
				// Some other exception occurred, one not related to timeout
				logger.error(e.getMessage(), e);
				throw e;
			} catch (Throwable t) {
				logger.error(t.getMessage(), t);
				throw new RuntimeException(t);
			}
			
			List<LinkedHashMap<String, Object>> responseBody = (List<LinkedHashMap<String, Object>>) response.getBody();
			
			boolean allOk = true;
			
			for (LinkedHashMap<String, Object> responseItem : responseBody) {
				logger.debug("Investigating {}", responseItem);
				int statusCode = (Integer) responseItem.get("statusCode");
				logger.debug("Got {} for status code", statusCode);
				allOk &= (statusCode == 200);
				if (!allOk) {
					switch (statusCode) {
						case 403:
							LinkedHashMap<String, Object> response403Body = (LinkedHashMap<String, Object>) responseItem.get("body");
							logger.debug("Got response body {}", response403Body);
							String defaultUserMessage403 = (String) response403Body.get("defaultUserMessage");
							if (defaultUserMessage403.contains("OptimisticLockException")) {
								logger.info("Optimistic lock exception detected, retrying in a short while");
								logger.debug("Current Idempotency-Key HTTP header expired, a new one is generated");
								idempotencyPostfix++;
								retryCount--;
								continue retry;
							}
							break retry;
						case 500:
							String response500Body = (String) responseItem.get("body");
							logger.debug("Got response body {}", response500Body);
							if (response500Body.contains("NullPointerException")) {
								logger.info("Possible optimistic lock exception, retrying in a short while");
								logger.debug("Current Idempotency-Key HTTP header expired, a new one is generated");
								idempotencyPostfix++;
								retryCount--;
								continue retry;
							}
							break retry;
						case 409:
							logger.warn("Transaction is already executing, has not completed yet");
							return;
							
						default:
							throw new RuntimeException("An unexpected error occurred");
					}
				}
			}
			
			if (allOk) {
				return;
			}
			
			logger.info("{} more attempts", retryCount);
		}
		
		logger.error("Failed to execute transaction in {} tries.", idempotencyRetryCount - retryCount + 1);
		throw new RuntimeException("Failed to execute transaction.");
	}
}
