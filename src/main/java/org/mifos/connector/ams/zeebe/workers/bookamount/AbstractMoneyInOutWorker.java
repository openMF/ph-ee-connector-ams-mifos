package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.LinkedHashMap;
import java.util.List;

import org.mifos.connector.ams.log.IOTxLogger;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.camunda.zeebe.client.api.worker.JobHandler;

@Component
public abstract class AbstractMoneyInOutWorker implements JobHandler {
	
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
	
	@Value("${fineract.auth-token}")
	private String authToken;

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	protected static final String FORMAT = "yyyyMMdd";
	
	@SuppressWarnings("unchecked")
	protected void doBatch(List<TransactionItem> items, String tenantId, String internalCorrelationId) throws JsonProcessingException {
		HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
		httpHeaders.set("Authorization", "Basic " + authToken);
		httpHeaders.set("Fineract-Platform-TenantId", tenantId);
		int idempotencyPostfix = 0;
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
		httpHeaders.set(idempotencyKeyHeaderName, String.format("%s_%d", internalCorrelationId, idempotencyPostfix));
		
		retry:
		while (retryCount > 0) {
			wireLogger.sending(body.toString());
			ResponseEntity<Object> response = null;
			try {
				response = restTemplate.exchange(urlTemplate, HttpMethod.POST, entity, Object.class);
				wireLogger.receiving(response.toString());
			} catch (ResourceAccessException e) {
				// Do we have a timeout?
				if (e.getCause() instanceof SocketTimeoutException
						|| e.getCause() instanceof ConnectException) {
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
			} catch (RestClientException e) {
				// Some other exception occurred, one which is not related to timeout
				logger.error(e.getMessage(), e);
				throw e;
			}
			
			// Do we have an optimistic lock exception?
			List<LinkedHashMap<String, Object>> responseBody = (List<LinkedHashMap<String, Object>>) response.getBody();
			
			for (LinkedHashMap<String, Object> responseItem : responseBody) {
				int statusCode = (Integer) responseItem.get("statusCode");
				if (statusCode != 200) {
					// If it's 403 due to optimistic lock exception, we're retrying in a short while
					if (statusCode == 403) {
						LinkedHashMap<String, Object> responseItemBody = (LinkedHashMap<String, Object>) responseItem.get("body");
						String defaultUserMessage = (String) responseItemBody.get("defaultUserMessage");
						if (defaultUserMessage.contains("OptimisticLockException")) {
							// The current Idempotency key header expires, a new one is required
							idempotencyPostfix++;
							continue retry;
						}
					} else {
						if (statusCode == 409) {
							// If it's 409 due to the transaction already being in progress
							logger.warn("Transaction is already executing, has not completed yet");
							return;
						}
					}
					
					// Otherwise something else happened. The Zeebe flow will take care of it.
					throw new RuntimeException("An unexpected error occurred");
				}
			}
				
			retryCount--;
		}
		
		logger.error("Failed to execute transaction in {} tries.", idempotencyRetryCount);
		throw new RuntimeException("Failed to execute transaction.");
	}
}
