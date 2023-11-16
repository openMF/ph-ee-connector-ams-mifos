package org.mifos.connector.ams.zeebe.workers.bookamount;

import static org.apache.hc.core5.http.HttpStatus.SC_CONFLICT;
import static org.apache.hc.core5.http.HttpStatus.SC_LOCKED;
import static org.apache.hc.core5.http.HttpStatus.SC_OK;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Map;

import org.apache.fineract.client.models.BatchResponse;
import org.apache.fineract.client.models.CommandProcessingResult;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.log.IOTxLogger;
import org.mifos.connector.ams.zeebe.workers.utils.AuthTokenHelper;
import org.mifos.connector.ams.zeebe.workers.utils.HoldAmountBody;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;
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

import com.baasflow.commons.events.EventLogLevel;
import com.baasflow.commons.events.EventService;
import com.baasflow.commons.events.EventType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
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
    
    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private EventService eventService;

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

        log.trace("calling {} with HttpHeaders {}", urlTemplate, httpHeaders);

        return eventService.auditedEvent(
                // TODO internalCorrelationId?
                eventBuilder -> EventLogUtil.initFineractCall(urlTemplate, currencyAccountAmsId, -1, null, eventBuilder),
                eventBuilder -> restTemplate.exchange(urlTemplate,
                        HttpMethod.POST,
                        entity,
                        Object.class));
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

        log.trace("calling {} with HttpHeaders {}", urlTemplate, httpHeaders);

        return eventService.auditedEvent(
                // TODO internalCorrelationId?
                eventBuilder -> EventLogUtil.initFineractCall(urlTemplate, currencyAccountAmsId, -1, null, eventBuilder),
                eventBuilder -> restTemplate.exchange(urlTemplate,
                        HttpMethod.POST,
                        entity,
                        Object.class));
    }
    
    protected Long holdBatch(List<TransactionItem> items,
            String tenantId,
            Integer disposalAccountId,
            Integer conversionAccountId,
            String internalCorrelationId,
            String calledFrom) {
    	return eventService.auditedEvent(
    			eventBuilder -> EventLogUtil.initFineractBatchCall(calledFrom,
    					items,
    					disposalAccountId,
    					conversionAccountId,
    					internalCorrelationId,
    					eventBuilder),
    			eventBuilder -> holdBatchInternal(items, tenantId, internalCorrelationId, calledFrom));
    }

    protected String doBatch(List<TransactionItem> items,
                           String tenantId,
                           Integer disposalAccountId,
                           Integer conversionAccountId,
                           String internalCorrelationId,
                           String calledFrom) {
        return eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initFineractBatchCall(calledFrom,
                        items,
                        disposalAccountId,
                        conversionAccountId,
                        internalCorrelationId,
                        eventBuilder),
                eventBuilder -> doBatchInternal(items, tenantId, internalCorrelationId, calledFrom));
    }

    protected void doBatchOnUs(List<TransactionItem> items,
                               String tenantId,
                               Integer debtorDisposalAccountAmsId,
                               Integer debtorConversionAccountAmsId,
                               Integer creditorDisposalAccountAmsId,
                               String internalCorrelationId) {
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initFineractBatchCallOnUs("transferTheAmountBetweenDisposalAccounts",
                        items,
                        debtorDisposalAccountAmsId,
                        debtorConversionAccountAmsId,
                        creditorDisposalAccountAmsId,
                        internalCorrelationId,
                        eventBuilder),
                eventBuilder -> doBatchInternal(items, tenantId, internalCorrelationId, "transferTheAmountBetweenDisposalAccounts"));
    }
    
    private Long holdBatchInternal(List<TransactionItem> items, String tenantId, String internalCorrelationId, String from) {
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

        log.debug(">> Sending {} to {} with headers {} and idempotency {}", items, urlTemplate, httpHeaders, internalCorrelationId);

        int retryCount = idempotencyRetryCount;

        retry:
        while (retryCount > 0) {
            httpHeaders.remove(idempotencyKeyHeaderName);
            String idempotencyKey = String.format("%s_%d", internalCorrelationId, idempotencyPostfix);
            httpHeaders.set(idempotencyKeyHeaderName, idempotencyKey);
            wireLogger.sending(items.toString());
            eventService.sendEvent(builder -> builder
            		.setSourceModule("ams_connector")
            		.setEventLogLevel(EventLogLevel.INFO)
            		.setEvent(from + " - holdBatchInternal")
            		.setEventType(EventType.audit)
            		.setCorrelationIds(Map.of("idempotencyKey", idempotencyKey))
            		.setPayload(entity.toString()));
            ResponseEntity<String> response;
			try {
                response = restTemplate.exchange(urlTemplate, HttpMethod.POST, entity, String.class);
                eventService.sendEvent(builder -> builder
                		.setSourceModule("ams_connector")
                		.setEventLogLevel(EventLogLevel.INFO)
                		.setEvent(from + " - holdBatchInternal")
                		.setEventType(EventType.audit)
                		.setCorrelationIds(Map.of("idempotencyKey", idempotencyKey))
                		.setPayload(response.toString()));
                wireLogger.receiving(response.getBody());
            } catch (ResourceAccessException e) {
                if (e.getCause() instanceof SocketTimeoutException || e.getCause() instanceof ConnectException) {
                    log.warn("Communication with Fineract timed out for request [{}]", idempotencyKey);
                    retryCount--;
                    if (retryCount > 0) {
                        log.warn("Retrying request [{}], {} more times", internalCorrelationId, retryCount);
                    }
                } else {
                    log.error(e.getMessage(), e);
                    throw e;
                }
                continue;
            } catch (RestClientException e) {
                // Some other exception occurred, one not related to timeout
                log.error(e.getMessage(), e);
                throw e;
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
                throw new RuntimeException(t);
            }
            
			String responseBody = response.getBody();

			List<BatchResponse> batchResponseList;
			try {
				JsonNode rootNode = objectMapper.readTree(responseBody);
	            if (rootNode.isTextual()) {
	            	throw new RuntimeException(responseBody);
	            }
	            
	            batchResponseList = objectMapper.readValue(responseBody, new TypeReference<List<BatchResponse>>() {});
	            if (batchResponseList == null) {
	                return null;
	            }
			} catch (JsonProcessingException j) {
				log.error(j.getMessage(), j);
				throw new RuntimeException(j);
			}
			if (batchResponseList.size() != 2) {
				throw new RuntimeException("An unexpected error occurred for hold request " + idempotencyKey);
			}
            BatchResponse responseItem = batchResponseList.get(0);
            log.debug("Investigating response item {} for request [{}]", responseItem, idempotencyKey);
            int statusCode = responseItem.getStatusCode();
            log.debug("Got status code {} for request [{}]", statusCode, idempotencyKey);
            if (statusCode == SC_OK) {
                String responseItemBody = responseItem.getBody();
                JsonNode rootNode = null;
                try {
                	rootNode = objectMapper.readTree(responseItemBody);
    	            if (rootNode.isTextual()) {
    	            	throw new RuntimeException(responseItemBody);
    	            }
    	            
    	            CommandProcessingResult commandProcessingResult = objectMapper.readValue(responseItemBody, CommandProcessingResult.class);
    	            return commandProcessingResult.getResourceId();
                } catch (JsonProcessingException j) {
                	throw new RuntimeException("An unexpected error occurred for hold request " + idempotencyKey + ": " + rootNode);
                }
            }
            log.debug("Got error {}, response item '{}' for request [{}]", statusCode, responseItem, idempotencyKey);
            if (responseItem.getBody().contains("validation.msg.savingsaccount.insufficient.balance")) {
            	throw new ZeebeBpmnError("Error_FailedCreditTransfer", "Insufficient balance error");
            }
            switch (statusCode) {
                case SC_CONFLICT -> {
                    log.warn("Transaction request [{}] is already executing, has not completed yet", idempotencyKey);
                    return null;
                }
                case SC_LOCKED -> {
                    log.info("Locking exception detected, retrying request [{}]", idempotencyKey);
                    idempotencyPostfix++;
                    retryCount--;
                    continue retry;
                }
                default -> throw new RuntimeException("An unexpected error occurred for request " + idempotencyKey + ": " + statusCode);
            }
        }

        log.error("Failed to execute transaction request [{}] in {} tries.", internalCorrelationId, idempotencyRetryCount - retryCount + 1);
        throw new RuntimeException("Failed to execute transaction " + internalCorrelationId);
    }

    private String doBatchInternal(List<TransactionItem> items, String tenantId, String internalCorrelationId, String from) {
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

        log.debug(">> Sending {} to {} with headers {} and idempotency {}", items, urlTemplate, httpHeaders, internalCorrelationId);

        int retryCount = idempotencyRetryCount;

        retry:
        while (retryCount > 0) {
            httpHeaders.remove(idempotencyKeyHeaderName);
            String idempotencyKey = String.format("%s_%d", internalCorrelationId, idempotencyPostfix);
            httpHeaders.set(idempotencyKeyHeaderName, idempotencyKey);
            wireLogger.sending(items.toString());
            eventService.sendEvent(builder -> builder
            		.setSourceModule("ams_connector")
            		.setEventLogLevel(EventLogLevel.INFO)
            		.setEvent(from + " - doBatchInternal")
            		.setEventType(EventType.audit)
            		.setCorrelationIds(Map.of("idempotencyKey", idempotencyKey))
            		.setPayload(entity.toString()));
            ResponseEntity<String> response;
            try {
                response = restTemplate.exchange(urlTemplate, HttpMethod.POST, entity, String.class);
                eventService.sendEvent(builder -> builder
                		.setSourceModule("ams_connector")
                		.setEventLogLevel(EventLogLevel.INFO)
                		.setEvent(from + " - doBatchInternal")
                		.setEventType(EventType.audit)
                		.setCorrelationIds(Map.of("idempotencyKey", idempotencyKey))
                		.setPayload(response.toString()));
                wireLogger.receiving(response.toString());
            } catch (ResourceAccessException e) {
                if (e.getCause() instanceof SocketTimeoutException || e.getCause() instanceof ConnectException) {
                    log.warn("Communication with Fineract timed out for request [{}]", idempotencyKey);
                    retryCount--;
                    if (retryCount > 0) {
                        log.warn("Retrying request [{}], {} more times", internalCorrelationId, retryCount);
                    }
                } else {
                    log.error(e.getMessage(), e);
                    throw e;
                }
                continue;
            } catch (RestClientException e) {
                // Some other exception occurred, one not related to timeout
                log.error(e.getMessage(), e);
                throw e;
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
                throw new RuntimeException(t);
            }
            
			String responseBody = response.getBody();

			List<BatchResponse> batchResponseList;
			try {
				JsonNode rootNode = objectMapper.readTree(responseBody);
	            if (rootNode.isTextual()) {
	            	throw new RuntimeException(responseBody);
	            }
	            
	            batchResponseList = objectMapper.readValue(responseBody, new TypeReference<List<BatchResponse>>() {});
	            if (batchResponseList == null) {
	                return null;
	            }
			} catch (JsonProcessingException j) {
				log.error(j.getMessage(), j);
				throw new RuntimeException(j);
			}
            for (BatchResponse responseItem : batchResponseList) {
                log.debug("Investigating response item {} for request [{}]", responseItem, idempotencyKey);
                int statusCode = responseItem.getStatusCode();
                log.debug("Got status code {} for request [{}]", statusCode, idempotencyKey);
                if (statusCode == SC_OK) {
                    continue;
                }
                log.debug("Got error {}, response item '{}' for request [{}]", statusCode, responseItem, idempotencyKey);
                switch (statusCode) {
                    case SC_CONFLICT -> {
                        log.warn("Transaction request [{}] is already executing, has not completed yet", idempotencyKey);
                        return null;
                    }
                    case SC_LOCKED -> {
                        log.info("Locking exception detected, retrying request [{}]", idempotencyKey);
                        idempotencyPostfix++;
                        retryCount--;
                        continue retry;
                    }
                    default -> throw new RuntimeException("An unexpected error occurred for request " + idempotencyKey + ": " + statusCode);
                }
            }
            BatchResponse lastResponseItem = batchResponseList.get(Math.min(4, batchResponseList.size()) - 1);
            
            String lastResponseBody = lastResponseItem.getBody();
            try {
				CommandProcessingResult cpResult = objectMapper.readValue(lastResponseBody, CommandProcessingResult.class);
	            return cpResult.getTransactionId();
			} catch (JsonProcessingException j) {
				log.error(j.getMessage(), j);
				throw new RuntimeException(j);
			} finally {
				log.info("Request [{}] successful", idempotencyKey);
			}
        }

        log.error("Failed to execute transaction request [{}] in {} tries.", internalCorrelationId, idempotencyRetryCount - retryCount + 1);
        throw new RuntimeException("Failed to execute transaction " + internalCorrelationId);
    }
}