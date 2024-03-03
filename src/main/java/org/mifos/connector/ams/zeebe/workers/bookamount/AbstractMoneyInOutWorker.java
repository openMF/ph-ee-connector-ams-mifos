package org.mifos.connector.ams.zeebe.workers.bookamount;

import com.baasflow.commons.events.EventLogLevel;
import com.baasflow.commons.events.EventService;
import com.baasflow.commons.events.EventType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import lombok.extern.slf4j.Slf4j;
import org.apache.fineract.client.models.BatchResponse;
import org.apache.fineract.client.models.CommandProcessingResult;
import org.mifos.connector.ams.common.exception.FineractOptimisticLockingException;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.log.IOTxLogger;
import org.mifos.connector.ams.zeebe.workers.utils.AuthTokenHelper;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.retry.support.RetrySynchronizationManager;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Map;

import static org.apache.hc.core5.http.HttpStatus.SC_CONFLICT;
import static org.apache.hc.core5.http.HttpStatus.SC_FORBIDDEN;
import static org.apache.hc.core5.http.HttpStatus.SC_LOCKED;
import static org.apache.hc.core5.http.HttpStatus.SC_OK;

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


    @Value("${fineract.idempotency.key-header-name}")
    private String idempotencyKeyHeaderName;

    @Autowired
    private AuthTokenHelper authTokenHelper;

    @Autowired
    @Qualifier("painMapper")
    private ObjectMapper objectMapper;

    @Autowired
    private EventService eventService;

    protected static final String FORMAT = "yyyyMMdd";


    protected Long holdBatch(List<TransactionItem> items,
                             String tenantId,
                             String transactionGroupId,
                             String disposalAccountId,
                             String conversionAccountId,
                             String internalCorrelationId,
                             String calledFrom) {
        return eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initFineractBatchCall(calledFrom,
                        items,
                        disposalAccountId,
                        conversionAccountId,
                        internalCorrelationId,
                        eventBuilder),
                eventBuilder -> holdBatchInternal(transactionGroupId, items, tenantId, internalCorrelationId, calledFrom));
    }

    protected String doBatch(List<TransactionItem> items,
                             String tenantId,
                             String transactionGroupId,
                             String disposalAccountId,
                             String conversionAccountId,
                             String internalCorrelationId,
                             String calledFrom) {
        return eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initFineractBatchCall(calledFrom,
                        items,
                        disposalAccountId,
                        conversionAccountId,
                        internalCorrelationId,
                        eventBuilder),
                eventBuilder -> doBatchInternal(items, tenantId, transactionGroupId, internalCorrelationId, calledFrom));
    }

    protected void doBatchOnUs(List<TransactionItem> items,
                               String tenantId,
                               String transactionGroupId,
                               String debtorDisposalAccountAmsId,
                               String debtorConversionAccountAmsId,
                               String creditorDisposalAccountAmsId,
                               String internalCorrelationId) {
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initFineractBatchCallOnUs("transferTheAmountBetweenDisposalAccounts",
                        items,
                        debtorDisposalAccountAmsId,
                        debtorConversionAccountAmsId,
                        creditorDisposalAccountAmsId,
                        internalCorrelationId,
                        eventBuilder),
                eventBuilder -> doBatchInternal(items, tenantId, transactionGroupId, internalCorrelationId, "transferTheAmountBetweenDisposalAccounts"));
    }

    private Long holdBatchInternal(String transactionGroupId, List<TransactionItem> items, String tenantId, String internalCorrelationId, String from) {
        HttpHeaders httpHeaders = createHeaders(tenantId, transactionGroupId);
        HttpEntity<List<TransactionItem>> entity = new HttpEntity<>(items, httpHeaders);

        String urlTemplate = UriComponentsBuilder.fromHttpUrl(fineractApiUrl)
                .path("/batches")
                .queryParam("enclosingTransaction", true)
                .encode()
                .toUriString();

        log.debug(">> Sending {} to {} with headers {} and idempotency {}", items, urlTemplate, httpHeaders, internalCorrelationId);

        int retryCount = idempotencyRetryCount;

        return retryAbleHoldBatchInternal(httpHeaders, entity, urlTemplate, internalCorrelationId, from, idempotencyKeyHeaderName, items);
    }

    @Retryable(retryFor = FineractOptimisticLockingException.class, maxAttemptsExpression = "${fineract.idempotency.count:3}", backoff = @Backoff(delayExpression = "${fineract.idempotency.interval:10}"))
    private Long retryAbleHoldBatchInternal(HttpHeaders httpHeaders, HttpEntity entity, String urlTemplate, String internalCorrelationId, String from, String idempotencyKeyHeaderName, Object items) {
        int retryCount = RetrySynchronizationManager.getContext().getRetryCount();
        httpHeaders.remove(idempotencyKeyHeaderName);
        String idempotencyKey = String.format("%s_%d", internalCorrelationId, retryCount);
        httpHeaders.set(idempotencyKeyHeaderName, idempotencyKey);
        wireLogger.sending(items.toString());
        eventService.sendEvent(builder -> builder
                .setSourceModule("ams_connector")
                .setEventLogLevel(EventLogLevel.INFO)
                .setEvent(from + " - holdBatchInternal")
                .setEventType(EventType.audit)
                .setCorrelationIds(Map.of("idempotencyKey", idempotencyKey))
                .setPayload(entity.toString()));
        ResponseEntity<List<BatchResponse>> response;
        try {
            response = restTemplate.exchange(urlTemplate, HttpMethod.POST, entity, new ParameterizedTypeReference<List<BatchResponse>>() {
            });
            eventService.sendEvent(builder -> builder
                    .setSourceModule("ams_connector")
                    .setEventLogLevel(EventLogLevel.INFO)
                    .setEvent(from + " - holdBatchInternal")
                    .setEventType(EventType.audit)
                    .setCorrelationIds(Map.of("idempotencyKey", idempotencyKey))
                    .setPayload(response.toString()));
            wireLogger.receiving(response.toString());
        } catch (ResourceAccessException e) {
            if (e.getCause() instanceof SocketTimeoutException || e.getCause() instanceof ConnectException) {
                throw new FineractOptimisticLockingException(e.getMessage(), e.getCause());
            } else {
                log.error(e.getMessage(), e);
                throw e;
            }
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            throw new RuntimeException(t);
        }

        List<BatchResponse> batchResponseList = response.getBody();

        if (batchResponseList.size() != 2) {
            if (batchResponseList.get(0).getBody().contains("validation.msg.savingsaccount.insufficient.balance")) {
                throw new ZeebeBpmnError("Error_InsufficientFunds", "Insufficient balance error");
            }
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

        return handleResponseElementError(responseItem, statusCode, idempotencyKey);
    }

    private String doBatchInternal(List<TransactionItem> items, String tenantId, String transactionGroupId, String internalCorrelationId, String from) {
        HttpHeaders httpHeaders = createHeaders(tenantId, transactionGroupId);

        HttpEntity<List<TransactionItem>> entity = new HttpEntity<>(items, httpHeaders);

        String urlTemplate = UriComponentsBuilder.fromHttpUrl(fineractApiUrl)
                .path("/batches")
                .queryParam("enclosingTransaction", true)
                .encode()
                .toUriString();

        log.debug(">> Sending {} to {} with headers {} and idempotency {}", items, urlTemplate, httpHeaders, internalCorrelationId);
        return retryAbleBatchRequest(httpHeaders, entity, urlTemplate, internalCorrelationId, from, "X-Idempotency-Key", items).toString();

    }


    @Retryable(retryFor = FineractOptimisticLockingException.class, maxAttemptsExpression = "${fineract.idempotency.count:3}", backoff = @Backoff(delayExpression = "${fineract.idempotency.interval:10}"))
    private Object retryAbleBatchRequest(HttpHeaders httpHeaders, HttpEntity entity, String urlTemplate, String internalCorrelationId, String from, String idempotencyKeyHeaderName, Object items) {
        httpHeaders.remove(idempotencyKeyHeaderName);
        int retryCount = RetrySynchronizationManager.getContext().getRetryCount();
        String idempotencyKey = String.format("%s_%d", internalCorrelationId, retryCount);
        httpHeaders.set(idempotencyKeyHeaderName, idempotencyKey);
        wireLogger.sending(items.toString());
        eventService.sendEvent(builder -> builder
                .setSourceModule("ams_connector")
                .setEventLogLevel(EventLogLevel.INFO)
                .setEvent(from + " - doBatchInternal")
                .setEventType(EventType.audit)
                .setCorrelationIds(Map.of("idempotencyKey", idempotencyKey))
                .setPayload(entity.toString()));
        ResponseEntity<List<BatchResponse>> response = null;

        try {
            response = restTemplate.exchange(urlTemplate, HttpMethod.POST, entity, new ParameterizedTypeReference<List<BatchResponse>>() {
            });
            ResponseEntity<List<BatchResponse>> finalResponse = response;
            eventService.sendEvent(builder -> builder
                    .setSourceModule("ams_connector")
                    .setEventLogLevel(EventLogLevel.INFO)
                    .setEvent(from + " - doBatchInternal")
                    .setEventType(EventType.audit)
                    .setCorrelationIds(Map.of("idempotencyKey", idempotencyKey))
                    .setPayload(finalResponse.toString()));
            wireLogger.receiving(response.toString());
        } catch (ResourceAccessException e) {
            if (e.getCause() instanceof SocketTimeoutException || e.getCause() instanceof ConnectException) {
                throw new FineractOptimisticLockingException(e.getMessage(), e.getCause());
            } else {
                log.error(e.getMessage(), e);
                throw e;
            }

        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            throw new RuntimeException(t);
        }

        List<BatchResponse> batchResponseList = response.getBody();

        batchResponseList.stream().filter(element -> element.getStatusCode() != SC_OK).forEach(element -> {
            log.debug("Got status code {} for request [{}]", element.getStatusCode(), idempotencyKey);
            handleResponseElementError(element, element.getStatusCode(), idempotencyKey);
        });

        BatchResponse lastResponseItem = Iterables.getLast(batchResponseList);

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


    private Long handleResponseElementError(BatchResponse responseItem, int statusCode, String idempotencyKey) {
        log.debug("Got error {}, response item '{}' for request [{}]", statusCode, responseItem, idempotencyKey);
        switch (statusCode) {
            case SC_CONFLICT -> {
                log.warn("Transaction request [{}] is already executing, has not completed yet", idempotencyKey);
                return null;
            }
            case SC_LOCKED -> {
                log.info("Locking exception detected, retrying request [{}]", idempotencyKey);
                throw new FineractOptimisticLockingException("Locking exception detected retry transaction");
            }
            case SC_FORBIDDEN -> {
                String body = responseItem.getBody();
                if (body != null && (body.contains("error.msg.current.insufficient.funds"))) {
                    log.error("insufficient funds for request [{}]", idempotencyKey);
                    throw new ZeebeBpmnError("Error_InsufficientFunds", "Insufficient funds");
                }
                throw new ZeebeBpmnError("Error_CaughtException", "Forbidden");
            }
            default -> throw new RuntimeException("An unexpected error occurred for request " + idempotencyKey + ": " + statusCode);
        }
    }

    private HttpHeaders createHeaders(String tenantId, String transactionGroupId) {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
        httpHeaders.set("Authorization", authTokenHelper.generateAuthToken());
        httpHeaders.set("Fineract-Platform-TenantId", tenantId);
        httpHeaders.set("X-Correlation-ID", transactionGroupId);
        return httpHeaders;
    }
}
