package org.mifos.connector.ams.zeebe.workers.bookamount;

import com.baasflow.commons.events.EventLogLevel;
import com.baasflow.commons.events.EventService;
import com.baasflow.commons.events.EventType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.fineract.client.models.BatchResponse;
import org.apache.fineract.client.models.CommandProcessingResult;
import org.mifos.connector.ams.common.exception.FineractOptimisticLockingException;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.log.IOTxLogger;
import org.mifos.connector.ams.zeebe.workers.utils.AuthTokenHelper;
import org.mifos.connector.ams.zeebe.workers.utils.BatchItem;
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
import org.springframework.retry.annotation.Recover;
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
import static org.apache.hc.core5.http.HttpStatus.SC_OK;
import static org.apache.hc.core5.http.HttpStatus.SC_TOO_EARLY;

@Component
@Slf4j
public class MoneyInOutWorker {

    @Getter
    @Autowired
    protected RestTemplate restTemplate;

    @Autowired
    private IOTxLogger wireLogger;

    @Getter
    @Value("${fineract.api-url}")
    protected String fineractApiUrl;

    @Value("${fineract.incoming-money-api}")
    protected String incomingMoneyApi;

    @Getter
    @Value("${fineract.locale}")
    protected String locale;

    @Autowired
    private AuthTokenHelper authTokenHelper;

    @Autowired
    @Qualifier("painMapper")
    private ObjectMapper objectMapper;

    @Autowired
    private EventService eventService;

    public static final String FORMAT = "yyyyMMdd";
    public static final String DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";

    @Retryable(retryFor = FineractOptimisticLockingException.class, maxAttemptsExpression = "${fineract.idempotency.count}", backoff = @Backoff(delayExpression = "${fineract.idempotency.interval}"))
    protected Long holdBatch(List<BatchItem> items,
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

    @Retryable(retryFor = FineractOptimisticLockingException.class, maxAttemptsExpression = "${fineract.idempotency.count}", backoff = @Backoff(delayExpression = "${fineract.idempotency.interval}"))
    public Pair<String, List<BatchResponse>> doBatch(List items,
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

    @Retryable(retryFor = FineractOptimisticLockingException.class, maxAttemptsExpression = "${fineract.idempotency.count}", backoff = @Backoff(delayExpression = "${fineract.idempotency.interval}"))
    protected void doBatchOnUs(List<BatchItem> items,
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

    @Recover
    public long recoverHoldBatch(FineractOptimisticLockingException e) {
        log.error(e.getMessage(), e);
        throw new ZeebeBpmnError("Error_CaughtException", "Failed to handle holdBatch request");
    }

    @Recover
    public Pair<String, List<BatchResponse>> recoverDoBatch(FineractOptimisticLockingException e) {
        log.error(e.getMessage(), e);
        throw new ZeebeBpmnError("Error_CaughtException", "Failed to handle doBatch request");
    }

    @Recover
    public void recoverDoBatchOnUs(FineractOptimisticLockingException e) {
        log.error(e.getMessage(), e);
        throw new ZeebeBpmnError("Error_CaughtException", "Failed to handle batchOnUs request");
    }

    @Recover
    public long recoverHoldBatch(ZeebeBpmnError e) {
        log.error(e.getMessage(), e);
        throw e;
    }

    @Recover
    public Pair<String, List<BatchResponse>> recoverDoBatch(ZeebeBpmnError e) {
        log.error(e.getMessage(), e);
        throw e;
    }

    @Recover
    public void recoverDoBatchOnUs(ZeebeBpmnError e) {
        log.error(e.getMessage(), e);
        throw e;
    }

    @Recover
    public void recoverDoBatch(RuntimeException e) {
        log.error(e.getMessage(), e);
        throw new ZeebeBpmnError("Error_CaughtException", "Failed to handle doBatch request");
    }

    private Long holdBatchInternal(String transactionGroupId, List<BatchItem> items, String tenantId, String internalCorrelationId, String from) {
        HttpHeaders httpHeaders = createHeaders(tenantId, transactionGroupId);
        HttpEntity<List<BatchItem>> entity = new HttpEntity<>(items, httpHeaders);

        String urlTemplate = UriComponentsBuilder.fromHttpUrl(fineractApiUrl)
                .path("/batches")
                .queryParam("enclosingTransaction", true)
                .encode()
                .toUriString();

        log.debug(">> Sending {} to {} with headers {} and idempotency {}", items, urlTemplate, httpHeaders, internalCorrelationId);

        return retryAbleHoldBatchInternal(entity, urlTemplate, internalCorrelationId, from, items);
    }

    private Long retryAbleHoldBatchInternal(HttpEntity entity, String urlTemplate, String internalCorrelationId, String from, Object items) {
        int retryCount = RetrySynchronizationManager.getContext().getRetryCount();
        log.debug("setting retry count to {} for internal correlation id {}", retryCount, internalCorrelationId);
        wireLogger.sending(items.toString());
        eventService.sendEvent(builder -> builder
                .setSourceModule("ams_connector")
                .setEventLogLevel(EventLogLevel.INFO)
                .setEvent(from + " - holdBatchInternal")
                .setEventType(EventType.audit)
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
            throw new RuntimeException("An unexpected error occurred for hold request");
        }
        BatchResponse responseItem = batchResponseList.get(0);
        log.debug("Investigating response item {} for request", responseItem);
        int statusCode = responseItem.getStatusCode();
        log.debug("Got status code {} for request", statusCode);
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
                throw new RuntimeException("An unexpected error occurred for hold request: " + rootNode);
            }
        }

        return handleResponseElementError(responseItem, statusCode, retryCount);
    }

    private Pair<String, List<BatchResponse>> doBatchInternal(List items, String tenantId, String transactionGroupId, String internalCorrelationId, String from) {
        HttpHeaders httpHeaders = createHeaders(tenantId, transactionGroupId);

        HttpEntity<List> entity = new HttpEntity<>(items, httpHeaders);

        String urlTemplate = UriComponentsBuilder.fromHttpUrl(fineractApiUrl)
                .path("/batches")
                .queryParam("enclosingTransaction", true)
                .encode()
                .toUriString();

        log.debug(">> Sending {} to {} with headers {} and idempotency {}", items, urlTemplate, httpHeaders, internalCorrelationId);
        int retryCount = RetrySynchronizationManager.getContext().getRetryCount();
        log.debug("setting retry count to {} for internal correlation id {}", retryCount, internalCorrelationId);

        String idempotencyKey = String.format("%s_%d", internalCorrelationId, retryCount);
        httpHeaders.set("X-Idempotency-Key", idempotencyKey);
        wireLogger.sending(items.toString());
        eventService.sendEvent(builder -> builder
                .setSourceModule("ams_connector")
                .setEventLogLevel(EventLogLevel.INFO)
                .setEvent(from + " - doBatchInternal")
                .setEventType(EventType.audit)
                .setCorrelationIds(Map.of("idempotencyKey", idempotencyKey))
                .setPayload(entity.toString()));
        ResponseEntity<List<BatchResponse>> response;

        try {
            response = restTemplate.exchange(urlTemplate, HttpMethod.POST, entity, new ParameterizedTypeReference<>() {
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
            handleResponseElementError(element, element.getStatusCode(), retryCount);
        });

        BatchResponse lastResponseItem = Iterables.getLast(batchResponseList);
        String lastResponseBody = lastResponseItem.getBody();
        try {
            CommandProcessingResult cpResult = objectMapper.readValue(lastResponseBody, CommandProcessingResult.class);
            return Pair.of(cpResult.getTransactionId(), batchResponseList);
        } catch (JsonProcessingException j) {
            log.error(j.getMessage(), j);
            throw new RuntimeException(j);
        }
    }


    private Long handleResponseElementError(BatchResponse response, int statusCode, int retryCount) {
        log.error("Got error {}, response: '{}'", statusCode, response.getBody());
        switch (statusCode) {
            case SC_CONFLICT -> {
                log.warn("Locking exception detected will retry for {} times", retryCount);
                throw new FineractOptimisticLockingException("Locking exception detected");
            }
            case SC_TOO_EARLY -> {
                log.info("Request have been send to early will retry for {} times", retryCount);
                throw new FineractOptimisticLockingException("Locking exception detected");
            }
            case SC_FORBIDDEN -> {
                String body = response.getBody();
                if (body != null && (body.contains("error.msg.current.insufficient.funds"))) {
                    log.error("insufficient funds for request");
                    throw new ZeebeBpmnError("Error_InsufficientFunds", "Insufficient funds");
                } else {
                    log.error("HTTP 403 returned by Fineract: {}", body);
                    throw new RuntimeException("HTTP 403 returned by Fineract: " + body);
                }
            }
            default -> throw new RuntimeException("An unexpected error occurred for request: " + statusCode);
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
