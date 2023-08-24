package org.mifos.connector.ams.zeebe.workers.bookamount;

import com.baasflow.commons.events.EventService;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.log.IOTxLogger;
import org.mifos.connector.ams.zeebe.workers.utils.AuthTokenHelper;
import org.mifos.connector.ams.zeebe.workers.utils.HoldAmountBody;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.LinkedHashMap;
import java.util.List;

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
                eventBuilder -> EventLogUtil.initFineractCall(urlTemplate, eventBuilder),
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
                eventBuilder -> EventLogUtil.initFineractCall(urlTemplate, eventBuilder),
                eventBuilder -> restTemplate.exchange(urlTemplate,
                        HttpMethod.POST,
                        entity,
                        Object.class));
    }

    protected void doBatch(List<TransactionItem> items,
                           String tenantId,
                           Integer disposalAccountId,
                           Integer conversionAccountId,
                           String internalCorrelationId,
                           String calledFrom) {
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initFineractBatchCall(calledFrom,
                        items,
                        disposalAccountId,
                        conversionAccountId,
                        internalCorrelationId,
                        eventBuilder),
                eventBuilder -> doBatchInternal(items, tenantId, internalCorrelationId));
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
                eventBuilder -> doBatchInternal(items, tenantId, internalCorrelationId));
    }

    @SuppressWarnings("unchecked")
    private Void doBatchInternal(List<TransactionItem> items, String tenantId, String internalCorrelationId) {
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

        log.debug(">> Sending {} to {} with headers {}", items, urlTemplate, httpHeaders);

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(Include.NON_NULL);
        String body;
        try {
            body = objectMapper.writeValueAsString(items);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("failed to create json from batch items", e);
        }

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
                log.debug("Response is " + (response == null ? "" : "not ") + "null");
            } catch (ResourceAccessException e) {
                if (e.getCause() instanceof SocketTimeoutException
                        || e.getCause() instanceof ConnectException) {
                    log.warn("Communication with Fineract timed out");
                    retryCount--;
                    if (retryCount > 0) {
                        log.warn("Retrying transaction {} more times with idempotency header value {}", retryCount, internalCorrelationId);
                    }
                } else {
                    log.error(e.getMessage(), e);
                    throw e;
                }
                try {
                    Thread.sleep(idempotencyRetryInterval);
                } catch (InterruptedException ie) {
                    log.error(ie.getMessage(), ie);
                    throw new RuntimeException(ie);
                }
                continue retry;
            } catch (RestClientException e) {
                // Some other exception occurred, one not related to timeout
                log.error(e.getMessage(), e);
                throw e;
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
                throw new RuntimeException(t);
            }

            List<LinkedHashMap<String, Object>> responseBody = (List<LinkedHashMap<String, Object>>) response.getBody();

            boolean allOk = true;

            for (LinkedHashMap<String, Object> responseItem : responseBody) {
                log.debug("Investigating {}", responseItem);
                int statusCode = (Integer) responseItem.get("statusCode");
                log.debug("Got {} for status code", statusCode);
                allOk &= (statusCode == 200);
                if (!allOk) {
                    switch (statusCode) {
                        case 403:
                            LinkedHashMap<String, Object> response403Body = (LinkedHashMap<String, Object>) responseItem.get("body");
                            log.debug("Got response body {}", response403Body);
                            String defaultUserMessage403 = (String) response403Body.get("defaultUserMessage");
                            if (defaultUserMessage403.contains("OptimisticLockException")) {
                                log.info("Optimistic lock exception detected, retrying in a short while");
                                log.debug("Current Idempotency-Key HTTP header expired, a new one is generated");
                                idempotencyPostfix++;
                                retryCount--;
                                continue retry;
                            }
                            break retry;
                        case 500:
                            String response500Body = (String) responseItem.get("body");
                            log.debug("Got response body {}", response500Body);
                            if (response500Body.contains("NullPointerException")) {
                                log.info("Possible optimistic lock exception, retrying in a short while");
                                log.debug("Current Idempotency-Key HTTP header expired, a new one is generated");
                                idempotencyPostfix++;
                                retryCount--;
                                continue retry;
                            }
                            break retry;
                        case 409:
                            log.warn("Transaction is already executing, has not completed yet");
                            return null;

                        default:
                            throw new RuntimeException("An unexpected error occurred");
                    }
                }
            }

            if (allOk) {
                return null;
            }

            log.info("{} more attempts", retryCount);
        }

        log.error("Failed to execute transaction in {} tries.", idempotencyRetryCount - retryCount + 1);
        throw new RuntimeException("Failed to execute transaction.");
    }
}