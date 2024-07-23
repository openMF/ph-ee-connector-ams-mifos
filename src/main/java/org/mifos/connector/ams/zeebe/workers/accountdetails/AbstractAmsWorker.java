package org.mifos.connector.ams.zeebe.workers.accountdetails;

import com.baasflow.commons.events.EventLogLevel;
import com.baasflow.commons.events.EventService;
import com.baasflow.commons.events.EventType;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.mifos.connector.ams.fineract.currentaccount.request.BaseQuery;
import org.mifos.connector.ams.fineract.currentaccount.request.DatatableQuery;
import org.mifos.connector.ams.fineract.currentaccount.request.FineractCurrentAccountRequest;
import org.mifos.connector.ams.fineract.currentaccount.request.Query;
import org.mifos.connector.ams.fineract.currentaccount.request.Request;
import org.mifos.connector.ams.fineract.currentaccount.response.CAGetResponse;
import org.mifos.connector.ams.fineract.currentaccount.response.IdentifiersResponse;
import org.mifos.connector.ams.fineract.currentaccount.response.PageFineractResponse;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.zeebe.workers.utils.AuthTokenHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.ObjectUtils;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractAmsWorker {
    public static final String flagsResultColumns = "account_flag_list_cd_flag_code";
    public static final String currentAccountFlagsResultColumns = "account_flag_list_cd_flag_code";

    @Value("${fineract.api-url}")
    protected String fineractApiUrl;

    @Value("${fineract.current-account-api}")
    private String accountUrl;

    @Value("${fineract.datatable-query-api}")
    private String datatableQueryApi;

    @Value("${fineract.column-filter}")
    private String columnFilter;

    @Value("${fineract.result-columns}")
    private String resultColumns;

    @Value("${fineract.current-account-result-columns}")
    private Set<String> currentAccountResultColumns;

    @Value("${fineract.flags-query-api}")
    private String flagsQueryApi;

    @Value("${fineract.flags-column-filter}")
    private String flagsColumnFilter;

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private AuthTokenHelper authTokenHelper;

    @Autowired
    private EventService eventService;


    @PostConstruct
    public void setup() {
        if (ObjectUtils.isEmpty(fineractApiUrl)) {
            throw new IllegalStateException("FINERACT_API_URL is missing from environment variables");
        }

        log.info("using Fineract API URL: {}", fineractApiUrl);
    }

    protected PageFineractResponse lookupCurrentAccountWithFlags(String iban, String accountSubValue, String tenantId) {
        FineractCurrentAccountRequest fineractCurrentAccountRequest = new FineractCurrentAccountRequest()
                .request(
                        new Request()
                                .baseQuery(
                                        new BaseQuery()
                                                .resultColumns(currentAccountResultColumns))
                                .datatableQueries(
                                        List.of(new DatatableQuery()
                                                .table("dt_current_account_flags")
                                                .query(new Query().resultColumns(Set.of(currentAccountFlagsResultColumns))))))
                .page(0)
                .size(20);

        return httpPost(UriComponentsBuilder
                        .fromHttpUrl(fineractApiUrl)
                        .path(accountUrl)
                        .pathSegment("iban", iban, accountSubValue, "query")
                        .encode().toUriString(),
                PageFineractResponse.class,
                tenantId,
                fineractCurrentAccountRequest,
                "lookupAccount");
    }

    protected CAGetResponse lookupCurrentAccountByIban(String iban, String accountSubValue, String tenantId) {
        return httpGet(UriComponentsBuilder
                        .fromHttpUrl(fineractApiUrl)
                        .path(accountUrl)
                        .pathSegment("iban", iban, accountSubValue)
                        .encode().toUriString(),
                CAGetResponse.class,
                tenantId,
                "lookupAccount");
    }

    protected CAGetResponse lookupCurrentAccountByCardId(String cardAccountId, String accountSubValue, String tenantId) {
        return httpGet(UriComponentsBuilder
                        .fromHttpUrl(fineractApiUrl)
                        .path(accountUrl)
                        .pathSegment("cardAccountId", cardAccountId, accountSubValue)
                        .encode().toUriString(),
                CAGetResponse.class,
                tenantId,
                "lookupAccount");
    }

    protected IdentifiersResponse lookupIdentifiers(String iban, String accountSubValue, String tenantId) {
        return httpGet(UriComponentsBuilder
                        .fromHttpUrl(fineractApiUrl)
                        .path(accountUrl)
                        .pathSegment("iban", iban, accountSubValue, "identifiers")
                        .encode().toUriString(),
                IdentifiersResponse.class,
                tenantId,
                "lookupAccount");
    }

    protected IdentifiersResponse lookupIdentifiersByCardId(String cardAccountId, String accountSubValue, String tenantId) {
        return httpGet(UriComponentsBuilder
                        .fromHttpUrl(fineractApiUrl)
                        .path(accountUrl)
                        .pathSegment("cardAccountId", cardAccountId, accountSubValue, "identifiers")
                        .encode().toUriString(),
                IdentifiersResponse.class,
                tenantId,
                "lookupAccount");
    }


    protected AmsDataTableQueryResponse[] lookupSavingsAccount(String iban, String tenantId) {
        return httpGet(UriComponentsBuilder
                        .fromHttpUrl(fineractApiUrl)
                        .path(datatableQueryApi)
                        .queryParam("columnFilter", columnFilter)
                        .queryParam("valueFilter", iban)
                        .queryParam("resultColumns", resultColumns)
                        .encode().toUriString(),
                AmsDataTableQueryResponse[].class,
                tenantId,
                "lookupAccount");
    }

    @SuppressWarnings("unchecked")
    protected List<Object> lookupFlags(Long accountId, String tenantId) {
        List<LinkedHashMap<String, Object>> flags = httpGet(UriComponentsBuilder
                        .fromHttpUrl(fineractApiUrl)
                        .path(flagsQueryApi)
                        .queryParam("columnFilter", flagsColumnFilter)
                        .queryParam("valueFilter", accountId)
                        .queryParam("resultColumns", flagsResultColumns)
                        .encode().toUriString(),
                List.class,
                tenantId,
                "lookupFlags");
        return flags.stream().map(flagResult -> flagResult.get(flagsResultColumns)).collect(Collectors.toList());
    }

    protected <T> T httpGet(String url, Class<T> responseType, String tenantId, String eventName) {
        return httpRequest(tenantId, url, responseType, eventName, HttpMethod.GET, null);
    }

    protected <T> T httpPost(String url, Class<T> responseType, String tenantId, FineractCurrentAccountRequest requestBody, String eventName) {
        return httpRequest(tenantId, url, responseType, eventName, HttpMethod.POST, requestBody);
    }

    private <T, Y> T httpRequest(String tenantId, String url, Class<T> responseType, String eventName, HttpMethod method, Y requestBody) {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
        httpHeaders.set(HttpHeaders.AUTHORIZATION, authTokenHelper.generateAuthToken());
        httpHeaders.set("Fineract-Platform-TenantId", tenantId);
        HttpEntity<Y> entity = new HttpEntity<>(requestBody, httpHeaders);

        log.trace("calling {} {} with HttpHeaders {}", method, url, httpHeaders);
        return eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initFineractCall("ams_connector", "-1", "-1", null, eventBuilder),
                eventBuilder -> {
                    eventService.sendEvent(builder -> builder
                            .setSourceModule("ams_connector")
                            .setEventLogLevel(EventLogLevel.INFO)
                            .setEvent(eventName)
                            .setEventType(EventType.audit)
                            .setPayload(url));
                    ResponseEntity<T> response = restTemplate.exchange(
                            url,
                            method,
                            entity,
                            responseType);
                    eventService.sendEvent(builder -> builder
                            .setSourceModule("ams_connector")
                            .setEventLogLevel(EventLogLevel.INFO)
                            .setEvent(eventName)
                            .setEventType(EventType.audit)
                            .setPayload(response.toString()));
                    return response.getBody();
                });
    }
}