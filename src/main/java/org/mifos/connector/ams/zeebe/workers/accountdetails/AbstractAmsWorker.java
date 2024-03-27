package org.mifos.connector.ams.zeebe.workers.accountdetails;

import com.baasflow.commons.events.EventLogLevel;
import com.baasflow.commons.events.EventService;
import com.baasflow.commons.events.EventType;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.mifos.connector.ams.fineract.currentaccount.request.*;
import org.mifos.connector.ams.fineract.currentaccount.response.CAGetResponse;
import org.mifos.connector.ams.fineract.currentaccount.response.IdentifiersResponse;
import org.mifos.connector.ams.fineract.currentaccount.response.PageFineractResponse;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.zeebe.workers.utils.AuthTokenHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
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

    protected PageFineractResponse lookupCurrentAccountPostFlagsAndStatus(String iban, String accountSubValue, String tenantId) {
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


        return exchangeCurrentFlagsStatus(UriComponentsBuilder
                        .fromHttpUrl(fineractApiUrl)
                        .path(accountUrl)
                        .pathSegment("iban", iban, accountSubValue, "query")
                        .encode().toUriString(),
                PageFineractResponse.class,
                tenantId,
                fineractCurrentAccountRequest,
                "ams_connector",
                "lookupAccount");
    }

    protected CAGetResponse lookupCurrentAccountGet(String iban, String accountSubValue, String tenantId) {
        return lookupExchange(UriComponentsBuilder
                        .fromHttpUrl(fineractApiUrl)
                        .path(accountUrl)
                        .pathSegment("iban", iban, accountSubValue)
                        .encode().toUriString(),
                CAGetResponse.class,
                tenantId,
                "ams_connector",
                "lookupAccount");
    }

    protected IdentifiersResponse lookupIdentifiersGet(String iban, String accountSubValue, String tenantId) {
        return lookupExchange(UriComponentsBuilder
                        .fromHttpUrl(fineractApiUrl)
                        .path(accountUrl)
                        .pathSegment("iban", iban, accountSubValue, "identifiers")
                        .encode().toUriString(),
                IdentifiersResponse.class,
                tenantId,
                "ams_connector",
                "lookupAccount");
    }


    protected AmsDataTableQueryResponse[] lookupSavingsAccount(String iban, String tenantId) {
        return lookupExchange(UriComponentsBuilder
                        .fromHttpUrl(fineractApiUrl)
                        .path(datatableQueryApi)
                        .queryParam("columnFilter", columnFilter)
                        .queryParam("valueFilter", iban)
                        .queryParam("resultColumns", resultColumns)
                        .encode().toUriString(),
                AmsDataTableQueryResponse[].class,
                tenantId,
                "ams_connector",
                "lookupAccount");
    }

    @SuppressWarnings("unchecked")
    protected List<Object> lookupFlags(Long accountId, String tenantId) {
        List<LinkedHashMap<String, Object>> flags = lookupExchange(UriComponentsBuilder
                        .fromHttpUrl(fineractApiUrl)
                        .path(flagsQueryApi)
                        .queryParam("columnFilter", flagsColumnFilter)
                        .queryParam("valueFilter", accountId)
                        .queryParam("resultColumns", flagsResultColumns)
                        .encode().toUriString(),
                List.class,
                tenantId,
                "ams_connector",
                "lookupFlags");
        return flags.stream().map(flagResult -> flagResult.get(flagsResultColumns)).collect(Collectors.toList());
    }

    protected <T> T lookupExchange(String urlTemplate, Class<T> responseType, String tenantId, String calledFrom, String eventName) {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
        httpHeaders.set("Authorization", authTokenHelper.generateAuthToken());
        httpHeaders.set("Fineract-Platform-TenantId", tenantId);
        log.trace("calling {} with HttpHeaders {}", urlTemplate, httpHeaders);
        return eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initFineractCall(calledFrom, "-1", "-1", null, eventBuilder),
                eventBuilder -> {
                    var entity = new HttpEntity<>(httpHeaders);
                    eventService.sendEvent(builder -> builder
                            .setSourceModule(calledFrom)
                            .setEventLogLevel(EventLogLevel.INFO)
                            .setEvent(eventName)
                            .setEventType(EventType.audit)
                            .setPayload(urlTemplate));
                    ResponseEntity<T> response = restTemplate.exchange(
                            urlTemplate,
                            HttpMethod.GET,
                            entity,
                            responseType);
                    eventService.sendEvent(builder -> builder
                            .setSourceModule(calledFrom)
                            .setEventLogLevel(EventLogLevel.INFO)
                            .setEvent(eventName)
                            .setEventType(EventType.audit)
                            .setPayload(response.toString()));
                    return response.getBody();
                });
    }

    protected <T> T exchangeCurrentFlagsStatus(String urlTemplate, Class responseType, String tenantId, FineractCurrentAccountRequest requestBody, String calledFrom, String eventName) {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
        httpHeaders.set("Authorization", authTokenHelper.generateAuthToken());
        httpHeaders.set("Fineract-Platform-TenantId", tenantId);
        log.trace("calling {} with HttpHeaders {}", urlTemplate, httpHeaders);

        return eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initFineractCall(calledFrom, "-1", "-1", null, eventBuilder),
                eventBuilder -> {
                    var entity = new HttpEntity<>(requestBody, httpHeaders);
                    eventService.sendEvent(builder -> builder
                            .setSourceModule(calledFrom)
                            .setEventLogLevel(EventLogLevel.INFO)
                            .setEvent(eventName)
                            .setEventType(EventType.audit)
                            .setPayload(urlTemplate));
                    ResponseEntity<T> response = restTemplate.exchange(
                            urlTemplate,
                            HttpMethod.POST,
                            entity,
                            responseType);
                    eventService.sendEvent(builder -> builder
                            .setSourceModule(calledFrom)
                            .setEventLogLevel(EventLogLevel.INFO)
                            .setEvent(eventName)
                            .setEventType(EventType.audit)
                            .setPayload(response.toString()));
                    return response.getBody();
                });
    }


}