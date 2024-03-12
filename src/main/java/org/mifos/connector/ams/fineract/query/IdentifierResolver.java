package org.mifos.connector.ams.fineract.query;

import org.mifos.connector.ams.fineract.currentaccount.response.Identifier;
import org.mifos.connector.ams.fineract.currentaccount.response.IdentifiersResponse;
import org.mifos.connector.ams.zeebe.workers.utils.AuthTokenHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/query")
public class IdentifierResolver {

    Logger logger = LoggerFactory.getLogger(IdentifierResolver.class);

    @Value("${fineract.api-url}")
    protected String fineractApiUrl;

    @Value("${fineract.datatable-query-api}")
    private String datatableQueryApi;

    @Value("${fineract.internal-id-column}")
    private String columnFilter;

    @Value("${fineract.column-filter}")
    private String resultColumns;

    @Value("${ams.account-type-key.conversion}")
    private String conversionSub;

    @Value("${ams.account-type-key.disposal}")
    private String disposalSub;

    @Value("${fineract.current-account-api}")
    private String accountUrl;

    @Value("${fineract.current-account-internal-account-id-type-name:alias}")
    private String alias;

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private AuthTokenHelper authTokenHelper;

    @GetMapping
    public List<?> retrieve(@RequestHeader("internalAccountId") String internalAccountId, @RequestHeader("Fineract-Platform-TenantId") String tenantId) {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
        httpHeaders.set("Authorization", authTokenHelper.generateAuthToken());
        httpHeaders.set("Fineract-Platform-TenantId", tenantId);
        logger.info("Sending http request with the following headers: {}", httpHeaders);
        try {
            return List.of(
                    Objects.requireNonNull(
                            restTemplate.exchange(
                            UriComponentsBuilder
                                    .fromHttpUrl(fineractApiUrl)
                                    .path(accountUrl)
                                    .pathSegment(alias, internalAccountId, disposalSub, "identifiers")
                                    .encode().toUriString(),
                            HttpMethod.GET,
                            new HttpEntity<>(httpHeaders),
                            IdentifiersResponse.class).getBody()).getSecondaryIdentifiers().stream()
                            .collect(Collectors.toMap(Identifier::getIdType, Identifier::getValue)));
        } catch (HttpClientErrorException.NotFound e) {
            return restTemplate.exchange(
                            UriComponentsBuilder
                                    .fromHttpUrl(fineractApiUrl)
                                    .path(datatableQueryApi)
                                    .queryParam("columnFilter", columnFilter)
                                    .queryParam("valueFilter", internalAccountId)
                                    .queryParam("resultColumns", resultColumns)
                                    .encode().toUriString(),
                            HttpMethod.GET,
                            new HttpEntity<>(httpHeaders),
                            List.class)
                    .getBody();
        }

    }
}