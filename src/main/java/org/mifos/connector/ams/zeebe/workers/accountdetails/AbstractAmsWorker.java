package org.mifos.connector.ams.zeebe.workers.accountdetails;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import io.camunda.zeebe.client.api.worker.JobHandler;

public abstract class AbstractAmsWorker implements JobHandler {
	
	@Value("${fineract.api-url}")
	protected String fineractApiUrl;
	
	@Value("${fineract.datatable-query-api}")
	private String datatableQueryApi;

	@Value("${fineract.column-filter}")
	private String columnFilter;

	@Value("${fineract.result-columns}")
	private String resultColumns;
	
	@Autowired
	private RestTemplate restTemplate;
	
	@Autowired
	private HttpHeaders httpHeaders;
	
	public AbstractAmsWorker() {
	}

	public AbstractAmsWorker(RestTemplate restTemplate, HttpHeaders httpHeaders) {
		this.restTemplate = restTemplate;
		this.httpHeaders = httpHeaders;
	}
	
	protected AmsDataTableQueryResponse[] lookupAccount(String iban, String tenantId) {
		return exchange(UriComponentsBuilder
				.fromHttpUrl(fineractApiUrl)
				.path(datatableQueryApi)
				.queryParam("columnFilter", columnFilter)
				.queryParam("valueFilter", iban)
				.queryParam("resultColumns", resultColumns)
				.encode().toUriString(),
				AmsDataTableQueryResponse[].class,
				tenantId);
	}

	protected <T> T exchange(String urlTemplate, Class<T> responseType, String tenantId) {
		httpHeaders.add("Fineract-Platform-TenantId", tenantId);
		return restTemplate.exchange(
				urlTemplate, 
				HttpMethod.GET, 
				new HttpEntity<>(httpHeaders), 
				responseType)
			.getBody();
	}
}
