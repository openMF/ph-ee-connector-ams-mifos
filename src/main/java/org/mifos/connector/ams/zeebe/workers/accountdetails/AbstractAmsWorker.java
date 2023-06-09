package org.mifos.connector.ams.zeebe.workers.accountdetails;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Base64.Encoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

public abstract class AbstractAmsWorker {
	
	Logger logger = LoggerFactory.getLogger(AbstractAmsWorker.class);
	
	private static final Encoder ENCODER = Base64.getEncoder();
	
	@Value("${fineract.api-url}")
	protected String fineractApiUrl;
	
	@Value("${fineract.datatable-query-api}")
	private String datatableQueryApi;

	@Value("${fineract.column-filter}")
	private String columnFilter;

	@Value("${fineract.result-columns}")
	private String resultColumns;
	
	@Value("${fineract.auth-user}")
	private String authUser;
	
	@Value("${fineract.auth-password}")
	private String authPassword;
	
	@Autowired
	private RestTemplate restTemplate;
	
	public AbstractAmsWorker() {
	}

	public AbstractAmsWorker(RestTemplate restTemplate) {
		this.restTemplate = restTemplate;
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
		HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
		httpHeaders.set("Authorization", generateAuthToken(authUser, authPassword));
		httpHeaders.set("Fineract-Platform-TenantId", tenantId);
		logger.info("Sending http request with the following headers: {}", httpHeaders);
		return restTemplate.exchange(
				urlTemplate, 
				HttpMethod.GET, 
				new HttpEntity<>(httpHeaders), 
				responseType)
			.getBody();
	}
	
	public static String generateAuthToken(String user, String password) {
		String userPass = new StringBuilder(user).append(":").append(password).toString();
		StringBuilder sb = new StringBuilder("Basic ");
		sb.append(new String(ENCODER.encode(userPass.getBytes()), StandardCharsets.ISO_8859_1));
		return sb.toString();
	}
}
