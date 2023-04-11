package org.mifos.connector.ams.zeebe.workers.utils;

import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import com.fasterxml.jackson.core.JsonProcessingException;

public class BatchItemBuilder {

	private String internalCorrelationId;
	private String tenantId;
	
	@Value("${fineract.auth-token}")
	private String authToken;
	
	public BatchItemBuilder(String internalCorrelationId, String tenantId) {
		this.internalCorrelationId = internalCorrelationId;
		this.tenantId = tenantId;
	}
	
	public void add(List<TransactionItem> items, String url, String body, boolean isDetails) throws JsonProcessingException {
		items.add(createTransactionItem(items.size() + 1, url, internalCorrelationId, tenantId, body, isDetails ? items.size() : null));
	}
	
	private TransactionItem createTransactionItem(Integer requestId, String relativeUrl, String internalCorrelationId, String tenantId, String bodyItem, Integer reference) throws JsonProcessingException {
		HttpHeaders headers = headers(internalCorrelationId, tenantId);
		return new TransactionItem(requestId, relativeUrl, "POST", reference, headers, bodyItem);
	}
	
	private HttpHeaders headers(String internalCorrelationId, String tenantId) {
		HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.set("Idempotency-Key", internalCorrelationId);
		httpHeaders.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
		httpHeaders.set("Authorization", "Basic " + authToken);
		httpHeaders.set("Fineract-Platform-TenantId", tenantId);
		return httpHeaders;
	}
}
