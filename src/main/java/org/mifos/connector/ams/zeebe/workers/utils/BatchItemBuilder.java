package org.mifos.connector.ams.zeebe.workers.utils;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;

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
		List<Header> headers = headers(internalCorrelationId, tenantId);
		return new TransactionItem(requestId, relativeUrl, "POST", reference, headers, bodyItem);
	}
	
	private List<Header> headers(String internalCorrelationId, String tenantId) {
		List<Header> headers = new ArrayList<>();
		headers.add(new Header("Idempotency-Key", internalCorrelationId));
		headers.add(new Header("Content-Type", "application/json"));
		headers.add(new Header("Fineract-Platform-TenantId", tenantId));
		headers.add(new Header("Authorization", "Basic " + authToken));
		return headers;
	}
}
