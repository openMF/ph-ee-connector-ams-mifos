package org.mifos.connector.ams.zeebe.workers.utils;

import java.util.ArrayList;
import java.util.Collections;
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
		return Collections.unmodifiableList(new ArrayList<>() {
			private static final long serialVersionUID = 1L;

			{
				add(new Header("Idempotency-Key", internalCorrelationId));
				add(new Header("Content-Type", "application/json"));
				add(new Header("Fineract-Platform-TenantId", tenantId));
				add(new Header("Authorization", authToken));
			}
		});
	}
}
