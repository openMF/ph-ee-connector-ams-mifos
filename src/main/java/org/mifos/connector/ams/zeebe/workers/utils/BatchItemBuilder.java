package org.mifos.connector.ams.zeebe.workers.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;

@Component
public class BatchItemBuilder {

	private String tenantId;
	
	@Autowired
	private AuthTokenHelper authTokenHelper;
	
	public BatchItemBuilder tenantId(String tenantId) {
		this.tenantId = tenantId;
		return this;
	}
	
	public void add(List<TransactionItem> items, String url, String body, boolean isDetails) throws JsonProcessingException {
		items.add(createTransactionItem(items.size() + 1, url, tenantId, body, isDetails ? items.size() : null));
	}
	
	private TransactionItem createTransactionItem(Integer requestId, String relativeUrl, String tenantId, String bodyItem, Integer reference) throws JsonProcessingException {
		List<Header> headers = headers(tenantId);
		return new TransactionItem(requestId, relativeUrl, "POST", reference, headers, bodyItem);
	}
	
	private List<Header> headers(String tenantId) {
		List<Header> headers = new ArrayList<>();
		headers.add(new Header("Idempotency-Key", UUID.randomUUID().toString()));
		headers.add(new Header("Content-Type", "application/json"));
		headers.add(new Header("Fineract-Platform-TenantId", tenantId));
		headers.add(new Header("Authorization", authTokenHelper.generateAuthToken()));
		return headers;
	}
}