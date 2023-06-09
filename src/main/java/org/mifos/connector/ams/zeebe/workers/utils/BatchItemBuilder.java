package org.mifos.connector.ams.zeebe.workers.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.mifos.connector.ams.zeebe.workers.accountdetails.AbstractAmsWorker;
import org.springframework.beans.factory.annotation.Value;

import com.fasterxml.jackson.core.JsonProcessingException;

public class BatchItemBuilder {

	private final String tenantId;
	
	@Value("${fineract.auth-user}")
	private String authUser;
	
	@Value("${fineract.auth-password}")
	private String authPassword;
	
	public BatchItemBuilder(String tenantId) {
		this.tenantId = tenantId;
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
		headers.add(new Header("Authorization", AbstractAmsWorker.generateAuthToken(authUser, authPassword)));
		return headers;
	}
}
