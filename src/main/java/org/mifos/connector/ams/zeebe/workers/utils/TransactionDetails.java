package org.mifos.connector.ams.zeebe.workers.utils;

import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

public record TransactionDetails(Object savings_account_id, 
		@JsonProperty("ams_transaction_id") Object amsTransactionId, 
		@JsonProperty("internal_correlation_id") String internalCorrelationId, 
		@JsonProperty("structured_transaction_details") String structuredTransactionDetails, 
		LocalDateTime created_at, 
		LocalDateTime updated_at) {
}
