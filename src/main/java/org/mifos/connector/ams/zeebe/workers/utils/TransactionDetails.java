package org.mifos.connector.ams.zeebe.workers.utils;

import com.fasterxml.jackson.annotation.JsonProperty;

public record TransactionDetails(@JsonProperty("internal_correlation_id") String internalCorrelationId, 
		@JsonProperty("structured_transaction_details") String structuredTransactionDetails,
		@JsonProperty("account_iban") String accountIban,
		@JsonProperty("transaction_date") String transactionDate,
		String dateFormat,
		String locale,
		@JsonProperty("transaction_group_id") String transactionGroupId,
		@JsonProperty("category_purpose_code") String categoryPurposeCode) {
}
