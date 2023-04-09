package org.mifos.connector.ams.zeebe.workers.utils;

public record TransactionDetails(String ams_transaction_id, String internal_correlation_id, String structured_transaction_details) {
}
