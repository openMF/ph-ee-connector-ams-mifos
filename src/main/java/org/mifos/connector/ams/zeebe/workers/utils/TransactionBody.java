package org.mifos.connector.ams.zeebe.workers.utils;

public record TransactionBody(String transactionDate, Object transactionAmount, String paymentTypeId, String note, String dateFormat, String locale) {
}
