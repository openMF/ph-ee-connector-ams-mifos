package org.mifos.connector.ams.zeebe.workers.utils;

public record JournalEntry(String officeId, 
		String currencyCode, 
		AccountIdAmountPair[] debits, 
		AccountIdAmountPair[] credits, 
		String referenceNumber,
		String transactionDate,
		String paymentTypeId,
		String accountNumber,
		String checkNumber,
		String routingCode,
		String receiptNumber,
		String bankNumber,
		String comments,
		String locale,
		String dateFormat) {
}
