package org.mifos.connector.ams.zeebe.workers.utils;

public record TransactionBody(String transactionDate, Object transactionAmount, Integer paymentTypeId, String note, String dateFormat, String locale) {
	
	@Override
	public String toString() {
		return "{\"transactionDate\":\"" + transactionDate + "\",\"transactionAmount\":" + transactionAmount + ",\"paymentTypeId\":" + paymentTypeId + ",\"dateFormat\":\"" + dateFormat + "\",\"locale\":\"" + locale + "\"}";
	}
}
