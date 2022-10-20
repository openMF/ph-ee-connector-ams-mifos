package org.mifos.connector.ams.zeebe.workers.bookamount;

public record DepositBody(String transactionDate, Object amount, Integer paymentTypeId, String note, String dateFormat, String locale) {
}
