package org.mifos.connector.ams.zeebe.workers.bookamount;

public record HoldAmountBody(String transactionDate, Object transactionAmount, String reasonForBlock, String locale, String dateFormat) {
}
