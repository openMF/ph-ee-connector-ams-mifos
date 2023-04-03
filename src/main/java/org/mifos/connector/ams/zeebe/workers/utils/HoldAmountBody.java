package org.mifos.connector.ams.zeebe.workers.utils;

public record HoldAmountBody(String transactionDate, Object transactionAmount, String reasonForBlock, String locale, String dateFormat) {
}
