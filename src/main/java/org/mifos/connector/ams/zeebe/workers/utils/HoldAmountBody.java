package org.mifos.connector.ams.zeebe.workers.utils;

public record HoldAmountBody(String transactionDate, Object transactionAmount, Integer reasonForBlock, String locale, String dateFormat) {
}
