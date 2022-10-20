package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;

public record DepositBody(String transactionDate, BigDecimal amount, Integer paymentTypeId, String note, String dateFormat, String locale) {
}
