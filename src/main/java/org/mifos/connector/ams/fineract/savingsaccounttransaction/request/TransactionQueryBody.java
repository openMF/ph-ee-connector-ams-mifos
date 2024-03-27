package org.mifos.connector.ams.fineract.savingsaccounttransaction.request;

import lombok.Builder;
import lombok.experimental.Accessors;

@Builder
@Accessors(fluent = true)
public record TransactionQueryBody(TransactionQueryRequest request, String dateFormat, String locale, Integer page, Integer size, String[] sorts) {

}
