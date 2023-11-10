package org.mifos.connector.ams.fineract.savingsaccounttransaction.request;

import lombok.Builder;
import lombok.experimental.Accessors;

@Builder(builderMethodName = "builder")
@Accessors(fluent = true)
public record TransactionQueryFilter(String operator, String[] values) {
}
