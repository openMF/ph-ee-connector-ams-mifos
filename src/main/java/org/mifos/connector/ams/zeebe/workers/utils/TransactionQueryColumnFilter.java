package org.mifos.connector.ams.zeebe.workers.utils;

import lombok.Builder;
import lombok.experimental.Accessors;

@Builder(builderMethodName = "builder")
@Accessors(fluent = true)
public record TransactionQueryColumnFilter(String column, TransactionQueryFilter[] filters) {

}
