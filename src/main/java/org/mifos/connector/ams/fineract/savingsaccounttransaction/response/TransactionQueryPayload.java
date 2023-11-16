package org.mifos.connector.ams.fineract.savingsaccounttransaction.response;

import java.util.List;

public record TransactionQueryPayload(Integer total, List<TransactionQueryContent> content, TransactionQueryPageable pageable) {

}
