package org.mifos.connector.ams.fineract.savingsaccounttransaction.response;

import java.util.List;

public record TransactionQuerySort(List<TransactionQueryOrder> orders) {

}
