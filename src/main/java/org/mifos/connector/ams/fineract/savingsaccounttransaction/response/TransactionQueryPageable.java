package org.mifos.connector.ams.fineract.savingsaccounttransaction.response;

public record TransactionQueryPageable(TransactionQuerySort sort, Integer page, Integer size) {

}
