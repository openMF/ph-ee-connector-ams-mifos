package org.mifos.connector.ams.fineract.savingsaccounttransaction.response;

public record TransactionQueryOrder(String direction, String property, Boolean ignoreCase, String nullHandling) {

}
