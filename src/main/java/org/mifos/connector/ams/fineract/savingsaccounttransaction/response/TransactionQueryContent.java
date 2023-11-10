package org.mifos.connector.ams.fineract.savingsaccounttransaction.response;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonProperty;

public record TransactionQueryContent(@JsonProperty("running_balance_derived") BigDecimal runningBalanceDerived) {

}
