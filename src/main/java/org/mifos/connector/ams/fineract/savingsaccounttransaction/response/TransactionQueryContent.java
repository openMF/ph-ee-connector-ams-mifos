package org.mifos.connector.ams.fineract.savingsaccounttransaction.response;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;

public record TransactionQueryContent(@JsonProperty("running_balance_derived") BigDecimal runningBalanceDerived) {

}
