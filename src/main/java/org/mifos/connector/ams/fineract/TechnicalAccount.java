package org.mifos.connector.ams.fineract;

import com.fasterxml.jackson.annotation.JsonProperty;

public record TechnicalAccount(
        @JsonProperty("Operation") String operation,
        @JsonProperty("AccountId") Integer accountId) {
}
