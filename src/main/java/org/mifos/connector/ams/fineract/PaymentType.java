package org.mifos.connector.ams.fineract;

import com.fasterxml.jackson.annotation.JsonProperty;

public record PaymentType(
        @JsonProperty("Operation") String operation,
        @JsonProperty("PaymentTypeCode") String paymentTypeCode,
        @JsonProperty("FineractId") Integer fineractId) {
}
