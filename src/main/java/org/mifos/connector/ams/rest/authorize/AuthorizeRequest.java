package org.mifos.connector.ams.rest.authorize;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class AuthorizeRequest {
    BigDecimal transactionAmount;
    BigDecimal originalAmount;
    String sequenceDateTime;
}
