package org.mifos.connector.ams.rest.authorize;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class AuthorizeRequest {
    BigDecimal originalAmount;
    BigDecimal transactionAmount;
    Boolean isContactless;
    Boolean isEcommerce;
    String accId;
    String cardToken;
    String dateTimeFormat;
    String instructedAmount;
    String instructedCurrency;
    String merchantCategoryCode;
    String messageId;
    String partnerCountry;
    String processCode;
    String requestId;
    String sequenceDateTime;
    String tenantId;
}
