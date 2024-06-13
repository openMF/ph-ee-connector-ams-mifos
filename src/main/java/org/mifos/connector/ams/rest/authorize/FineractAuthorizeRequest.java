package org.mifos.connector.ams.rest.authorize;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FineractAuthorizeRequest {
    BigDecimal transactionAmount;
    BigDecimal originalAmount;
    String sequenceDateTime;
    final String accId = "123"; // TODO
    final String locale = "en";
    final String dateFormat = "dd MMMM yyyy";
}
