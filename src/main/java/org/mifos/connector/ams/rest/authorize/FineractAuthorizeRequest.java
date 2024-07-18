package org.mifos.connector.ams.rest.authorize;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class FineractAuthorizeRequest {
    BigDecimal transactionAmount;
    BigDecimal originalAmount;
    String sequenceDateTime;
    final String locale = "en";
    String dateTimeFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
}
