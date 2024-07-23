package org.mifos.connector.ams.zeebe.workers.utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.math.BigDecimal;
import java.util.List;

@Data
@Accessors(chain = true)
@NoArgsConstructor
@AllArgsConstructor
public class ExternalHoldItem implements BatchItem {
    BigDecimal originalAmount;
    BigDecimal transactionAmount;
    Integer requestId;
    List<Header> headers;
    String body;
    String dateTimeFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    String locale = "en";
    String method;
    String relativeUrl;
    String sequenceDateTime;
}
