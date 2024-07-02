package org.mifos.connector.ams.rest.authorize;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class FineractAuthorizeResponse {
    String clientId;
    String resourceIdentifier;
    String entityExternalId;

    Changes changes;

    @Data
    public static class Changes {
        BigDecimal accountBalance;
        BigDecimal holdAmount;
        BigDecimal availableBalance;
        BigDecimal externalHoldAmount;
        boolean balanceChanged;
    }
}
