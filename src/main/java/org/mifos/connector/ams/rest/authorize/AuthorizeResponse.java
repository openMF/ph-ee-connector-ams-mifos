package org.mifos.connector.ams.rest.authorize;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AuthorizeResponse {
    BigDecimal accountBalance;
    BigDecimal holdAmount;
    BigDecimal availableBalance;
    BigDecimal externalHold;

    public AuthorizeResponse(FineractAuthorizeResponse fineractResponse) {
        FineractAuthorizeResponse.Changes changes = fineractResponse.getChanges();
        if (changes == null) {
            throw new IllegalStateException("missing `changes` structure in fineract response");
        }
        this.accountBalance = changes.getAccountBalance();
        this.holdAmount = changes.getHoldAmount();
        this.availableBalance = changes.getAvailableBalance();
        this.externalHold = changes.getExternalHold();
    }
}

