package org.mifos.connector.ams.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class BalanceResponseDTO {
        private String accountStatus;
        private String reservedBalance;
        private String currentBalance;
        private String currency;
        private String availableBalance;
        private String unclearedBalance;
}
