package org.mifos.connector.ams.fineract.currentaccount.response;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class CAGetResponse {
    private String id;
    private String accountNumber;
    private String externalId;
    private int clientId;
    private Product product;
    private Status status;
    private String activatedOnDate;
    private Currency currency;
    private boolean allowOverdraft;
    private boolean allowForceTransaction;
    private BigDecimal minimumRequiredBalance;
    private BalanceCalculationType balanceCalculationType;
    private BigDecimal accountBalance;
    private BigDecimal holdAmount;


    @Data
    public class Product {
        private String id;
        private String name;
        private String shortName;
        private String description;
    }

    @Data
    public class Status {
        private String id;
        private String code;
        private String value;
    }

    @Data
    public class Currency {
        private String code;
        private String name;
        private int decimalPlaces;
        private String displayLabel;
    }

    @Data
    public class BalanceCalculationType {
        private String id;
        private String code;
        private String value;
    }
}