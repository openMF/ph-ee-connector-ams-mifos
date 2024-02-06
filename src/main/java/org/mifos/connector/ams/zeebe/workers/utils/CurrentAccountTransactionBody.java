package org.mifos.connector.ams.zeebe.workers.utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CurrentAccountTransactionBody {
    private BigDecimal transactionAmount;
    private String dateFormat;
    private String locale;
    private Integer paymentTypeId;
    private String currencyCode;
    private List<DataTable> datatables;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DataTable {
        private List<Entry> entries;
        private String name;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Entry {
        private String account_iban;
        private String structured_transaction_details;
        private String internal_correlation_id;
        private String partner_name;
        private String partner_account_iban;
    }
}

