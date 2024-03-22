package org.mifos.connector.ams.zeebe.workers.utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.List;

@Data
@AllArgsConstructor
public class CurrentAccountTransactionBody {
    private BigDecimal transactionAmount;
    private String dateFormat;
    private String locale;
    private String paymentTypeId;
    private String currencyCode;
    private List<DataTable> datatables;

    @Data
    @AllArgsConstructor
    public static class DataTable {
        private List<Entry> entries;
        private String name;
    }

    @Data
    @AllArgsConstructor
    public static class Entry {
        private String account_iban;
        private String structured_transaction_details;
        private String internal_correlation_id;
        private String partner_name;
        private String partner_account_iban;
        private String transaction_group_id;
        private String transaction_id;
        private String end_to_end_id;
        private String category_purpose_code;
        private String payment_scheme;
        private String remittance_information_unstructured;
        private String source_ams_account_id;
        private String target_ams_account_id;
        private String transactionCreationChannel;
        private String partner_secondary_id_mobile;
        private String partner_secondary_id_email;
        private String partner_secondary_id_tax_id;
        private String partner_secondary_id_tax_number;
        private String partner_account_internal_account_id; // onus / ig2
        private boolean value_dated; // onus / ig2
        private String direction;
    }
}
