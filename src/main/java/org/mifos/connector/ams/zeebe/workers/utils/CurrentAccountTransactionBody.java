package org.mifos.connector.ams.zeebe.workers.utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.util.ObjectUtils;

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

        public Entry(String account_iban, String structured_transaction_details, String internal_correlation_id, String partner_name, String partner_account_iban, String transaction_group_id, String transaction_id, String end_to_end_id, String category_purpose_code, String payment_scheme, String remittance_information_unstructured, String source_ams_account_id, String target_ams_account_id, String transactionCreationChannel, String partner_secondary_id_mobile, String partner_secondary_id_email, String partner_secondary_id_tax_id, String partner_secondary_id_tax_number, String partner_account_internal_account_id, boolean value_dated, String direction) {
            this.account_iban = account_iban;
            this.structured_transaction_details = structured_transaction_details;
            this.internal_correlation_id = internal_correlation_id;
            this.partner_name = partner_name;
            this.partner_account_iban = partner_account_iban;
            this.transaction_group_id = transaction_group_id;
            this.transaction_id = transaction_id;
            this.end_to_end_id = end_to_end_id;
            this.category_purpose_code = category_purpose_code;
            this.payment_scheme = payment_scheme;
            this.remittance_information_unstructured = remittance_information_unstructured;
            this.source_ams_account_id = source_ams_account_id;
            this.target_ams_account_id = target_ams_account_id;
            this.transactionCreationChannel = transactionCreationChannel;
            this.partner_secondary_id_mobile = partner_secondary_id_mobile;
            this.partner_secondary_id_email = partner_secondary_id_email;
            this.partner_secondary_id_tax_id = partner_secondary_id_tax_id;
            this.partner_secondary_id_tax_number = partner_secondary_id_tax_number;
            this.partner_account_internal_account_id = partner_account_internal_account_id;
            this.value_dated = value_dated;
            this.direction = direction;

            if (ObjectUtils.isEmpty(partner_secondary_id_mobile) &&
                    ObjectUtils.isEmpty(partner_secondary_id_email) &&
                    ObjectUtils.isEmpty(partner_secondary_id_tax_id) &&
                    ObjectUtils.isEmpty(partner_secondary_id_tax_number)) {
                return;
            }

            // any of the secondary identifiers are present, so creditor IBAN and creditor name are removed from data table and camt.053
            this.partner_name = "";
            this.partner_account_iban = "";
        }
    }
}