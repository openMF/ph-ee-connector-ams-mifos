package org.mifos.connector.ams.zeebe.workers.utils;

import com.fasterxml.jackson.annotation.JsonProperty;

public record DtSavingsTransactionDetails(@JsonProperty("internal_correlation_id") String internalCorrelationId,
                                          @JsonProperty("structured_transaction_details") String structuredTransactionDetails,
                                          @JsonProperty("account_iban") String accountIban,
                                          @JsonProperty("payment_type_code") String paymentTypeCode,
                                          @JsonProperty("transaction_group_id") String transactionGroupId,
                                          @JsonProperty("partner_name") String partnerName,
                                          @JsonProperty("partner_account_iban") String partnerAccountIban,
                                          @JsonProperty("partner_account_internal_account_id") String partnerAccountInternalAccountId,
                                          @JsonProperty("partner_secondary_identifier") String partnerSecondaryIdentifier,
                                          @JsonProperty("remittance_information_unstructured") String remittanceInformationUnstructured,
                                          @JsonProperty("category_purpose_code") String categoryPurposeCode,
                                          @JsonProperty("payment_scheme") String paymentScheme,
                                          @JsonProperty("source_ams_account_id") String sourceAmsAccountId,
                                          @JsonProperty("target_ams_account_id") String targetAmsAccountId,
                                          @JsonProperty("end_to_end_id") String endToEndId) {
}
