package org.mifos.connector.ams.zeebe.workers.utils;

import com.fasterxml.jackson.annotation.JsonProperty;

public record DtSavingsTransactionDetails(String internalCorrelationId, 
		String structuredTransactionDetails,
		String accountIban,
		String paymentTypeCode,
		String transactionGroupId,
		String partnerName,
		String partnerAccountIban,
		String partnerAccountInternalAccountId,
		@JsonProperty("partner_secondary_identifier") String partnerSecondaryIdentifier,
		@JsonProperty("remittance_information_unstructured") String remittanceInformationUnstructured,
		@JsonProperty("category_purpose_code") String categoryPurposeCode) {
	
	@Override
	public String toString() {
		return "{\"internal_correlation_id\":\"" + internalCorrelationId + "\","
				+ "\"structured_transaction_details\":\"" + structuredTransactionDetails + "\","
				+ "\"account_iban\":\"" + accountIban + "\","
				+ "\"payment_type_code\":\"" + paymentTypeCode + "\","
				+ "\"transaction_group_id\":\"" + transactionGroupId + "\","
				+ "\"partner_name\":\"" + partnerName + "\","
				+ "\"partner_account_iban\":\"" + partnerAccountIban + "\","
				+ "\"partner_account_internal_account_id\":\"" + partnerAccountInternalAccountId + "\","
				+ "\"partner_secondary_identifier\":\"" + partnerSecondaryIdentifier + "\","
				+ "\"remittance_information_unstructured\":\"" + remittanceInformationUnstructured + "\","
				+ "\"category_purpose_code\":\"" + categoryPurposeCode + "\"}";
	}
}