package org.mifos.connector.ams.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import iso.std.iso._20022.tech.json.camt_053_001.EntryTransaction10;
import iso.std.iso._20022.tech.json.camt_053_001.ReportEntry10;
import iso.std.iso._20022.tech.json.camt_053_001.SupplementaryData1;
import iso.std.iso._20022.tech.json.camt_053_001.TransactionParties6;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

import java.util.Map;

@Component
public class SerializationHelper {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    @Qualifier("painMapper")
    public ObjectMapper painMapper;


    public String writeCamt053AsString(String accountProductType, ReportEntry10 camt053) {
        removeNameAndIbanWhenSecondaryIdsAreUsed(camt053);

        try {
            if ("CURRENT".equalsIgnoreCase(accountProductType)) {
                logger.trace("serializeCamt053 for Current account");
                EntryTransaction10 transactionDetail = camt053.getEntryDetails().get(0).getTransactionDetails().get(0);
                return painMapper.writeValueAsString(removeFieldsFromCurrentAccount(transactionDetail));

            } else {
                logger.trace("serializeCamt053 for Savings account");
                return painMapper.writeValueAsString(camt053);
            }

        } catch (Exception e) {
            throw new RuntimeException("failed to serialize camt053", e);
        }
    }

    private void removeNameAndIbanWhenSecondaryIdsAreUsed(ReportEntry10 camt053) {
        try {
            TransactionParties6 relatedParty = camt053.getEntryDetails().get(0).getTransactionDetails().get(0).getRelatedParties();
            Map<String, Object> creditorProperties = relatedParty.getCreditor().getAdditionalProperties();
            if (creditorProperties != null) {
                Map party = (Map) creditorProperties.get("Party");
                if (party != null) {
                    Map contactDetails = (Map) party.get("ContactDetails");
                    if (contactDetails != null) {
                        Object mobileNumber = contactDetails.get("MobileNumber");
                        Object emailAddress = contactDetails.get("EmailAddress");
                        Object other = contactDetails.get("Other");
                        if (!ObjectUtils.isEmpty(mobileNumber) || !ObjectUtils.isEmpty(emailAddress) || !ObjectUtils.isEmpty(other)) {
                            logger.debug("removing creditor Name/IBAN/CreditorIdentification from camt053");
                            try {
                                relatedParty.getCreditorAccount().getIdentification().setAdditionalProperty("IBAN", "");
                            } catch (Exception e) {
                                logger.warn("failed to remove IBAN from camt053", e);
                            }
                            try {
                                ((Map) relatedParty.getCreditor().getAdditionalProperties().get("Party")).put("Name", "");
                            } catch (Exception e) {
                                logger.warn("failed to remove CreditorParty Name from camt053", e);
                            }
                            try {
                                for (SupplementaryData1 supplementaryData : camt053.getEntryDetails().get(0).getTransactionDetails().get(0).getSupplementaryData()) {
                                    Map otherIdentification = (Map) supplementaryData.getEnvelope().getAdditionalProperties().get("OtherIdentification");
                                    if (otherIdentification != null) {
                                        Map creditorAccount = (Map) otherIdentification.get("CreditorAccount");
                                        Map identification = (Map) creditorAccount.get("Identification");
                                        identification.clear();
                                    }
                                }
                            } catch (Exception e) {
                                logger.warn("failed to remove CreditorAccount Identification from camt053", e);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("failed to remove name/iban from camt053 if secondary ids were used", e);
        }
    }

    EntryTransaction10 removeFieldsFromCurrentAccount(EntryTransaction10 transactionDetail) {
        transactionDetail.setAdditionalTransactionInformation(null);
        transactionDetail.getSupplementaryData().forEach(data -> {
            try {
                Object omSupplementaryData = data.getEnvelope().getAdditionalProperties().get("OrderManagerSupplementaryData");
                if (omSupplementaryData == null) return;
                ((Map) omSupplementaryData).remove("transactionCreationChannel");
            } catch (Exception e) {
                logger.warn("failed to remove supplementaryData from transactionDetail: " + transactionDetail.getSupplementaryData(), e);
            }
        });
        return transactionDetail;
    }
}