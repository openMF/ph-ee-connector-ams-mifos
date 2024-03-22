package org.mifos.connector.ams.zeebe.workers.utils;

import iso.std.iso._20022.tech.json.pain_001_001.Contact4;
import iso.std.iso._20022.tech.json.pain_001_001.OtherContact1;
import iso.std.iso._20022.tech.xsd.pacs_008_001.ContactDetails2;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ContactDetailsUtil {

    public String getId(iso.std.iso._20022.tech.xsd.pacs_008_001.ContactDetails2 contactDetails) {
        if (contactDetails == null) {
            return null;
        }

        if (contactDetails.getEmailAdr() != null) {
            return contactDetails.getEmailAdr();
        }

        if (contactDetails.getMobNb() != null) {
            return contactDetails.getMobNb();
        }

        if (contactDetails.getOthr() != null) {
            return contactDetails.getOthr();
        }

        return null;
    }

    public String getId(iso.std.iso._20022.tech.xsd.pacs_004_001.ContactDetails2 contactDetails) {
        if (contactDetails == null) {
            return null;
        }

        if (contactDetails.getEmailAdr() != null) {
            return contactDetails.getEmailAdr();
        }

        if (contactDetails.getMobNb() != null) {
            return contactDetails.getMobNb();
        }

        if (contactDetails.getOthr() != null) {
            return contactDetails.getOthr();
        }

        return null;
    }

    public String getId(iso.std.iso._20022.tech.json.pain_001_001.Contact4 contactDetails) {
        if (contactDetails == null) {
            return null;
        }

        if (contactDetails.getEmailAddress() != null) {
            return contactDetails.getEmailAddress();
        }

        if (contactDetails.getMobileNumber() != null) {
            return contactDetails.getMobileNumber();
        }

        List<OtherContact1> otherList = contactDetails.getOther();
        if (otherList != null && !otherList.isEmpty()) {
            return otherList.get(0).getIdentification();
        }

        return null;
    }

    public String getId(iso.std.iso._20022.tech.xsd.camt_056_001.ContactDetails2 contactDetails) {
        if (contactDetails == null) {
            return null;
        }

        if (contactDetails.getEmailAdr() != null) {
            return contactDetails.getEmailAdr();
        }

        if (contactDetails.getMobNb() != null) {
            return contactDetails.getMobNb();
        }

        if (contactDetails.getOthr() != null) {
            return contactDetails.getOthr();
        }

        return null;
    }

    public String getTaxId(ContactDetails2 contactDetails) {
        return (contactDetails != null &&
                contactDetails.getOthr() != null &&
                contactDetails.getOthr().startsWith("TXID")
        ) ? contactDetails.getOthr() : null;

    }

    public String getTaxId(Contact4 contactDetails) {
        return (contactDetails != null &&
                contactDetails.getOther() != null &&
                !contactDetails.getOther().isEmpty() &&
                contactDetails.getOther().get(0).getIdentification() != null &&
                contactDetails.getOther().get(0).getIdentification().startsWith("TXID")
        ) ? contactDetails.getOther().get(0).getIdentification() : null;
    }

    public String getTaxId(iso.std.iso._20022.tech.xsd.pacs_004_001.ContactDetails2 contactDetails) {
        return (contactDetails != null &&
                contactDetails.getEmailAdr() != null &&
                contactDetails.getEmailAdr().startsWith("TXID")
        ) ? contactDetails.getEmailAdr() : null;
    }

    public String getTaxId(iso.std.iso._20022.tech.xsd.camt_056_001.ContactDetails2 contactDetails) {
        return (contactDetails != null &&
                contactDetails.getEmailAdr() != null &&
                contactDetails.getEmailAdr().startsWith("TXID")
        ) ? contactDetails.getEmailAdr() : null;
    }

    public String getTaxNumber(ContactDetails2 contactDetails) {
        return (contactDetails != null &&
                contactDetails.getOthr() != null &&
                contactDetails.getOthr().startsWith("TXNB")
        ) ? contactDetails.getOthr() : null;
    }

    public String getTaxNumber(Contact4 contactDetails) {
        return (contactDetails != null &&
                contactDetails.getOther() != null &&
                !contactDetails.getOther().isEmpty() &&
                contactDetails.getOther().get(0).getIdentification() != null &&
                contactDetails.getOther().get(0).getIdentification().startsWith("TXNB")
        ) ? contactDetails.getOther().get(0).getIdentification() : null;
    }

    public String getTaxNumber(iso.std.iso._20022.tech.xsd.pacs_004_001.ContactDetails2 contactDetails) {
        return (contactDetails != null &&
                contactDetails.getEmailAdr() != null &&
                contactDetails.getEmailAdr().startsWith("TXNB")
        ) ? contactDetails.getEmailAdr() : null;

    }

    public String getTaxNumber(iso.std.iso._20022.tech.xsd.camt_056_001.ContactDetails2 contactDetails) {
        return (contactDetails != null &&
                contactDetails.getEmailAdr() != null &&
                contactDetails.getEmailAdr().startsWith("TXNB")
        ) ? contactDetails.getEmailAdr() : null;
    }
}
