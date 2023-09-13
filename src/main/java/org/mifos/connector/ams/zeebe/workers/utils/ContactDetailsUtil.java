package org.mifos.connector.ams.zeebe.workers.utils;

import org.springframework.stereotype.Component;

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
		
		if (contactDetails.getOther() != null) {
			return contactDetails.getOther().toString();
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
}
