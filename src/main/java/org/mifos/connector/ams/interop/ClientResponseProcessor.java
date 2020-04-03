package org.mifos.connector.ams.interop;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.mifos.connector.ams.properties.TenantProperties;
import org.mifos.phee.common.ams.dto.ClientData;
import org.mifos.phee.common.ams.dto.Customer;
import org.mifos.phee.common.ams.dto.LegalForm;
import org.mifos.phee.common.mojaloop.dto.ComplexName;
import org.mifos.phee.common.mojaloop.dto.Party;
import org.mifos.phee.common.mojaloop.dto.PartyIdInfo;
import org.mifos.phee.common.mojaloop.dto.PersonalInfo;
import org.mifos.phee.common.mojaloop.type.IdentifierType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static org.mifos.connector.ams.camel.config.CamelProperties.PARTY_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.PARTY_ID_TYPE;
import static org.mifos.phee.common.ams.dto.LegalForm.PERSON;

@Component
public class ClientResponseProcessor implements Processor {

    @Value("${ams.local.version}")
    private String amsVersion;

    @Autowired
    private TenantProperties tenantProperties;

    @Override
    public void process(Exchange exchange) {
        String partyIdType = exchange.getProperty(PARTY_ID_TYPE, String.class);
        String partyId = exchange.getProperty(PARTY_ID, String.class);

        Party party = new Party(
                new PartyIdInfo(IdentifierType.valueOf(partyIdType),
                        partyId,
                        null,
                        tenantProperties.getTenant(partyIdType, partyId).getFspId()),
                null,
                null,
                null);

        if ("1.2".equals(amsVersion)) {
            ClientData client = exchange.getIn().getBody(ClientData.class);
            if (PERSON.equals(LegalForm.fromValue(client.getId().intValue()))) {
                PersonalInfo pi = new PersonalInfo();
                pi.setDateOfBirth(client.getDateOfBirth() != null ? client.getDateOfBirth().toString() : null);
                ComplexName cn = new ComplexName();
                cn.setFirstName(client.getFirstname());
                cn.setLastName(client.getLastname());
                cn.setMiddleName(client.getMiddlename());
                pi.setComplexName(cn);
                party.setPersonalInfo(pi);
            } else { // entity
                party.setName(client.getFullname());
            }
        } else { // cn
            Customer client = exchange.getIn().getBody(Customer.class);
            if (PERSON.equals(client.getType())) {
                PersonalInfo pi = new PersonalInfo();
                pi.setDateOfBirth(client.getDateOfBirth() != null ? client.getDateOfBirth().toString() : null);
                ComplexName cn = new ComplexName();
                cn.setFirstName(client.getGivenName());
                cn.setLastName(client.getSurname());
                cn.setMiddleName(client.getMiddleName());
                pi.setComplexName(cn);
                party.setPersonalInfo(pi);
            } else {
                party.setName(client.getGivenName());
            }
        }

        exchange.getIn().setBody(party);
    }
}