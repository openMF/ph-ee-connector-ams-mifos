package org.mifos.connector.ams.zeebe.workers.utils;

import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBElement;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;
import java.io.StringReader;

@Component
public class JAXBUtils {

    private Unmarshaller unmarshaller;

    @PostConstruct
    public void init() throws JAXBException {
        JAXBContext jaxbContext = JAXBContext.newInstance(eu.nets.realtime247.ri_2015_10.ObjectFactory.class,
                iso.std.iso._20022.tech.xsd.pacs_008_001.ObjectFactory.class,
                iso.std.iso._20022.tech.xsd.pacs_002_001.ObjectFactory.class,
                iso.std.iso._20022.tech.xsd.camt_056_001.ObjectFactory.class);
        unmarshaller = jaxbContext.createUnmarshaller();
    }

    @SuppressWarnings("unchecked")
    public iso.std.iso._20022.tech.xsd.camt_056_001.Document unmarshalCamt056(String camt056) throws JAXBException {
        return ((JAXBElement<iso.std.iso._20022.tech.xsd.camt_056_001.Document>) unmarshaller.unmarshal(new StringReader(camt056))).getValue();
    }

    @SuppressWarnings("unchecked")
    public iso.std.iso._20022.tech.xsd.pacs_008_001.Document unmarshalPacs008(String pacs008) throws JAXBException {
        return ((JAXBElement<iso.std.iso._20022.tech.xsd.pacs_008_001.Document>) unmarshaller.unmarshal(new StringReader(pacs008))).getValue();
    }
    
    @SuppressWarnings("unchecked")
    public iso.std.iso._20022.tech.xsd.pacs_004_001.Document unmarshalPacs004(String pacs004) throws JAXBException {
        return ((JAXBElement<iso.std.iso._20022.tech.xsd.pacs_004_001.Document>) unmarshaller.unmarshal(new StringReader(pacs004))).getValue();
    }
}
