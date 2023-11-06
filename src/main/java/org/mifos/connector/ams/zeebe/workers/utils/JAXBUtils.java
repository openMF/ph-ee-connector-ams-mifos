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

    private JAXBContext jaxbContext;

    @PostConstruct
    public void init() throws JAXBException {
        jaxbContext = JAXBContext.newInstance(eu.nets.realtime247.ri_2015_10.ObjectFactory.class,
                iso.std.iso._20022.tech.xsd.pacs_008_001.ObjectFactory.class,
                iso.std.iso._20022.tech.xsd.pacs_002_001.ObjectFactory.class,
                iso.std.iso._20022.tech.xsd.camt_056_001.ObjectFactory.class,
        		iso.std.iso._20022.tech.xsd.pacs_004_001.ObjectFactory.class,
        		iso.std.iso._20022.tech.xsd.pacs_002_001.ObjectFactory.class);
    }

    public iso.std.iso._20022.tech.xsd.camt_056_001.Document unmarshalCamt056(String camt056) throws JAXBException {
    	return unmarshal(camt056, iso.std.iso._20022.tech.xsd.camt_056_001.Document.class);
    }

    @SuppressWarnings("unchecked")
    public iso.std.iso._20022.tech.xsd.pacs_008_001.Document unmarshalPacs008(String pacs008) throws JAXBException {
    	Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        return ((JAXBElement<iso.std.iso._20022.tech.xsd.pacs_008_001.Document>) unmarshaller.unmarshal(new StringReader(pacs008))).getValue();
    }
    
    @SuppressWarnings("unchecked")
    public iso.std.iso._20022.tech.xsd.pacs_004_001.Document unmarshalPacs004(String pacs004) throws JAXBException {
    	Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        return ((JAXBElement<iso.std.iso._20022.tech.xsd.pacs_004_001.Document>) unmarshaller.unmarshal(new StringReader(pacs004))).getValue();
    }
    
    @SuppressWarnings("unchecked")
    public iso.std.iso._20022.tech.xsd.pacs_002_001.Document unmarshalPacs002(String pacs002) throws JAXBException {
    	Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
    	return ((JAXBElement<iso.std.iso._20022.tech.xsd.pacs_002_001.Document>) unmarshaller.unmarshal(new StringReader(pacs002))).getValue();
    }
    
    @SuppressWarnings("unchecked")
    private <T> T unmarshal(String original, Class<T> clz) throws JAXBException {
    	Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
    	return ((JAXBElement<T>) unmarshaller.unmarshal(new StringReader(original))).getValue();
    }
}
