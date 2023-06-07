package org.mifos.connector.ams.zeebe.workers.utils;

import java.io.Reader;
import java.io.StringReader;

import javax.annotation.PostConstruct;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.springframework.stereotype.Component;

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
}
