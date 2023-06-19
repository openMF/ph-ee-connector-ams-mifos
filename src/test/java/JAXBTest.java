import org.junit.jupiter.api.Test;

import jakarta.xml.bind.JAXBContext;

public class JAXBTest {

    @Test
    public void test() throws Exception {
        JAXBContext.newInstance(eu.nets.realtime247.ri_2015_10.ObjectFactory.class,
                iso.std.iso._20022.tech.xsd.pacs_008_001.ObjectFactory.class,
                iso.std.iso._20022.tech.xsd.pacs_002_001.ObjectFactory.class,
                iso.std.iso._20022.tech.xsd.camt_056_001.ObjectFactory.class);
    }
}
