package org.mifos.connector.ams.zeebe;

import org.apache.camel.Exchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class ZeebeUtil {

    private static Logger logger = LoggerFactory.getLogger(ZeebeUtil.class);

    public static void zeebeVariablesToCamelProperties(Map<String, Object> variables, Exchange exchange, String... names) {
        for (String name : names) {
            Object value = variables.get(name);
            if (value == null) {
                logger.error("failed to find Zeebe variable name {}", name);
            } else {
                exchange.setProperty(name, value);
            }
        }
    }
}
