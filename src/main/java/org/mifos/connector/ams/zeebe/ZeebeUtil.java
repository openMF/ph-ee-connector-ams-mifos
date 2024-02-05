package org.mifos.connector.ams.zeebe;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class ZeebeUtil {

    private static final Logger logger = LoggerFactory.getLogger(ZeebeUtil.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void zeebeVariablesToCamelProperties(Map<String, Object> variables, Exchange exchange, String... names) {
        exchange.setProperty("zeebeVariables", variables);

        for (String name : names) {
            Object value = variables.get(name);
            if (value == null) {
                logger.error("failed to find Zeebe variable name {}", name);
            } else {
                exchange.setProperty(name, value);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> zeebeVariablesFrom(Exchange exchange) {
        return exchange.getProperty("zeebeVariables", Map.class);
    }

    @SuppressWarnings("unchecked")
    public static <T> T zeebeVariable(Exchange exchange, String name, Class<T> clazz) throws Exception {
        Object content = zeebeVariablesFrom(exchange).get(name);
        if (content instanceof Map) {
            return objectMapper.readValue(objectMapper.writeValueAsString(content), clazz);
        }
        return (T) content;
    }
}
