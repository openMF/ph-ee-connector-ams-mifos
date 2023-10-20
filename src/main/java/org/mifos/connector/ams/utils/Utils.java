package org.mifos.connector.ams.utils;

import static org.mifos.connector.ams.zeebe.ZeebeVariables.ERROR_CODE;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.ERROR_INFORMATION;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.ERROR_PAYLOAD;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.HashMap;
import java.util.Map;
import org.apache.camel.Exchange;
import org.mifos.connector.ams.errorhandler.ErrorTranslator;

public final class Utils {

    private Utils() {}

    // use this utility method only if exchange has ERROR_CODE, ERROR_INFORMATION & ERROR_PAYLOAD variables
    public static Map<String, Object> getDefaultZeebeErrorVariable(Exchange exchange, ErrorTranslator translator)
            throws JsonProcessingException {
        Map<String, Object> variables = new HashMap<>();
        variables.put(ERROR_CODE, exchange.getProperty(ERROR_CODE));
        variables.put(ERROR_INFORMATION, exchange.getProperty(ERROR_INFORMATION));
        variables.put(ERROR_PAYLOAD, exchange.getProperty(ERROR_PAYLOAD));

        return translator.translateError(variables);
    }

}
