package org.mifos.connector.ams.camel.cxfrs;

import java.util.Map;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.ProducerTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CxfrsUtil {

    @Autowired
    private ProducerTemplate template;

    /**
     * Warning! Clears IN headers.
     */
    public void sendInOut(String endpoint, Exchange ex, Map<String, Object> headers, Object body) {
        ExchangePattern oldPattern = ex.getPattern();
        if (body != null) {
            ex.getIn().setBody(body);
        }
        ex.getIn().removeHeaders("*");
        ex.getIn().setHeaders(headers);
        ex.setPattern(ExchangePattern.InOut);
        template.send(endpoint, ex);
        ex.setPattern(oldPattern);
    }
}
