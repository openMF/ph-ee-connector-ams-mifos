package org.mifos.connector.ams.camel.cxfrs;

import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.ProducerTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import static org.apache.camel.Exchange.HTTP_METHOD;
import static org.apache.camel.Exchange.HTTP_PATH;
import static org.mifos.connector.ams.camel.cxfrs.HeaderBasedInterceptor.CXF_TRACE_HEADER;
@Component
public class CxfrsUtil {

    @Autowired
    private ProducerTemplate template;

    @Value("${wiremock.local.host}")
    private String callbackUrl;
    /**
     * Warning! Clears IN headers.
     */
    public void sendInOut(String endpoint, Exchange ex, Map<String, Object> headers, Object body) {
        ExchangePattern oldPattern = ex.getPattern();
        if(body != null) {
            ex.getIn().setBody(body);
        }
        ex.getIn().removeHeaders("*");
        ex.getIn().setHeaders(headers);
        ex.setPattern(ExchangePattern.InOut);
        Exchange response = template.send(endpoint, ex);
        if(response.getOut().getHeader("CamelHttpResponseCode").equals(200)){
            callback(ex);
        }
        ex.setPattern(oldPattern);
    }
    void callback(Exchange ex){
        Map<String, Object> headers = new HashMap<>();
        headers.put(CXF_TRACE_HEADER, true);
        headers.put(HTTP_METHOD, "GET");
        headers.put(HTTP_PATH, callbackUrl);
        ex.getIn().setHeaders(headers);
        ex.setPattern(ExchangePattern.InOut);
        template.send("cxfrs:bean:wiremock.local.host",ex);
    }

}
