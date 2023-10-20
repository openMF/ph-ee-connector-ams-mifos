package org.mifos.connector.ams.camel.cxfrs;

import java.io.InputStream;
import org.apache.camel.Exchange;
import org.apache.camel.component.cxf.jaxrs.DefaultCxfRsBinding;
import org.apache.cxf.helpers.IOUtils;
import org.apache.cxf.jaxrs.impl.ResponseImpl;
import org.springframework.stereotype.Component;

@Component
public class CfxRsNotStreamingBinding extends DefaultCxfRsBinding {

    @Override
    public Object bindResponseToCamelBody(Object response, Exchange camelExchange) throws Exception {
        if (response instanceof ResponseImpl && ((ResponseImpl) response).getEntity() instanceof InputStream) {
            InputStream inputStream = (InputStream) ((ResponseImpl) response).getEntity();
            return IOUtils.toString(inputStream);
        }
        return super.bindResponseToCamelBody(response, camelExchange);
    }
}
