package org.mifos.connector.ams.camel.cxfrs;

import org.apache.cxf.ext.logging.LoggingOutInterceptor;
import org.apache.cxf.interceptor.Fault;
import org.apache.cxf.message.Message;
import org.springframework.stereotype.Component;

@Component
public class OutboundInterceptor extends LoggingOutInterceptor implements HeaderBasedInterceptor {

    @Override
    public void handleMessage(Message message) throws Fault {
        if (isCxfTraceEnabled(message)) {
            super.handleMessage(message);
        }
    }
}
