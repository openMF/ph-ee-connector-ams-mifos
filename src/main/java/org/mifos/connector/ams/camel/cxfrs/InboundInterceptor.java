package org.mifos.connector.ams.camel.cxfrs;


import org.apache.cxf.ext.logging.LoggingInInterceptor;
import org.apache.cxf.interceptor.Fault;
import org.apache.cxf.message.Message;
import org.springframework.stereotype.Component;


@Component
public class InboundInterceptor extends LoggingInInterceptor implements HeaderBasedInterceptor {

    @Override
    public void handleMessage(Message message) throws Fault {
        if (isCxfTraceEnabled(message)) {
            super.handleMessage(message);
        }
    }
}
