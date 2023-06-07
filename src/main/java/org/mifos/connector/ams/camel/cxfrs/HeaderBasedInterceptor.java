package org.mifos.connector.ams.camel.cxfrs;

import org.apache.cxf.message.Message;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface HeaderBasedInterceptor {

    String CXF_TRACE_HEADER = "cxfTrace";

    default boolean isCxfTraceEnabled(Message message) {
        return Optional.ofNullable(message.getExchange().get(Message.PROTOCOL_HEADERS))
                .filter(Map.class::isInstance)
                .map(it -> {
                    try {
                        Object headerValue = ((List)((Map) it).get(CXF_TRACE_HEADER)).get(0);
                        return Boolean.TRUE.equals(headerValue instanceof String ? Boolean.valueOf((String) headerValue) :
                                headerValue);
                    } catch (Exception ex) {
                        return false;
                    }
                })
                .orElse(false);
    }
}