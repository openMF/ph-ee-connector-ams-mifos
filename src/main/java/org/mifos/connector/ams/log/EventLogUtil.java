package org.mifos.connector.ams.log;

import com.baasflow.commons.events.Event;
import com.baasflow.commons.events.EventLogLevel;
import io.camunda.zeebe.client.api.response.ActivatedJob;

import java.util.HashMap;
import java.util.Map;

public final class EventLogUtil {

    public static Event.Builder initFineractCall(String url, Event.Builder eventBuilder) {
        eventBuilder.setSourceModule("ams_connector");
        eventBuilder.setEventLogLevel(EventLogLevel.INFO);
        eventBuilder.setEvent(url);
        return eventBuilder;
    }

    public static Event.Builder initZeebeJob(ActivatedJob activatedJob,
                                             String event,
                                             Event.Builder eventBuilder) {
        return initZeebeJob(activatedJob, event, null, null, eventBuilder);
    }

    public static Event.Builder initZeebeJob(ActivatedJob activatedJob,
                                             String event,
                                             String internalCorrelationId,
                                             Event.Builder eventBuilder) {
        return initZeebeJob(activatedJob, event, internalCorrelationId, null, eventBuilder);
    }

    public static Event.Builder initZeebeJob(ActivatedJob activatedJob,
                                             String event,
                                             String internalCorrelationId,
                                             String transactionGroupId,
                                             Event.Builder eventBuilder) {
        eventBuilder.setSourceModule("ams_connector");
        eventBuilder.setEventLogLevel(EventLogLevel.INFO);
        eventBuilder.setEvent(event);
        Map<String, String> correlationIds = new HashMap<>();
        correlationIds.put("processInstanceKey", Long.toString(activatedJob.getProcessInstanceKey()));
        if (internalCorrelationId != null) {
            correlationIds.put("internalCorrelationId", internalCorrelationId);
        }
        if (transactionGroupId != null) {
            correlationIds.put("transactionGroupId", transactionGroupId);
        }
        eventBuilder.setCorrelationIds(correlationIds);
        return eventBuilder;
    }
}