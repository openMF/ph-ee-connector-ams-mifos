package org.mifos.connector.ams.log;

import com.baasflow.commons.events.Event;
import com.baasflow.commons.events.EventLogLevel;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class EventLogUtil {

    public static final String AMS_CONNECTOR = "ams_connector";

    public static Event.Builder initFineractCall(String calledFrom,
                                                 Integer disposalAccountId,
                                                 Integer conversionAccountId,
                                                 String internalCorrelationId,
                                                 Event.Builder eventBuilder) {
        eventBuilder.setSourceModule(AMS_CONNECTOR);
        eventBuilder.setEventLogLevel(EventLogLevel.INFO);
        eventBuilder.setEvent(calledFrom);
        Map<String, String> correlationIds = new HashMap<>();
        correlationIds.put("disposalAccountId", Integer.toString(disposalAccountId));
        correlationIds.put("conversionAccountId", Integer.toString(conversionAccountId));
        correlationIds.put("internalCorrelationId", internalCorrelationId);
        eventBuilder.setCorrelationIds(correlationIds);
        return eventBuilder;
    }

    public static Event.Builder initFineractBatchCall(String calledFrom,
                                                      List<TransactionItem> batchItems,
                                                      Integer disposalAccountId,
                                                      Integer conversionAccountId,
                                                      String internalCorrelationId,
                                                      Event.Builder eventBuilder) {
        eventBuilder.setSourceModule(AMS_CONNECTOR);
        eventBuilder.setEventLogLevel(EventLogLevel.INFO);
        eventBuilder.setEvent(calledFrom + ".AmsBatch");
        Map<String, String> correlationIds = new HashMap<>();
        correlationIds.put("disposalAccountId", Integer.toString(disposalAccountId));
        correlationIds.put("conversionAccountId", Integer.toString(conversionAccountId));
        // TODO should use operation text instead of the requestId
        correlationIds.put("bacthItems", batchItems.stream()
                .map(item -> item.requestId().toString())
                .collect(Collectors.joining(",")));
        correlationIds.put("internalCorrelationId", internalCorrelationId);
        eventBuilder.setCorrelationIds(correlationIds);
        return eventBuilder;
    }

    public static Event.Builder initFineractBatchCallOnUs(String calledFrom,
                                                          List<TransactionItem> batchItems,
                                                          Integer debtorDisposalAccountAmsId,
                                                          Integer debtorConversionAccountAmsId,
                                                          Integer creditorDisposalAccountAmsId,
                                                          String internalCorrelationId,
                                                          Event.Builder eventBuilder) {
        eventBuilder.setSourceModule(AMS_CONNECTOR);
        eventBuilder.setEventLogLevel(EventLogLevel.INFO);
        eventBuilder.setEvent(calledFrom + ".AmsBatch");
        Map<String, String> correlationIds = new HashMap<>();
        correlationIds.put("debtorDisposalAccountAmsId", Integer.toString(debtorDisposalAccountAmsId));
        correlationIds.put("debtorConversionAccountAmsId", Integer.toString(debtorConversionAccountAmsId));
        correlationIds.put("creditorDisposalAccountAmsId", Integer.toString(creditorDisposalAccountAmsId));
        // TODO should use operation text instead of the requestId
        correlationIds.put("bacthItems", batchItems.stream()
                .map(item -> item.requestId().toString())
                .collect(Collectors.joining(",")));
        correlationIds.put("internalCorrelationId", internalCorrelationId);
        eventBuilder.setCorrelationIds(correlationIds);
        return eventBuilder;
    }

    public static Event.Builder initZeebeJob(ActivatedJob activatedJob,
                                             String event,
                                             String internalCorrelationId,
                                             String transactionGroupId,
                                             Event.Builder eventBuilder) {
        eventBuilder.setSourceModule(AMS_CONNECTOR);
        eventBuilder.setEventLogLevel(EventLogLevel.INFO);
        eventBuilder.setEvent(event);
        Map<String, String> correlationIds = new HashMap<>();
        correlationIds.put("processInstanceKey", Long.toString(activatedJob.getProcessInstanceKey()));
        correlationIds.put("internalCorrelationId", internalCorrelationId);
        correlationIds.put("transactionGroupId", transactionGroupId);
        eventBuilder.setCorrelationIds(correlationIds);
        return eventBuilder;
    }
}