package org.mifos.connector.ams.log;

import com.baasflow.commons.events.Event;
import com.baasflow.commons.events.EventLogLevel;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import lombok.experimental.UtilityClass;
import org.mifos.connector.ams.zeebe.workers.utils.BatchItem;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@UtilityClass
public class EventLogUtil {

    private static final String AMS_CONNECTOR = "ams_connector";

    public Event.Builder initFineractCall(String event,
                                          String disposalAccountId,
                                          String conversionAccountId,
                                          String internalCorrelationId,
                                          Event.Builder eventBuilder) {
        setHead(event, eventBuilder);
        eventBuilder.setCorrelationIds(new CorrelationIdBuilder()
                .disposalAccountId(disposalAccountId)
                .conversionAccountId(conversionAccountId)
                .internalCorrelationId(internalCorrelationId)
                .build());
        return eventBuilder;
    }

    public Event.Builder initFineractBatchCall(String event,
                                               List<BatchItem> batchItems,
                                               String disposalAccountId,
                                               String conversionAccountId,
                                               String internalCorrelationId,
                                               Event.Builder eventBuilder) {
        setHead(event + ".AmsBatch", eventBuilder);
        eventBuilder.setCorrelationIds(new CorrelationIdBuilder()
                .disposalAccountId(disposalAccountId)
                .conversionAccountId(conversionAccountId)
                .internalCorrelationId(internalCorrelationId)
                // TODO should use operation text instead of the requestId
                .add("batchItems", batchItems.stream()
                        .map(item -> item.getRequestId().toString())
                        .collect(Collectors.joining(",")))
                .build());
        return eventBuilder;
    }

    public Event.Builder initFineractBatchCallOnUs(String event,
                                                   List<BatchItem> batchItems,
                                                   String debtorDisposalAccountAmsId,
                                                   String debtorConversionAccountAmsId,
                                                   String creditorDisposalAccountAmsId,
                                                   String internalCorrelationId,
                                                   Event.Builder eventBuilder) {
        setHead(event + ".AmsBatch", eventBuilder);
        eventBuilder.setCorrelationIds(new CorrelationIdBuilder()
                .add("debtorDisposalAccountAmsId", debtorDisposalAccountAmsId)
                .add("debtorConversionAccountAmsId", debtorConversionAccountAmsId)
                .add("creditorDisposalAccountAmsId", creditorDisposalAccountAmsId)
                .internalCorrelationId(internalCorrelationId)
                // TODO should use operation text instead of the requestId
                .add("batchItems", batchItems.stream()
                        .map(item -> item.getRequestId().toString())
                        .collect(Collectors.joining(",")))
                .build());
        return eventBuilder;
    }

    public Event.Builder initZeebeJob(ActivatedJob activatedJob,
                                      String event,
                                      String internalCorrelationId,
                                      String transactionGroupId,
                                      Event.Builder eventBuilder) {
        setHead(event, eventBuilder);
        eventBuilder.setCorrelationIds(new CorrelationIdBuilder()
                .add("processInstanceKey", activatedJob.getProcessInstanceKey())
                .add("transactionGroupId", transactionGroupId)
                .internalCorrelationId(internalCorrelationId)
                .build());
        return eventBuilder;
    }

    private void setHead(String event, Event.Builder eventBuilder) {
        eventBuilder.setSourceModule(AMS_CONNECTOR);
        eventBuilder.setEventLogLevel(EventLogLevel.INFO);
        eventBuilder.setEvent(event);
    }

    private static class CorrelationIdBuilder {

        private static final String EMPTY = "N/A";
        private final Map<String, String> correlationIds;

        public CorrelationIdBuilder() {
            correlationIds = new HashMap<>();
        }

        public CorrelationIdBuilder add(String id, Object value) {
            correlationIds.put(id, value != null ? value.toString() : EMPTY);
            return this;
        }

        public CorrelationIdBuilder internalCorrelationId(String internalCorrelationId) {
            correlationIds.put("internalCorrelationId", internalCorrelationId != null ? internalCorrelationId : EMPTY);
            return this;
        }

        public CorrelationIdBuilder disposalAccountId(String disposalAccountId) {
            correlationIds.put("disposalAccountId", disposalAccountId != null ? disposalAccountId : EMPTY);
            return this;
        }

        public CorrelationIdBuilder conversionAccountId(String conversionAccountId) {
            correlationIds.put("conversionAccountId", conversionAccountId != null ? conversionAccountId : EMPTY);
            return this;
        }

        public Map<String, String> build() {
            return Map.copyOf(correlationIds);
        }
    }
}