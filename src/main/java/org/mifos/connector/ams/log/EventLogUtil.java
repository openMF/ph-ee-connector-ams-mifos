package org.mifos.connector.ams.log;

import com.baasflow.commons.events.Event;
import com.baasflow.commons.events.EventLogLevel;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import lombok.experimental.UtilityClass;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@UtilityClass
public class EventLogUtil {

    private static final String AMS_CONNECTOR = "ams_connector";

    public Event.Builder initFineractCall(String event,
                                          Integer disposalAccountId,
                                          Integer conversionAccountId,
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
                                               List<TransactionItem> batchItems,
                                               Integer disposalAccountId,
                                               Integer conversionAccountId,
                                               String internalCorrelationId,
                                               Event.Builder eventBuilder) {
        setHead(event + ".AmsBatch", eventBuilder);
        eventBuilder.setCorrelationIds(new CorrelationIdBuilder()
                .disposalAccountId(disposalAccountId)
                .conversionAccountId(conversionAccountId)
                .internalCorrelationId(internalCorrelationId)
                // TODO should use operation text instead of the requestId
                .add("bacthItems", batchItems.stream()
                        .map(item -> item.requestId().toString())
                        .collect(Collectors.joining(",")))
                .build());
        return eventBuilder;
    }

    public Event.Builder initFineractBatchCallOnUs(String event,
                                                   List<TransactionItem> batchItems,
                                                   Integer debtorDisposalAccountAmsId,
                                                   Integer debtorConversionAccountAmsId,
                                                   Integer creditorDisposalAccountAmsId,
                                                   String internalCorrelationId,
                                                   Event.Builder eventBuilder) {
        setHead(event + ".AmsBatch", eventBuilder);
        eventBuilder.setCorrelationIds(new CorrelationIdBuilder()
                .add("debtorDisposalAccountAmsId", debtorDisposalAccountAmsId)
                .add("debtorConversionAccountAmsId", debtorConversionAccountAmsId)
                .add("creditorDisposalAccountAmsId", creditorDisposalAccountAmsId)
                .internalCorrelationId(internalCorrelationId)
                // TODO should use operation text instead of the requestId
                .add("bacthItems", batchItems.stream()
                        .map(item -> item.requestId().toString())
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

        public CorrelationIdBuilder disposalAccountId(Integer disposalAccountId) {
            correlationIds.put("disposalAccountId", disposalAccountId != null ? Integer.toString(disposalAccountId) : EMPTY);
            return this;
        }

        public CorrelationIdBuilder conversionAccountId(Integer conversionAccountId) {
            correlationIds.put("conversionAccountId", conversionAccountId != null ? Integer.toString(conversionAccountId) : EMPTY);
            return this;
        }

        public Map<String, String> build() {
            return Map.copyOf(correlationIds);
        }
    }
}