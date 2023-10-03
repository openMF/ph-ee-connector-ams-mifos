package org.mifos.connector.ams.zeebe.workers.accountdetails;

import com.baasflow.commons.events.Event;
import com.baasflow.commons.events.EventService;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.log.LogInternalCorrelationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class AmsDebtorWorker extends AbstractAmsWorker {

    Logger logger = LoggerFactory.getLogger(AmsDebtorWorker.class);

    @Autowired
    private EventService eventService;

    @JobWorker
    @LogInternalCorrelationId
    public Map<String, Object> getAccountIdsFromAms(JobClient jobClient,
                                                    ActivatedJob activatedJob,
                                                    @Variable String debtorIban,
                                                    @Variable String tenantIdentifier) {
        return eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "getAccountIdsFromAms", null, null, eventBuilder),
                eventBuilder -> getAccountIdsFromAms(debtorIban, tenantIdentifier, eventBuilder));
    }

    private Map<String, Object> getAccountIdsFromAms(String debtorIban, String tenantIdentifier, Event.Builder eventBuilder) {
        logger.info("getAccountIdsFromAms");
        logger.debug("looking up debtor iban {} for tenant {}", debtorIban, tenantIdentifier);

        eventBuilder.setEvent("getAccountIdsFromAms");

        AmsDataTableQueryResponse[] lookupAccount = lookupAccount(debtorIban, tenantIdentifier);

        if (lookupAccount.length == 0) {
            throw new ZeebeBpmnError(debtorIban, String.format("No entry found for IBAN %s", debtorIban));
        }

        AmsDataTableQueryResponse responseItem = lookupAccount[0];
        return Map.of("disposalAccountAmsId", responseItem.disposal_account_id(),
                "conversionAccountAmsId", responseItem.conversion_account_id(),
                "internalAccountId", responseItem.internal_account_id());
    }
}