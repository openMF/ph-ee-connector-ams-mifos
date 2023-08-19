package org.mifos.connector.ams.zeebe.workers.accountdetails;

import com.baasflow.commons.events.Event;
import com.baasflow.commons.events.EventService;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import org.apache.fineract.client.models.GetSavingsAccountsAccountIdResponse;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.common.ams.dto.SavingsAccountStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@Component
public class GetAccountDetailsFromAmsWorker extends AbstractAmsWorker {

    @Value("${fineract.incoming-money-api}")
    private String incomingMoneyApi;

    @Autowired
    private EventService eventService;

    Logger logger = LoggerFactory.getLogger(GetAccountDetailsFromAmsWorker.class);

    @JobWorker
    public Map<String, Object> getAccountDetailsFromAms(JobClient jobClient,
                                                        ActivatedJob activatedJob,
                                                        @Variable String internalCorrelationId,
                                                        @Variable String iban,
                                                        @Variable String tenantIdentifier,
                                                        @Variable String currency) {
        MDC.put("internalCorrelationId", internalCorrelationId);
        try {
            return eventService.auditedEvent(
                    eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "getAccountDetailsFromAms", eventBuilder),
                    eventBuilder -> getAccountDetailsFromAms(internalCorrelationId, iban, tenantIdentifier, currency, eventBuilder));
        } finally {
            MDC.remove("internalCorrelationId");
        }
    }

    private Map<String, Object> getAccountDetailsFromAms(String internalCorrelationId,
                                                         String iban,
                                                         String tenantIdentifier,
                                                         String currency,
                                                         Event.Builder eventBuilder) {
        logger.info("getAccountDetailsFromAms");
        logger.debug("started AMS creditor worker for creditor IBAN {} and Tenant Id {}", iban, tenantIdentifier);
        eventBuilder.getCorrelationIds().put("internalCorrelationId", internalCorrelationId);

        AmsDataTableQueryResponse[] response = lookupAccount(iban, tenantIdentifier);

        if (response.length == 0) {
            throw new ZeebeBpmnError("Error_AccountNotFound", "Error_AccountNotFound");
        }

        String status = AccountAmsStatus.NOT_READY_TO_RECEIVE_MONEY.name();

        var responseItem = response[0];
        Long accountConversionId = responseItem.conversion_account_id();
        Long accountDisposalId = responseItem.disposal_account_id();
        Long internalAccountId = responseItem.internal_account_id();

        GetSavingsAccountsAccountIdResponse conversion = retrieveCurrencyIdAndStatus(accountConversionId, tenantIdentifier);
        logger.debug("Conversion account details: {}", conversion);

        GetSavingsAccountsAccountIdResponse disposal = retrieveCurrencyIdAndStatus(accountDisposalId, tenantIdentifier);
        logger.debug("Disposal account details: {}", disposal);

        Integer disposalAccountAmsId = disposal.getId();
        Integer conversionAccountAmsId = conversion.getId();

        if (currency.equalsIgnoreCase(conversion.getCurrency().getCode())
                && conversion.getStatus().getId() == 300
                && disposal.getStatus().getId() == 300) {
            status = AccountAmsStatus.READY_TO_RECEIVE_MONEY.name();
        }

        List<Object> flags = lookupFlags(accountDisposalId, tenantIdentifier);

        SavingsAccountStatusType statusType = null;
        for (SavingsAccountStatusType statType : SavingsAccountStatusType.values()) {
            if (Objects.equals(statType.getValue(), disposal.getStatus().getId())) {
                statusType = statType;
                break;
            }
        }

        logger.debug("AMS creditor worker for creditor IBAN {} finished with status {}", iban, status);

        return Map.of("disposalAccountAmsId", disposalAccountAmsId,
                "conversionAccountAmsId", conversionAccountAmsId,
                "accountAmsStatus", status,
                "internalAccountId", internalAccountId,
                "disposalAccountFlags", flags,
                "disposalAccountAmsStatusType", statusType);
    }

    private GetSavingsAccountsAccountIdResponse retrieveCurrencyIdAndStatus(Long accountCurrencyId, String tenantId) {
        return exchange(UriComponentsBuilder
                        .fromHttpUrl(fineractApiUrl)
                        .path(incomingMoneyApi)
                        .path(String.format("%d", accountCurrencyId))
                        .encode().toUriString(),
                GetSavingsAccountsAccountIdResponse.class,
                tenantId);
    }
}