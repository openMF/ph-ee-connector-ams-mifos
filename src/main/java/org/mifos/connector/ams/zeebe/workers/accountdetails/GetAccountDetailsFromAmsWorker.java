package org.mifos.connector.ams.zeebe.workers.accountdetails;

import com.baasflow.commons.events.Event;
import com.baasflow.commons.events.EventService;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import lombok.extern.slf4j.Slf4j;
import org.apache.fineract.client.models.GetSavingsAccountsAccountIdResponse;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.log.LogInternalCorrelationId;
import org.mifos.connector.ams.log.TraceZeebeArguments;
import org.mifos.connector.common.ams.dto.SavingsAccountStatusType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Component
@Slf4j
public class GetAccountDetailsFromAmsWorker extends AbstractAmsWorker {

    @Value("${fineract.incoming-money-api}")
    private String incomingMoneyApi;

    @Autowired
    private EventService eventService;

    private Map<String, String> schemeAndDirectionToReasonCode = Map.of(
            "HCT_INST-IN", "AC07",
            "IG2-IN", "AC04"
    );


    @JobWorker
    @LogInternalCorrelationId
    @TraceZeebeArguments
    public Map<String, Object> getAccountDetailsFromAms(JobClient jobClient,
                                                        ActivatedJob activatedJob,
                                                        @Variable String internalCorrelationId,
                                                        @Variable String iban,
                                                        @Variable String tenantIdentifier,
                                                        @Variable String currency,
                                                        @Variable String paymentScheme,
                                                        @Variable String direction) {
        log.info("getAccountDetailsFromAms");
        return eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "getAccountDetailsFromAms", internalCorrelationId, null, eventBuilder),
                eventBuilder -> getAccountDetailsFromAms(internalCorrelationId, iban, tenantIdentifier, currency, paymentScheme, direction, eventBuilder));
    }

    private Map<String, Object> getAccountDetailsFromAms(String internalCorrelationId,
                                                         String iban,
                                                         String tenantIdentifier,
                                                         String currency,
                                                         String paymentScheme,
                                                         String direction,
                                                         Event.Builder eventBuilder) {
        AmsDataTableQueryResponse[] response = lookupAccount(iban, tenantIdentifier);

        if (response.length == 0) {
            throw new ZeebeBpmnError("Error_AccountNotFound", String.format("IBAN %s not found in AMS", iban));
        }

        String status = AccountAmsStatus.NOT_READY_TO_RECEIVE_MONEY.name();

        var responseItem = response[0];
        Long accountConversionId = responseItem.conversion_account_id();
        Long accountDisposalId = responseItem.disposal_account_id();
        Long internalAccountId = responseItem.internal_account_id();

        GetSavingsAccountsAccountIdResponse conversion = retrieveCurrencyIdAndStatus(accountConversionId, tenantIdentifier);
        log.trace("conversion account details: {}", conversion);

        GetSavingsAccountsAccountIdResponse disposal = retrieveCurrencyIdAndStatus(accountDisposalId, tenantIdentifier);
        log.trace("disposal account details: {}", disposal);

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

        log.trace("IBAN {} status is {}", iban, status);

        HashMap<String, Object> outputVariables = new HashMap<>();
        outputVariables.put("disposalAccountAmsId", disposalAccountAmsId);
        outputVariables.put("conversionAccountAmsId", conversionAccountAmsId);
        outputVariables.put("accountAmsStatus", status);
        outputVariables.put("internalAccountId", internalAccountId);
        outputVariables.put("disposalAccountFlags", flags);
        outputVariables.put("disposalAccountAmsStatusType", statusType);

        String reasonCode = null;
        if (SavingsAccountStatusType.CLOSED.equals(statusType)) {
            reasonCode = schemeAndDirectionToReasonCode.get(paymentScheme + "-" + direction);
            log.info("CLOSED account, returning reasonCode based on scheme and direction: {}-{}: {}", paymentScheme, direction, reasonCode);
        }

        outputVariables.put("reasonCode", reasonCode != null ? reasonCode : "NOT_PROVIDED");
        return outputVariables;
    }

    private GetSavingsAccountsAccountIdResponse retrieveCurrencyIdAndStatus(Long accountCurrencyId, String tenantId) {
        return exchange(UriComponentsBuilder
                        .fromHttpUrl(fineractApiUrl)
                        .path(incomingMoneyApi)
                        .path(String.format("%d", accountCurrencyId))
                        .encode().toUriString(),
                GetSavingsAccountsAccountIdResponse.class,
                tenantId,
                "getAccountDetailsFromAms");
    }
}