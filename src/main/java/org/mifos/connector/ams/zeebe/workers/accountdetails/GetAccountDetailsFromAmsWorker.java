package org.mifos.connector.ams.zeebe.workers.accountdetails;

import com.baasflow.commons.events.Event;
import com.baasflow.commons.events.EventService;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import lombok.extern.slf4j.Slf4j;
import org.apache.fineract.client.models.GetSavingsAccountsAccountIdResponse;
import org.mifos.connector.ams.common.SavingsAccountStatusType;
import org.mifos.connector.ams.common.util.BeanWalker;
import org.mifos.connector.ams.fineract.currentaccount.response.CAGetResponse;
import org.mifos.connector.ams.fineract.currentaccount.response.FineractResponse;
import org.mifos.connector.ams.fineract.currentaccount.response.Identifier;
import org.mifos.connector.ams.fineract.currentaccount.response.IdentifiersResponse;
import org.mifos.connector.ams.fineract.currentaccount.response.PageFineractResponse;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.log.LogInternalCorrelationId;
import org.mifos.connector.ams.log.TraceZeebeArguments;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.mifos.connector.ams.common.util.BeanWalker.element;

@Component
@Slf4j
public class GetAccountDetailsFromAmsWorker extends AbstractAmsWorker {

    @Value("${fineract.incoming-money-api}")
    private String incomingMoneyApi;

    @Value("${ams.account-type-key.conversion}")
    private String conversionSub;

    @Value("${ams.account-type-key.disposal}")
    private String disposalSub;
    @Autowired
    private EventService eventService;

    private Map<String, String> accountNotExistsReasons = Map.of(
            "HCT_INST-IN", "AC03",
            "IG2-IN", "AC01",
            "ON_US-IN", "BX01",
            "HCT_INST-OUT", "AC02"
    );

    private Map<String, String> accountClosedReasons = Map.of(
            "HCT_INST-IN", "AC07",
            "IG2-IN", "AC04",
            "ON_US-IN", "BX02",
            "HCT_INST-OUT", "AC04"
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

    // TODO add getSavingsAccountDetailsFromAms


    private Map<String, Object> getAccountDetailsFromAms(String internalCorrelationId,
                                                         String iban,
                                                         String tenantIdentifier,
                                                         String currency,
                                                         String paymentScheme,
                                                         String direction,
                                                         Event.Builder eventBuilder) {


        String paymentSchemePrefix = paymentScheme.split(":")[0];

        try {


            BeanWalker<List<FineractResponse>> disposalAccountData = BeanWalker.of(lookupCurrentAccountPostFlagsAndStatus(iban, disposalSub, tenantIdentifier)).get(PageFineractResponse::getContent);

            CAGetResponse disposalAccount = lookupCurrentAccountGet(iban, disposalSub, tenantIdentifier);
            CAGetResponse conversionAccount = lookupCurrentAccountGet(iban, conversionSub, tenantIdentifier);
            IdentifiersResponse disposalAccountIdentifiers = lookupIdentifiersGet(iban, disposalSub, tenantIdentifier);

            String disposalAccountStatus = BeanWalker.of(disposalAccount).get(CAGetResponse::getStatus).get(CAGetResponse.Status::getId).get();
            String conversionAccountStatus = BeanWalker.of(conversionAccount).get(CAGetResponse::getStatus).get(CAGetResponse.Status::getId).get();

            Boolean AccountStatusCheckResult = (Objects.equals(disposalAccountStatus, "ACTIVE") &&
                    Objects.equals(conversionAccountStatus, "ACTIVE"));
            Boolean CurrencyCheckResult = (Objects.equals(BeanWalker.of(disposalAccount).get(CAGetResponse::getCurrency).get(CAGetResponse.Currency::getCode).get(), "HUF") &&
                    Objects.equals(BeanWalker.of(conversionAccount).get(CAGetResponse::getCurrency).get(CAGetResponse.Currency::getCode).get(), "HUF"));
            //TODO map fineract response
            String status = AccountAmsStatus.NOT_READY_FOR_TRANSACTION.name();

            if (AccountStatusCheckResult && CurrencyCheckResult) {
                status = AccountAmsStatus.READY_FOR_TRANSACTION.name();
            } else {
                log.info("Conversion account currency: {}, disposal account: {}. Account is not ready to receive money.", conversionAccount, disposalAccount);
            }

            String reasonCode = "NOT_PROVIDED";
            if (Objects.equals(disposalAccountStatus, "CLOSED") || Objects.equals(conversionAccountStatus, "CLOSED")) {
                reasonCode = accountClosedReasons.getOrDefault(paymentSchemePrefix + "-" + direction, "NOT_PROVIDED");
                log.info("CLOSED account, returning reasonCode based on scheme and direction: {}-{}: {}", paymentSchemePrefix, direction, reasonCode);
            }

            if (!CurrencyCheckResult) {
                if (AccountStatusCheckResult)
                    reasonCode = "AM03";
            }

            HashMap<String, Object> outputVariables = new HashMap<>();


            outputVariables.put("accountAmsStatus", status);

            String conversionAccountId = BeanWalker.of(conversionAccount).get(CAGetResponse::getId).get();
            if (Objects.nonNull(conversionAccountId))
                outputVariables.put("conversionAccountAmsId", conversionAccountId);

            String disposalAccountId = BeanWalker.of(disposalAccount).get(CAGetResponse::getId).get();
            if (Objects.nonNull(disposalAccountId)) outputVariables.put("disposalAccountAmsId", disposalAccountId);

            List<String> flagCodes = disposalAccountData.get()
                    .stream().map(FineractResponse::getFlagCode).toList();

            for (String flagCode : flagCodes) {
                if (flagCode.equals("blockedPublic") && direction.equals("IN") && paymentSchemePrefix.equals("HCT_INST")) {
                    reasonCode = "AC06";
                    break;
                }
            }

            outputVariables.put("disposalAccountFlags", flagCodes);

            String disposalAccountStatusType = disposalAccountData.get(element(0)).get(FineractResponse::getStatusType).get();
            if (Objects.nonNull(conversionAccountId))
                outputVariables.put("disposalAccountAmsStatusType", disposalAccountStatusType);

            String internalAccountId = Optional.ofNullable(BeanWalker.of(disposalAccountIdentifiers).get(IdentifiersResponse::getSecondaryIdentifiers).get()).orElse(List.of())
                    .stream().filter(x -> Objects.equals(x.getIdType(), "alias")).map(Identifier::getValue).findFirst().orElse("NOT_PROVIDED");
            if (Objects.nonNull(conversionAccountId)) outputVariables.put("internalAccountId", internalAccountId);

            outputVariables.put("accountProductType", "CURRENT");
            outputVariables.put("reasonCode", reasonCode);

            return outputVariables;

        } catch (HttpClientErrorException.NotFound e) {
            AmsDataTableQueryResponse[] response = lookupSavingsAccount(iban, tenantIdentifier);   // TODO try to lookup CurrentAccount first
            log.info("1/4: Account details retrieval finished");

            if (response.length == 0) {
                // TODO handle also 4xx errors
                // TODO give another try at getSavingsAccountDetailsFromAms
                String reasonCode = accountNotExistsReasons.getOrDefault(paymentSchemePrefix + "-" + direction, "NOT_PROVIDED");
                log.debug("Account not found in AMS, returning reasonCode based on scheme and direction: {}-{}: {}", paymentSchemePrefix, direction, reasonCode);

                return Map.of(
                        "accountAmsStatus", AccountAmsStatus.NOT_READY_FOR_TRANSACTION.name(),
                        "reasonCode", reasonCode,
                        "accountProductType", "NOT_PROVIDED",
                        "conversionAccountAmsId", "NOT_PROVIDED",
                        "internalAccountId", "NOT_PROVIDED",
                        "disposalAccountAmsId", "NOT_PROVIDED",
                        "disposalAccountFlags", Collections.emptyList(),
                        "disposalAccountAmsStatusType", "NOT_PROVIDED"
                );
            }

            String status = AccountAmsStatus.NOT_READY_FOR_TRANSACTION.name();

            var responseItem = response[0];
            Long accountConversionId = responseItem.conversion_account_id();
            Long accountDisposalId = responseItem.disposal_account_id();
            String internalAccountId = responseItem.internal_account_id();

            log.info("Retrieving conversion account data");
            GetSavingsAccountsAccountIdResponse conversion = retrieveCurrencyIdAndStatus(accountConversionId, tenantIdentifier);
            log.trace("conversion account details: {}", conversion);
            log.info("2/4: Conversion account data retrieval finished");

            GetSavingsAccountsAccountIdResponse disposal = retrieveCurrencyIdAndStatus(accountDisposalId, tenantIdentifier);
            log.trace("disposal account details: {}", disposal);
            log.info("3/4: Disposal account data retrieval finished");

            Integer disposalAccountAmsId = disposal.getId();
            Integer conversionAccountAmsId = conversion.getId();

            if (currency.equalsIgnoreCase(conversion.getCurrency().getCode())
                    && currency.equalsIgnoreCase(disposal.getCurrency().getCode())
                    && conversion.getStatus().getId() == 300
                    && disposal.getStatus().getId() == 300) {
                status = AccountAmsStatus.READY_FOR_TRANSACTION.name();
            } else {
                log.info("Conversion account currency: {}, disposal account: {}. Account is not ready to receive money.", conversion, disposal);
            }

            List<Object> flags = lookupFlags(accountDisposalId, tenantIdentifier);
            log.info("4/4: Disposal account flags retrieval finished");

            SavingsAccountStatusType statusType = null;
            for (SavingsAccountStatusType statType : SavingsAccountStatusType.values()) {
                if (Objects.equals(statType.getValue(), disposal.getStatus().getId())) {
                    statusType = statType;
                    break;
                }
            }

            log.trace("IBAN {} status is {}", iban, status);

            String reasonCode = "NOT_PROVIDED";
            if (SavingsAccountStatusType.CLOSED.equals(statusType)) {
                reasonCode = accountClosedReasons.getOrDefault(paymentSchemePrefix + "-" + direction, "NOT_PROVIDED");
                log.info("CLOSED account, returning reasonCode based on scheme and direction: {}-{}: {}", paymentSchemePrefix, direction, reasonCode);
            }

            if (AccountAmsStatus.NOT_READY_FOR_TRANSACTION.name().equalsIgnoreCase(status) && SavingsAccountStatusType.ACTIVE.equals(statusType)) {
                statusType = SavingsAccountStatusType.INVALID;
                reasonCode = "AM03";
            }
            log.trace("IBAN {} status is {}", iban, status);
            HashMap<String, Object> outputVariables = new HashMap<>();
            outputVariables.put("accountAmsStatus", status);
            outputVariables.put("accountProductType", "SAVINGS");
            outputVariables.put("conversionAccountAmsId", conversionAccountAmsId);
            outputVariables.put("disposalAccountAmsId", disposalAccountAmsId);
            outputVariables.put("disposalAccountFlags", flags);
            outputVariables.put("disposalAccountAmsStatusType", statusType);
            outputVariables.put("internalAccountId", internalAccountId);
            outputVariables.put("reasonCode", reasonCode);
            return outputVariables;

        }

    }

    private GetSavingsAccountsAccountIdResponse retrieveCurrencyIdAndStatus(Long accountCurrencyId, String tenantId) {
        return lookupExchange(UriComponentsBuilder
                        .fromHttpUrl(fineractApiUrl)
                        .path(incomingMoneyApi)
                        .path(String.format("%d", accountCurrencyId))
                        .encode().toUriString(),
                GetSavingsAccountsAccountIdResponse.class,
                tenantId,
                "ams_connector",
                "getAccountDetailsFromAms - retrieveCurrencyIdAndStatus");
    }
}