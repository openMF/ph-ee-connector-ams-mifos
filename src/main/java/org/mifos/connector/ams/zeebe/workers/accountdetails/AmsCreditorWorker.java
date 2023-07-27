package org.mifos.connector.ams.zeebe.workers.accountdetails;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.fineract.client.models.GetSavingsAccountsAccountIdResponse;
import org.mifos.connector.common.ams.dto.SavingsAccountStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;

@Component
public class AmsCreditorWorker extends AbstractAmsWorker {

	@Value("${fineract.incoming-money-api}")
	private String incomingMoneyApi;

	Logger logger = LoggerFactory.getLogger(AmsCreditorWorker.class);
	
	public AmsCreditorWorker() {
	}

	public AmsCreditorWorker(RestTemplate restTemplate) {
		super(restTemplate);
	}

	@JobWorker
	public Map<String, Object> getAccountDetailsFromAms(JobClient jobClient, 
			ActivatedJob activatedJob,
			@Variable String internalCorrelationId,
			@Variable String iban,
			@Variable String tenantIdentifier,
			@Variable String currency) {
		
		Integer disposalAccountAmsId = null;
		Integer conversionAccountAmsId = null;
		Long internalAccountId = null;
		String status = AccountAmsStatus.NOT_READY_TO_RECEIVE_MONEY.name();
		List<Object> flags = null;
		SavingsAccountStatusType statusType = null;

		try {
			MDC.put("internalCorrelationId", internalCorrelationId);
			
			logger.info("Started AMS creditor worker for creditor IBAN {} and Tenant Id {}", iban, tenantIdentifier);
			
			AmsDataTableQueryResponse[] response = lookupAccount(iban, tenantIdentifier);
			
			if (response.length != 0) {
				var responseItem = response[0];
				Long accountConversionId = responseItem.conversion_account_id();
				Long accountDisposalId = responseItem.disposal_account_id();
				internalAccountId = responseItem.internal_account_id();

				try {
					GetSavingsAccountsAccountIdResponse conversion = retrieveCurrencyIdAndStatus(accountConversionId, tenantIdentifier);
					GetSavingsAccountsAccountIdResponse disposal = retrieveCurrencyIdAndStatus(accountDisposalId, tenantIdentifier);
					
					logger.debug("Conversion account details: {}", conversion);
					logger.debug("Disposal account details: {}", disposal);
	
					disposalAccountAmsId = disposal.getId();
					conversionAccountAmsId = conversion.getId();
					
					if (currency.equalsIgnoreCase(conversion.getCurrency().getCode())
							&& conversion.getStatus().getId() == 300 
							&& disposal.getStatus().getId() == 300) {
						status = AccountAmsStatus.READY_TO_RECEIVE_MONEY.name();
					}
					
					flags = lookupFlags(accountDisposalId, tenantIdentifier);

					for (SavingsAccountStatusType statType : SavingsAccountStatusType.values()) {
						if (Objects.equals(statType.getValue(), disposal.getStatus().getId())) {
							statusType = statType;
							break;
						}
					}
	
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
					throw e;
				}
			} else {
				throw new ZeebeBpmnError("Error_AccountNotFound", "Error_AccountNotFound");
			}
	
			
			logger.debug("AMS creditor worker for creditor IBAN {} finished with status {}", iban, status);
	
			MDC.remove("internalCorrelationId");
			
		} catch (Throwable t) {
			logger.error(t.getMessage(), t);
			throw new ZeebeBpmnError("Error_AccountNotFound", t.getMessage());
		}
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
