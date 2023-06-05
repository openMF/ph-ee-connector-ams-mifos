package org.mifos.connector.ams.zeebe.workers.accountdetails;

import java.util.Map;

import org.apache.fineract.client.models.GetSavingsAccountsAccountIdResponse;
import org.jboss.logging.MDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;

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

		try {
			MDC.put("internalCorrelationId", internalCorrelationId);
			
			logger.info("Started AMS creditor worker for creditor IBAN {} and Tenant Id {}", iban, tenantIdentifier);
			
			AmsDataTableQueryResponse[] response = lookupAccount(iban, tenantIdentifier);
			
			if (response.length != 0) {
				var responseItem = response[0];
				Long accountFiatCurrencyId = responseItem.conversion_account_id();
				Long accountECurrencyId = responseItem.disposal_account_id();
				internalAccountId = responseItem.internal_account_id();

				try {
					GetSavingsAccountsAccountIdResponse conversion = retrieveCurrencyIdAndStatus(accountFiatCurrencyId, tenantIdentifier);
					GetSavingsAccountsAccountIdResponse disposal = retrieveCurrencyIdAndStatus(accountECurrencyId, tenantIdentifier);
	
					if (currency.equalsIgnoreCase(conversion.getCurrency().getCode())
							&& conversion.getStatus().getId() == 300 
							&& disposal.getStatus().getId() == 300) {
						status = AccountAmsStatus.READY_TO_RECEIVE_MONEY.name();
						disposalAccountAmsId = disposal.getId();
						conversionAccountAmsId = conversion.getId();
					}
	
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
				}
			}
	
			
			logger.debug("AMS creditor worker for creditor IBAN {} finished with status {}", iban, status);
	
			MDC.remove("internalCorrelationId");
			
		} catch (Throwable t) {
			logger.error(t.getMessage(), t);
		}
		return Map.of("disposalAccountAmsId", disposalAccountAmsId,
				"conversionAccountAmsId", conversionAccountAmsId,
				"accountAmsStatus", status,
				"internalAccountId", internalAccountId);
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
