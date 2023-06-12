package org.mifos.connector.ams.zeebe.workers.accountdetails;

import java.util.Map;

import org.mifos.connector.ams.zeebe.workers.utils.AccountStatusDto;
import org.mifos.connector.ams.zeebe.workers.utils.GetSavingsAccountsAccountIdResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponentsBuilder;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;

@Component
public class AmsCreditorWorker {

	@Value("${fineract.incoming-money-api}")
	private String incomingMoneyApi;
	
	@Value("${fineract.api-url}")
	protected String fineractApiUrl;
	
	@Autowired
	private AmsWorkerHelper amsWorkerHelper;

	Logger logger = LoggerFactory.getLogger(AmsCreditorWorker.class);

	@JobWorker
	public Map<String, Object> getAccountDetailsFromAms(JobClient jobClient, 
			ActivatedJob activatedJob,
			@Variable String internalCorrelationId,
			@Variable String iban,
			@Variable String tenantIdentifier,
			@Variable String currency) {
		
		AccountStatusDto accountStatusDto = new AccountStatusDto(null, null, null, AccountAmsStatus.NOT_READY_TO_RECEIVE_MONEY);

		try {
			MDC.put("internalCorrelationId", internalCorrelationId);
			
			logger.info("Started AMS creditor worker for creditor IBAN {} and Tenant Id {}", iban, tenantIdentifier);
			
			AmsDataTableQueryResponse[] response = amsWorkerHelper.lookupAccount(iban, tenantIdentifier);
			
			if (response.length != 0) {
				var responseItem = response[0];
				accountStatusDto.internalAccount(responseItem.internal_account_id());

				fetchAccountIdAndStatus(tenantIdentifier, 
						currency, 
						accountStatusDto, 
						responseItem.conversion_account_id(),
						responseItem.disposal_account_id());
			}
	
			
			logger.debug("AMS creditor worker for creditor IBAN {} finished with status {}", iban, accountStatusDto.status());
	
			MDC.remove("internalCorrelationId");
			
		} catch (Throwable t) {
			logger.error(t.getMessage(), t);
		}
		return Map.of("disposalAccountAmsId", accountStatusDto.disposalAccount(),
				"conversionAccountAmsId", accountStatusDto.conversionAccount(),
				"accountAmsStatus", accountStatusDto.status().name(),
				"internalAccountId", accountStatusDto.internalAccount());
	}

	private void fetchAccountIdAndStatus(String tenantIdentifier, String currency, AccountStatusDto accountStatusDto,
			Long accountConversionCurrencyId, Long accountDisposalId) {
		try {
			GetSavingsAccountsAccountIdResponse conversion = retrieveCurrencyIdAndStatus(accountConversionCurrencyId, tenantIdentifier);
			GetSavingsAccountsAccountIdResponse disposal = retrieveCurrencyIdAndStatus(accountDisposalId, tenantIdentifier);

			if (currency.equalsIgnoreCase(conversion.currency().code())
					&& conversion.status().id() == 300 
					&& disposal.status().id() == 300) {
				accountStatusDto.status(AccountAmsStatus.READY_TO_RECEIVE_MONEY);
				accountStatusDto.disposalAccount(disposal.id());
				accountStatusDto.conversionAccount(conversion.id());
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	private GetSavingsAccountsAccountIdResponse retrieveCurrencyIdAndStatus(Long accountCurrencyId, String tenantId) {
		return amsWorkerHelper.exchange(UriComponentsBuilder
				.fromHttpUrl(fineractApiUrl)
				.path(incomingMoneyApi)
				.path(String.format("%d", accountCurrencyId))
				.encode().toUriString(),
				GetSavingsAccountsAccountIdResponse.class,
				tenantId);
	}
}
