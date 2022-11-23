package org.mifos.connector.ams.zeebe.workers.accountdetails;

import org.apache.fineract.client.models.GetSavingsAccountsAccountIdResponse;
import org.jboss.logging.MDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;

@Component
public class AmsCreditorWorker extends AbstractAmsWorker {

	@Value("${fineract.incoming-money-api}")
	private String incomingMoneyApi;

	Logger logger = LoggerFactory.getLogger(AmsCreditorWorker.class);
	
	public AmsCreditorWorker() {
	}

	public AmsCreditorWorker(RestTemplate restTemplate, HttpHeaders httpHeaders) {
		super(restTemplate, httpHeaders);
	}

	@Override
	public void handle(JobClient jobClient, ActivatedJob activatedJob) {
		try {
		var variables = activatedJob.getVariablesAsMap();
		String bicAndEndToEndId = (String) variables.get("bicAndEndToEndId");
		MDC.put("bicAndEndToEndId", bicAndEndToEndId);
		String creditorIban = (String) variables.get("creditorIban");
		
		logger.info("Started AMS creditor worker for creditor IBAN {}", creditorIban);
		
		AccountAmsStatus status = AccountAmsStatus.NOT_READY_TO_RECEIVE_MONEY;
		String currency = (String) variables.get("currency");

		AmsDataTableQueryResponse[] response = lookupAccount(creditorIban);
		
		variables.put("eCurrencyAccountAmsId", "");
		variables.put("fiatCurrencyAccountAmsId", "");
		
		if (response.length != 0) {
			var responseItem = response[0];
			Long accountFiatCurrencyId = responseItem.fiat_currency_account_id();
			Long accountECurrencyId = responseItem.ecurrency_account_id();

			try {
				GetSavingsAccountsAccountIdResponse fiatCurrency = retrieveCurrencyIdAndStatus(accountFiatCurrencyId);
				GetSavingsAccountsAccountIdResponse eCurrency = retrieveCurrencyIdAndStatus(accountECurrencyId);

				if (currency.equalsIgnoreCase(fiatCurrency.getCurrency().getCode())
						&& fiatCurrency.getStatus().getId() == 300 
						&& eCurrency.getStatus().getId() == 300) {
					status = AccountAmsStatus.READY_TO_RECEIVE_MONEY;
					variables.put("eCurrencyAccountAmsId", eCurrency.getId());
					variables.put("fiatCurrencyAccountAmsId", fiatCurrency.getId());
				}

			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}

		variables.put("accountAmsStatus", status.name());
		
		logger.info("AMS creditor worker for creditor IBAN {} finished with status {}", creditorIban, status);

		MDC.remove("bicAndEndToEndId");
		jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
		} catch (Throwable t) {
			logger.error(t.getMessage(), t);
		}
	}

	private GetSavingsAccountsAccountIdResponse retrieveCurrencyIdAndStatus(Long accountCurrencyId) {
		return exchange(UriComponentsBuilder
				.fromHttpUrl(fineractApiUrl)
				.path(incomingMoneyApi)
				.path(String.format("%d", accountCurrencyId))
				.encode().toUriString(),
				GetSavingsAccountsAccountIdResponse.class);
	}
}
