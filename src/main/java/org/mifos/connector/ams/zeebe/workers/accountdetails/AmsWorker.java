package org.mifos.connector.ams.zeebe.workers.accountdetails;

import java.util.Arrays;

import org.apache.fineract.client.models.GetSavingsAccountsAccountIdResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.client.api.worker.JobHandler;

@Component
public class AmsWorker implements JobHandler {

	@Autowired
	private RestTemplate restTemplate;
	
	@Autowired
	private HttpHeaders httpHeaders;

	@Value("${fineract.api-url}")
	private String fineractApiUrl;

	@Value("${fineract.datatable-query-api}")
	private String datatableQueryApi;

	@Value("${fineract.incoming-money-api}")
	private String incomingMoneyApi;

	@Value("${fineract.column-filter}")
	private String columnFilter;

	@Value("${fineract.result-columns}")
	private String resultColumns;

	Logger logger = LoggerFactory.getLogger(AmsWorker.class);

	private static final String[] ACCEPTED_CURRENCIES = new String[] { "HUF" };
	
	public AmsWorker() {
	}

	public AmsWorker(RestTemplate restTemplate) {
		this.restTemplate = restTemplate;
	}

	@Override
	public void handle(JobClient jobClient, ActivatedJob activatedJob) {
		var variables = activatedJob.getVariablesAsMap();
		AccountAmsStatus status = AccountAmsStatus.NOT_READY_TO_RECEIVE_MONEY;

		AmsDataTableQueryResponse[] response = lookupAccount((String) variables.get("creditorIban"));
		
		if (response.length == 0) {
			variables.put("eCurrencyAccountAmsId", "");
			variables.put("fiatCurrencyAccountAmsId", "");
		} else {
			var responseItem = response[0];
			Long accountFiatCurrencyId = responseItem.fiat_currency_account_id();
			Long accountECurrencyId = responseItem.ecurrency_account_id();

			try {
				GetSavingsAccountsAccountIdResponse fiatCurrency = retrieveCurrencyId(accountFiatCurrencyId);
				GetSavingsAccountsAccountIdResponse eCurrency = retrieveCurrencyId(accountECurrencyId);

				if (Arrays.stream(ACCEPTED_CURRENCIES).anyMatch(fiatCurrency.getCurrency().getCode()::equalsIgnoreCase)
						&& fiatCurrency.getStatus().getId() == 300 && eCurrency.getStatus().getId() == 300) {
					status = AccountAmsStatus.READY_TO_RECEIVE_MONEY;
					variables.put("eCurrencyAccountAmsId", eCurrency.getId());
					variables.put("fiatCurrencyAccountAmsId", fiatCurrency.getId());
				}

			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}

		variables.put("accountAmsStatus", status.name());

		jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
	}

	private AmsDataTableQueryResponse[] lookupAccount(String iban) {
		return exchange(UriComponentsBuilder
				.fromHttpUrl(fineractApiUrl)
				.path(datatableQueryApi)
				.queryParam("columnFilter", columnFilter)
				.queryParam("valueFilter", iban)
				.queryParam("resultColumns", resultColumns)
				.encode().toUriString(),
				AmsDataTableQueryResponse[].class);
	}

	private GetSavingsAccountsAccountIdResponse retrieveCurrencyId(Long accountCurrencyId) {
		return exchange(UriComponentsBuilder
				.fromHttpUrl(fineractApiUrl)
				.path(incomingMoneyApi)
				.path(String.format("%d", accountCurrencyId))
				.encode().toUriString(),
				GetSavingsAccountsAccountIdResponse.class);
	}
	
	private <T> T exchange(String urlTemplate, Class<T> responseType) {
		return restTemplate.exchange(
				urlTemplate, 
				HttpMethod.GET, 
				new HttpEntity<>(httpHeaders), 
				responseType)
			.getBody();
	}
}
