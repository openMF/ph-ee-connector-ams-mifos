package org.mifos.connector.ams.zeebe.workers.accountdetails;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.web.client.RestTemplate;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;

public class AmsDebtorWorker extends AbstractAmsWorker {

	Logger logger = LoggerFactory.getLogger(AmsDebtorWorker.class);
	
	public AmsDebtorWorker() {
	}
	
	public AmsDebtorWorker(RestTemplate restTemplate, HttpHeaders httpHeaders) {
		super(restTemplate, httpHeaders);
	}

	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		var variables = activatedJob.getVariablesAsMap();

		AmsDataTableQueryResponse responseItem = lookupAccount((String) variables.get("creditorIban"))[0];
		
		variables.put("eCurrencyAccountAmsId", responseItem.ecurrency_account_id());
		variables.put("fiatCurrencyAccountAmsId", responseItem.fiat_currency_account_id());
		
		jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
	}
}
