package org.mifos.connector.ams.zeebe.workers.accountdetails;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;

@Component
public class AmsDebtorWorker extends AbstractAmsWorker {

	Logger logger = LoggerFactory.getLogger(AmsDebtorWorker.class);
	
	public AmsDebtorWorker() {
	}
	
	public AmsDebtorWorker(RestTemplate restTemplate, HttpHeaders httpHeaders) {
		super(restTemplate, httpHeaders);
	}

	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		logger.info(">>>>>>>>>>>>>>>>>>> AMS debtor worker started <<<<<<<<<<<<<<<<<");
		var variables = activatedJob.getVariablesAsMap();

		String debtorIban = (String) variables.get("debtorIban");
		String tenantId = (String) variables.get("tenantIdentifier");
		logger.info(">>>>>>>>>>>>>>>>>>> looking up debtor iban {} for tenant {} <<<<<<<<<<<<<<<<<<", debtorIban, tenantId);
		
		AmsDataTableQueryResponse[] lookupAccount = lookupAccount(debtorIban, tenantId);
		
		if (lookupAccount.length == 0) {
			logger.error("####################  No entry found for iban {} !!!  #########################", debtorIban);
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
		} else {
			AmsDataTableQueryResponse responseItem = lookupAccount[0];
		
			variables.put("disposalAccountAmsId", responseItem.disposal_account_id());
			variables.put("conversionAccountAmsId", responseItem.conversion_account_id());
			jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
		}
	}
}
