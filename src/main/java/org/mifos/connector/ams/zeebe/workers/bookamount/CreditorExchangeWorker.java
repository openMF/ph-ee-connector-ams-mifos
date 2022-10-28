package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.jboss.logging.MDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;

@Component
public class CreditorExchangeWorker extends AbstractMoneyInOutWorker {

	Logger logger = LoggerFactory.getLogger(CreditorExchangeWorker.class);
	
	private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);
	
	@Override
	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		Map<String, Object> variables = activatedJob.getVariablesAsMap();
		
		String bicAndEndToEndId = (String) variables.get("bicAndEndToEndId");
		MDC.put("bicAndEndToEndId", bicAndEndToEndId);
		
		logger.info("Exchange to e-currency worker has started");
		
		String transactionDate = LocalDate.now().format(PATTERN);
		Object amount = variables.get("amount");
		
		Integer fiatCurrencyAccountAmsId = (Integer) variables.get("fiatCurrencyAccountAmsId");
		Integer eCurrencyAccountAmsId = (Integer) variables.get("eCurrencyAccountAmsId");
		
		try {
			ResponseEntity<Object> responseObject = withdraw(transactionDate, amount, fiatCurrencyAccountAmsId);
		
			if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
				return;
			}
		
			responseObject = deposit(transactionDate, amount, eCurrencyAccountAmsId);
		
			if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
				return;
			}
		
			logger.info("Exchange to e-currency worker has finished successfully");
			jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
		} catch (Exception e) {
			logger.error("Exchange to e-currency worker has failed, dispatching user task to handle exchange");
			jobClient.newThrowErrorCommand(activatedJob.getKey()).errorCode("Error_ToBeHandledManually").send();
		}
	}
}
