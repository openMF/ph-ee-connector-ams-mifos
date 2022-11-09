package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.jboss.logging.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;

@Component
public class IncomingMoneyWorker extends AbstractMoneyInOutWorker {
	
	private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);

	@Override
	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		try {
			Map<String, Object> variables = activatedJob.getVariablesAsMap();
		
			logger.info("Incoming money worker started with variables");
			variables.keySet().forEach(logger::info);
		
			String bicAndEndToEndId = (String) variables.get("bicAndEndToEndId");
			MDC.put("bicAndEndToEndId", bicAndEndToEndId);
		
			logger.info("Worker to book incoming money in AMS has started");
		
			String transactionDate = LocalDate.now().format(PATTERN);
			Object amount = variables.get("amount");
		
			Integer fiatCurrencyAccountAmsId = (Integer) variables.get("fiatCurrencyAccountAmsId");
		
			ResponseEntity<Object> responseObject = deposit(transactionDate, amount, fiatCurrencyAccountAmsId);
		
			if (HttpStatus.OK.equals(responseObject.getStatusCode())) {
				logger.info("Worker to book incoming money in AMS has finished successfully");
				jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
			} else {
				logger.error("Worker to book incoming money in AMS has failed, dispatching user task to handle fiat deposit");
				jobClient.newThrowErrorCommand(activatedJob.getKey()).errorCode("Error_ToBeHandledManually").send();
			}
		} catch (Exception e) {
			logger.error("Worker to book incoming money in AMS has failed, dispatching user task to handle fiat deposit", e);
			jobClient.newThrowErrorCommand(activatedJob.getKey()).errorCode("Error_ToBeHandledManually").send();
		} finally {
			MDC.remove("bicAndEndToEndId");
		}
	}
}
