package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;

@Component
public class RevertDebitOnFiatAccountWorker extends AbstractMoneyInOutWorker {
	
	@Value("${fineract.paymentType.paymentTypeExchangeFiatCurrencyId}")
	private Integer paymentTypeExchangeFiatCurrencyId;
	
	@Value("${fineract.paymentType.paymentTypeIssuingECurrencyId}")
	private Integer paymentTypeIssuingECurrencyId;
	
	private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);

	@Override
	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		Map<String, Object> variables = activatedJob.getVariablesAsMap();
		
		Integer fiatCurrencyAccountAmsId = (Integer) variables.get("fiatCurrencyAccountAmsId");
		Integer holdAmountId = (Integer) variables.get("holdAmountId");
		
		
		ResponseEntity<Object> responseObject = release(fiatCurrencyAccountAmsId, holdAmountId);
		
		if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
			logger.error("Debtor exchange and hold worker fails with status code {}", responseObject.getStatusCodeValue());
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
			return;
		}
		
		Object amount = variables.get("amount");
		responseObject = withdraw(LocalDate.now().format(PATTERN), amount, fiatCurrencyAccountAmsId, paymentTypeExchangeFiatCurrencyId);
		
		if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
			logger.error("Debtor exchange and hold worker fails with status code {}", responseObject.getStatusCodeValue());
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
			return;
		}
		
		Integer eCurrencyAccountAmsId = (Integer) variables.get("eCurrencyAccountAmsId");
		responseObject = deposit(LocalDate.now().format(PATTERN), amount, eCurrencyAccountAmsId, paymentTypeIssuingECurrencyId);
		
		if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
			logger.error("Debtor exchange and hold worker fails with status code {}", responseObject.getStatusCodeValue());
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
			return;
		}
		
		
		jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
	}

}
