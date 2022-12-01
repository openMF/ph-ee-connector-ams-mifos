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
public class TransferToConversionAccountWorker extends AbstractMoneyInOutWorker {
	
	@Value("${fineract.paymentType.paymentTypeExchangeECurrencyId}")
	private Integer paymentTypeExchangeECurrencyId;
	
	@Value("${fineract.paymentType.paymentTypeExchangeToFiatCurrencyId}")
	private Integer paymentTypeExchangeToFiatCurrencyId;
	
	@Value("${fineract.generalLedger.glLiabilityToCustomersInFiatCurrencyId}")
	private Integer glLiabilityToCustomersInFiatCurrencyId;
	
	@Value("${fineract.generalLedger.glLiabilityAmountOnHoldId}")
	private Integer glLiabilityAmountOnHoldId;
	
	private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);

	@Override
	@SuppressWarnings("unchecked")
	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		logger.error("Debtor exchange worker starting");
		try {
			Map<String, Object> variables = activatedJob.getVariablesAsMap();
			
			logger.error("Debtor exchange worker incoming variables:");
			variables.entrySet().forEach(e -> logger.error("{}: {}", e.getKey(), e.getValue()));
		
			String transactionDate = LocalDate.now().format(PATTERN);
			Object amount = variables.get("amount");
		
			Integer disposalAccountAmsId = (Integer) variables.get("disposalAccountAmsId");
			Integer conversionAccountAmsId = (Integer) variables.get("conversionAccountAmsId");
		
			ResponseEntity<Object> responseObject = withdraw(transactionDate, amount, disposalAccountAmsId, paymentTypeExchangeECurrencyId);
		
			if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
				return;
			}
		
			responseObject = deposit(transactionDate, amount, conversionAccountAmsId, paymentTypeExchangeToFiatCurrencyId);
		
			if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
				return;
			}
		
			jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			jobClient.newThrowErrorCommand(activatedJob.getKey()).errorCode("Error_InsufficientFunds").send();
		}
	}

}