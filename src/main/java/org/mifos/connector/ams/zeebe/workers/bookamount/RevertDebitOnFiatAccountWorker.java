package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;

@Component
public class RevertDebitOnFiatAccountWorker extends AbstractMoneyInOutWorker {
	
	private static final String ERROR_MESSAGE_PATTERN = "Revert debit worker fails with status code {}";

	@Value("${fineract.paymentType.paymentTypeExchangeFiatCurrencyId}")
	private Integer paymentTypeExchangeFiatCurrencyId;
	
	@Value("${fineract.paymentType.paymentTypeIssuingECurrencyId}")
	private Integer paymentTypeIssuingECurrencyId;
	
	@Value("${fineract.generalLedger.glLiabilityAmountOnHoldId}")
	private Integer glLiabilityAmountOnHoldId;
	
	@Value("${fineract.generalLedger.glLiabilityToCustomersInFiatCurrencyId}")
	private Integer glLiabilityToCustomersInFiatCurrencyId;
	
	private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);

	@Override
	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		Map<String, Object> variables = activatedJob.getVariablesAsMap();
		
		Integer conversionAccountAmsId = (Integer) variables.get("conversionAccountAmsId");
		
		Object amount = variables.get("amount");
		
		String interbankSettlementDate = (String) variables.get("interbankSettlementDate");
		String transactionDate = Optional.ofNullable(interbankSettlementDate)
				.orElse(LocalDateTime.now().format(PATTERN));
			
		ResponseEntity<Object> responseObject = withdraw(transactionDate, amount, conversionAccountAmsId, paymentTypeExchangeFiatCurrencyId);
		
		if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
			logger.error(ERROR_MESSAGE_PATTERN, responseObject.getStatusCodeValue());
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
			return;
		}
		
		Integer disposalAccountAmsId = (Integer) variables.get("disposalAccountAmsId");
		responseObject = deposit(transactionDate, amount, disposalAccountAmsId, paymentTypeIssuingECurrencyId);
		
		if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
			logger.error(ERROR_MESSAGE_PATTERN, responseObject.getStatusCodeValue());
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
			return;
		}
		
		
		jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
	}

}
