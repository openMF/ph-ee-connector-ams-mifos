package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import iso.std.iso20022plus.tech.json.pain_001_001.CreditTransferTransaction40;
import iso.std.iso20022plus.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;

@Component
public class TransferToConversionAccountInAmsWorker extends AbstractMoneyInOutWorker {
	
	@Value("${fineract.paymentType.paymentTypeExchangeECurrencyId}")
	private Integer paymentTypeExchangeECurrencyId;
	
	@Value("${fineract.paymentType.paymentTypeExchangeToFiatCurrencyId}")
	private Integer paymentTypeExchangeToFiatCurrencyId;
	
	@Value("${fineract.paymentType.paymentTypeFeeId}")
	private Integer paymentTypeFeeId;
	
	private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);

	@Override
	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		logger.error("Debtor exchange worker starting");
		try {
			Map<String, Object> variables = activatedJob.getVariablesAsMap();
			
			String originalPain001 = (String) variables.get("originalPain001");
			
			ObjectMapper om = new ObjectMapper();
			Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = om.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
			
			BigDecimal amount = BigDecimal.ZERO;
			BigDecimal fee = new BigDecimal(variables.get("transactionFeeAmount").toString());
			
			List<CreditTransferTransaction40> creditTransferTransactionInformation = pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation();
			
			for (CreditTransferTransaction40 ctti : creditTransferTransactionInformation) {
				amount = new BigDecimal(ctti.getAmount().getInstructedAmount().getAmount().toString());
			}
			
			logger.error("Debtor exchange worker incoming variables:");
			variables.entrySet().forEach(e -> logger.error("{}: {}", e.getKey(), e.getValue()));
		
			String transactionDate = LocalDate.now().format(PATTERN);
			
			logger.info("Attempting to deposit the amount of {}", amount);
		
			Integer disposalAccountAmsId = (Integer) variables.get("disposalAccountAmsId");
			Integer conversionAccountAmsId = (Integer) variables.get("conversionAccountAmsId");
			
			logger.info("Attempting to withdraw from account {}", disposalAccountAmsId);
			
			ResponseEntity<Object> responseObject = withdraw(transactionDate, amount, disposalAccountAmsId, paymentTypeExchangeECurrencyId);
				
			if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
				return;
			}
				
			responseObject = withdraw(transactionDate, fee, disposalAccountAmsId, paymentTypeFeeId);
				
			if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
				ResponseEntity<Object> sagaResponseObject = deposit(transactionDate, amount, disposalAccountAmsId, paymentTypeExchangeECurrencyId);
					
				if (!HttpStatus.OK.equals(sagaResponseObject.getStatusCode())) {
					jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
					return;
				}
				return;
			}
		
			if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
				return;
			}
			
			logger.info("Attempting to deposit to account {}", conversionAccountAmsId);
		
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
