package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;

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
		String transactionDate = LocalDate.now().format(PATTERN);
		logger.error("Debtor exchange worker starting");
		BigDecimal amount = BigDecimal.ZERO;
		Integer disposalAccountAmsId = null;
		String tenantId = null;
		try {
			Map<String, Object> variables = activatedJob.getVariablesAsMap();
			
			String internalCorrelationId = (String) variables.get("internalCorrelationId");
			MDC.put("internalCorrelationId", internalCorrelationId);
			
			amount = new BigDecimal((String) variables.get("amount"));
			
			BigDecimal fee = new BigDecimal(variables.get("transactionFeeAmount").toString());
			
			logger.error("Debtor exchange worker incoming variables:");
			variables.entrySet().forEach(e -> logger.error("{}: {}", e.getKey(), e.getValue()));
		
			
			disposalAccountAmsId = (Integer) variables.get("disposalAccountAmsId");
			Integer conversionAccountAmsId = (Integer) variables.get("conversionAccountAmsId");
			
			logger.info("Withdrawing amount {} from disposal account {}", amount, disposalAccountAmsId);
			
			tenantId = (String) variables.get("tenantIdentifier");
			
			ResponseEntity<Object> responseObject = withdraw(transactionDate, amount, disposalAccountAmsId, paymentTypeExchangeECurrencyId, tenantId);
				
			if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
				return;
			}
			
			logger.info("Withdrawing fee {} from disposal account {}", fee, disposalAccountAmsId);
			
			try {
				withdraw(transactionDate, fee, disposalAccountAmsId, paymentTypeFeeId, tenantId);
			} catch (Exception e) {
				logger.warn("Fee withdrawal failed, re-depositing {} to disposal account", amount);
				deposit(transactionDate, amount, disposalAccountAmsId, paymentTypeExchangeECurrencyId, tenantId);
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).errorMessage(e.getMessage()).send();
				return;
			}
			
			logger.info("Depositing amount {} to conversion account {}", amount, conversionAccountAmsId);
		
			responseObject = deposit(transactionDate, amount, conversionAccountAmsId, paymentTypeExchangeToFiatCurrencyId, tenantId);
		
			if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
				deposit(transactionDate, amount, disposalAccountAmsId, paymentTypeExchangeToFiatCurrencyId, tenantId);
				deposit(transactionDate, fee, disposalAccountAmsId, paymentTypeExchangeToFiatCurrencyId, tenantId);
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
				return;
			}
			
			logger.info("Depositing fee {} to conversion account {}", fee, conversionAccountAmsId);
			
			responseObject = deposit(transactionDate, fee, conversionAccountAmsId, paymentTypeFeeId, tenantId);
			
			if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
				deposit(transactionDate, amount, disposalAccountAmsId, paymentTypeExchangeToFiatCurrencyId, tenantId);
				deposit(transactionDate, fee, disposalAccountAmsId, paymentTypeExchangeToFiatCurrencyId, tenantId);
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
				return;
			}
		
			jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
		} catch (Exception e) {
			logger.warn("Fee withdrawal failed, re-depositing {} to disposal account", amount);
			deposit(transactionDate, amount, disposalAccountAmsId, paymentTypeExchangeECurrencyId, tenantId);
			jobClient
					.newThrowErrorCommand(activatedJob.getKey())
					.errorCode("Error_InsufficientFunds")
					.errorMessage(e.getMessage())
					.send();
		} finally {
			MDC.remove("internalCorrelationId");
		}
	}

}
