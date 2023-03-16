package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
import java.util.Map;

import org.mifos.connector.ams.mapstruct.Pain001Camt052Mapper;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import iso.std.iso._20022.tech.json.camt_052_001.BankToCustomerAccountReportV08;
import iso.std.iso._20022.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;

@Component
public class BookOnConversionAccountInAmsWorker extends AbstractMoneyInOutWorker {
	
	@Autowired
	private Pain001Camt052Mapper camt052Mapper;
	
	@Override
	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		Map<String, Object> variables = activatedJob.getVariablesAsMap();
		
		String originalPain001 = (String) variables.get("originalPain001");
		ObjectMapper om = new ObjectMapper();
		Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = om.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
		
		String internalCorrelationId = (String) variables.get("internalCorrelationId");
		MDC.put("internalCorrelationId", internalCorrelationId);
		
		String paymentScheme = (String) variables.get("paymentScheme");
		
		String transactionDate = (String) variables.get("transactionDate");
		
		Integer conversionAccountAmsId = (Integer) variables.get("conversionAccountAmsId");
		
		logger.info("Starting book debit on fiat account worker");
		
		Object amount = variables.get("amount");
		Object fee = variables.get("transactionFeeAmount");
		
		String tenantId = (String) variables.get("tenantIdentifier");
		logger.info("Withdrawing amount {} from conversion account {} of tenant {}", amount, conversionAccountAmsId, tenantId);
	
		ResponseEntity<Object> responseObject = withdraw(
				transactionDate, 
				amount, 
				conversionAccountAmsId, 
				paymentScheme,
				"MoneyOutAmountConversionWithdraw",
				tenantId, 
				internalCorrelationId);
		
		BankToCustomerAccountReportV08 convertedCamt052 = camt052Mapper.toCamt052(pain001.getDocument());
		String camt052 = om.writeValueAsString(convertedCamt052);
		
		postCamt052(tenantId, camt052, internalCorrelationId, responseObject);
		
		if (fee != null && ((fee instanceof Integer i && i > 0) || (fee instanceof BigDecimal bd && !bd.equals(BigDecimal.ZERO)))) {
				
			logger.info("Withdrawing fee {} from conversion account {}", fee, conversionAccountAmsId);
				
			responseObject = withdraw(
					transactionDate, 
					fee, 
					conversionAccountAmsId, 
					paymentScheme,
					"MoneyOutFeeConversionWithdraw",
					tenantId, 
					internalCorrelationId);
			postCamt052(tenantId, camt052, internalCorrelationId, responseObject);

			if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
				return;
			}
		}
		
		jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
			
		logger.info("Book debit on fiat account has finished  successfully");
		
		MDC.remove("internalCorrelationId");
	}
}
