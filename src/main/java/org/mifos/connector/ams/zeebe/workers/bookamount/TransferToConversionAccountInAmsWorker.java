package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.mifos.connector.ams.mapstruct.Pain001Camt052Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionDetails;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import iso.std.iso20022plus.tech.json.camt_052_001.BankToCustomerAccountReportV08;
import iso.std.iso20022plus.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;

@Component
public class TransferToConversionAccountInAmsWorker extends AbstractMoneyInOutWorker {
	
	@Autowired
	private Pain001Camt052Mapper camt052Mapper;
	
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
		Object amount = new Object();
		Integer disposalAccountAmsId = null;
		String tenantId = null;
		try {
			Map<String, Object> variables = activatedJob.getVariablesAsMap();
			
			String originalPain001 = (String) variables.get("originalPain001");
			ObjectMapper om = new ObjectMapper();
			Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = om.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
			
			String internalCorrelationId = (String) variables.get("internalCorrelationId");
			MDC.put("internalCorrelationId", internalCorrelationId);
			
			amount = variables.get("amount");
			
			Object fee = variables.get("transactionFeeAmount");
			
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
				logger.warn("Fee withdrawal failed");
				logger.error(e.getMessage(), e);
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).errorMessage(e.getMessage()).send();
				return;
			}
			
			logger.info("Depositing amount {} to conversion account {}", amount, conversionAccountAmsId);
		
			responseObject = deposit(transactionDate, amount, conversionAccountAmsId, paymentTypeExchangeToFiatCurrencyId, tenantId);
		
			if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
				return;
			}
			
			logger.info("Depositing fee {} to conversion account {}", fee, conversionAccountAmsId);
			
			responseObject = deposit(transactionDate, fee, conversionAccountAmsId, paymentTypeFeeId, tenantId);
			
			BankToCustomerAccountReportV08 convertedCamt052 = camt052Mapper.toCamt052(pain001.getDocument());
			String camt052 = om.writeValueAsString(convertedCamt052);
			
			logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>  camt.052  <<<<<<<<<<<<<<<<<<<<<<<<");
			logger.info("The following camt.052 will be inserted into the data table: {}", camt052);
			
			LocalDateTime now = LocalDateTime.now();
			
			TransactionDetails td = new TransactionDetails(
					8, 
					pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getPaymentIdentification().getEndToEndIdentification(),
					internalCorrelationId,
					camt052,
					now,
					now);
			
			logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>  camt.052  <<<<<<<<<<<<<<<<<<<<<<<<");
			logger.info("The following camt.052 will be inserted into the data table: {}", camt052);
			
			httpHeaders.remove("Fineract-Platform-TenantId");
			httpHeaders.add("Fineract-Platform-TenantId", tenantId);
			var entity = new HttpEntity<>(td, httpHeaders);
			
			var urlTemplate = UriComponentsBuilder.fromHttpUrl(fineractApiUrl)
					.path("datatables")
					.path("transaction_details")
					.path("8")
					.queryParam("genericResultSet", true)
					.encode()
					.toUriString();
			
			logger.info(">> Sending {} to {} with headers {}", td, urlTemplate, httpHeaders);
			
			try {
				ResponseEntity<Object> response = restTemplate.exchange(urlTemplate, HttpMethod.POST, entity, Object.class);
				logger.info("<< Received HTTP {}", response.getStatusCode());
			} catch (HttpClientErrorException e) {
				logger.error(e.getMessage(), e);
				logger.warn("Cam052 insert returned with status code {}", e.getRawStatusCode());
				jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
			}
			
			if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
				return;
			}
		
			jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
		} catch (Exception e) {
			logger.warn("Fee withdrawal failed");
			logger.error(e.getMessage(), e);
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
