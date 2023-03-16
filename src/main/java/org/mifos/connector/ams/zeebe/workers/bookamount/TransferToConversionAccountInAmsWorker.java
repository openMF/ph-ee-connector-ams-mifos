package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.io.InputStream;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Set;

import org.mifos.connector.ams.mapstruct.Pain001Camt052Mapper;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import iso.std.iso._20022.tech.json.camt_052_001.BankToCustomerAccountReportV08;
import iso.std.iso._20022.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;

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
			
			
			try {
				logger.info(">>>>>>>>>>>>>>>>>> Validating incoming pain.001 <<<<<<<<<<<<<<<<");
				InputStream resource = new ClassPathResource("/services/json-schema/pain.001.001/pain.001.001.10-CustomerCreditTransferInitiationV10.Message.schema.json").getInputStream();
				JsonSchemaFactory sf = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
			
				JsonNode json = om.readTree(originalPain001);
				JsonSchema schema = sf.getSchema(resource);
				Set<ValidationMessage> validationResult = schema.validate(json);
			
				if (validationResult.isEmpty()) {
					logger.info(">>>>>>>>>>>>>>>> pain.001 validation successful <<<<<<<<<<<<<<<");
				} else {
					logger.error(validationResult.toString());
				}
			} catch (Exception e) {
				logger.warn("Unable to validate pain.001: {}", e.getMessage());
			}
			
			
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
			
			ResponseEntity<Object> withdrawAmountResponseObject = withdraw(transactionDate, amount, disposalAccountAmsId, paymentTypeExchangeECurrencyId, tenantId, internalCorrelationId);
			
			if (!HttpStatus.OK.equals(withdrawAmountResponseObject.getStatusCode())) {
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
				return;
			}
			
			BankToCustomerAccountReportV08 convertedCamt052 = camt052Mapper.toCamt052(pain001.getDocument());
			String camt052 = om.writeValueAsString(convertedCamt052);

			postCamt052(tenantId, camt052, internalCorrelationId, withdrawAmountResponseObject);
			
			if (fee != null && ((fee instanceof Integer i && i > 0) || (fee instanceof BigDecimal bd && !bd.equals(BigDecimal.ZERO)))) {
				logger.info("Withdrawing fee {} from disposal account {}", fee, disposalAccountAmsId);
				
				try {
					ResponseEntity<Object> withdrawFeeResponseObject = withdraw(transactionDate, fee, disposalAccountAmsId, paymentTypeFeeId, tenantId, internalCorrelationId);
					postCamt052(tenantId, camt052, internalCorrelationId, withdrawFeeResponseObject);
				} catch (Exception e) {
					logger.warn("Fee withdrawal failed");
					logger.error(e.getMessage(), e);
					jobClient.newFailCommand(activatedJob.getKey()).retries(0).errorMessage(e.getMessage()).send();
					return;
				}
			}
			
			logger.info("Depositing amount {} to conversion account {}", amount, conversionAccountAmsId);
		
			ResponseEntity<Object> depositAmountResponseObject = deposit(transactionDate, amount, conversionAccountAmsId, paymentTypeExchangeToFiatCurrencyId, tenantId, internalCorrelationId);
		
			if (!HttpStatus.OK.equals(depositAmountResponseObject.getStatusCode())) {
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
				return;
			}
			
			postCamt052(tenantId, camt052, internalCorrelationId, depositAmountResponseObject);
			
			logger.info("Depositing fee {} to conversion account {}", fee, conversionAccountAmsId);
			
			ResponseEntity<Object> depositFeeResponseObject = deposit(transactionDate, fee, conversionAccountAmsId, paymentTypeFeeId, tenantId, internalCorrelationId);
			
			postCamt052(tenantId, camt052, internalCorrelationId, depositFeeResponseObject);
			
			if (!HttpStatus.OK.equals(depositFeeResponseObject.getStatusCode())) {
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
				return;
			}
		
			jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
		} catch (Exception e) {
			logger.warn("Fee to conversion account failed");
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
