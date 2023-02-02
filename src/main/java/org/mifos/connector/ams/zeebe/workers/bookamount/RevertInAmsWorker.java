package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import hu.dpc.rt.utils.mapstruct.Pain001Camt052Mapper;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import iso.std.iso20022plus.tech.json.camt_052_001.BankToCustomerAccountReportV08;
import iso.std.iso20022plus.tech.json.pain_001_001.CreditTransferTransaction40;
import iso.std.iso20022plus.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;

@Component
public class RevertInAmsWorker extends AbstractMoneyInOutWorker {

	@Value("${fineract.paymentType.paymentTypeExchangeFiatCurrencyId}")
	private Integer paymentTypeExchangeFiatCurrencyId;
	
	@Value("${fineract.paymentType.paymentTypeIssuingECurrencyId}")
	private Integer paymentTypeIssuingECurrencyId;
	
	@Autowired
	private Pain001Camt052Mapper camt052Mapper;
	
	private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);

	@Override
	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		Map<String, Object> variables = activatedJob.getVariablesAsMap();
		
		String internalCorrelationId = (String) variables.get("internalCorrelationId");
		MDC.put("internalCorrelationId", internalCorrelationId);
		
		String originalPain001 = (String) variables.get("originalPain001");
		
		Integer conversionAccountAmsId = (Integer) variables.get("conversionAccountAmsId");
		
		String interbankSettlementDate = LocalDate.now().format(PATTERN);
			
		Integer disposalAccountAmsId = (Integer) variables.get("disposalAccountAmsId");
		
		ObjectMapper om = new ObjectMapper();
		Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = om.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
		
		BigDecimal amount = BigDecimal.ZERO;
		BigDecimal fee = new BigDecimal(variables.get("transactionFeeAmount").toString());
		
		List<CreditTransferTransaction40> creditTransferTransactionInformation = pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation();
		
		for (CreditTransferTransaction40 ctti : creditTransferTransactionInformation) {
			amount = new BigDecimal(ctti.getAmount().getInstructedAmount().getAmount().toString());
		}
		
		logger.error("Withdrawing amount {} from conversion account {}", amount, conversionAccountAmsId);
		
		String tenantId = (String) variables.get("tenantIdentifier");
	
		ResponseEntity<Object> responseObject = withdraw(interbankSettlementDate, amount, conversionAccountAmsId, 1, tenantId);
			
		if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
			return;
		}
		
		logger.error("Withdrawing fee {} from conversion account {}", fee, conversionAccountAmsId);
			
		responseObject = withdraw(interbankSettlementDate, fee, conversionAccountAmsId, 1, tenantId);
			
		if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
			ResponseEntity<Object> sagaResponseObject = deposit(interbankSettlementDate, amount, conversionAccountAmsId, 1, tenantId);
			if (!HttpStatus.OK.equals(sagaResponseObject.getStatusCode())) {
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
			}
			return;
		}
		
		logger.error("Re-depositing amount {} in disposal account {}", amount, disposalAccountAmsId);
		
		responseObject = deposit(interbankSettlementDate, amount, disposalAccountAmsId, 1, tenantId);
		
		if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
			return;
		}
		
		logger.error("Re-depositing fee {} in disposal account {}", fee, disposalAccountAmsId);
			
		responseObject = deposit(interbankSettlementDate, fee, disposalAccountAmsId, 1, tenantId);
		
		BankToCustomerAccountReportV08 convertedCamt052 = camt052Mapper.toCamt052(pain001.getDocument());
		String camt052 = om.writeValueAsString(convertedCamt052);
		
		logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>  camt.052  <<<<<<<<<<<<<<<<<<<<<<<<");
		logger.info("The following camt.052 will be inserted into the data table: {}", camt052);
			
		if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
			ResponseEntity<Object> sagaResponseObject = withdraw(interbankSettlementDate, amount, disposalAccountAmsId, 1, tenantId);
			if (!HttpStatus.OK.equals(sagaResponseObject.getStatusCode())) {
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
			}
			return;
		}
		
		jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
		MDC.remove("internalCorrelationId");
	}

}
