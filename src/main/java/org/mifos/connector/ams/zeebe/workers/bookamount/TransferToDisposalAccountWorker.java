package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.util.Map;

import org.jboss.logging.MDC;
import org.mifos.connector.ams.mapstruct.Pain001Camt052Mapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import iso.std.iso20022plus.tech.json.camt_052_001.BankToCustomerAccountReportV08;
import iso.std.iso20022plus.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;

@Component
public class TransferToDisposalAccountWorker extends AbstractMoneyInOutWorker {
	
	@Autowired
	private Pain001Camt052Mapper camt052Mapper;
	
	@Value("${fineract.paymentType.paymentTypeExchangeFiatCurrencyId}")
	private Integer paymentTypeExchangeFiatCurrencyId;
	
	@Value("${fineract.paymentType.paymentTypeIssuingECurrencyId}")
	private Integer paymentTypeIssuingECurrencyId;

	@Override
	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		try {
			Map<String, Object> variables = activatedJob.getVariablesAsMap();
			
			String originalPain001 = (String) variables.get("originalPain001");
			ObjectMapper om = new ObjectMapper();
			Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = om.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
		
			String internalCorrelationId = (String) variables.get("internalCorrelationId");
			MDC.put("internalCorrelationId", internalCorrelationId);
		
			logger.info("Exchange to e-currency worker has started");
			
			String transactionDate = (String) variables.get("interbankSettlementDate");
			Object amount = variables.get("amount");
		
			Integer conversionAccountAmsId = (Integer) variables.get("conversionAccountAmsId");
			Integer disposalAccountAmsId = (Integer) variables.get("disposalAccountAmsId");
			
			String tenantId = (String) variables.get("tenantIdentifier");
		
			ResponseEntity<Object> responseObject = withdraw(transactionDate, amount, conversionAccountAmsId, paymentTypeExchangeFiatCurrencyId, tenantId);
		
			if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
				return;
			}
		
			responseObject = deposit(transactionDate, amount, disposalAccountAmsId, paymentTypeIssuingECurrencyId, tenantId);
			
			BankToCustomerAccountReportV08 convertedCamt052 = camt052Mapper.toCamt052(pain001.getDocument());
			String camt052 = om.writeValueAsString(convertedCamt052);
			
			logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>  camt.052  <<<<<<<<<<<<<<<<<<<<<<<<");
			logger.info("The following camt.052 will be inserted into the data table: {}", camt052);
		
			if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
				return;
			}
		
			logger.info("Exchange to e-currency worker has finished successfully");
			jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
		} catch (Exception e) {
			logger.error("Exchange to e-currency worker has failed, dispatching user task to handle exchange", e);
			jobClient.newThrowErrorCommand(activatedJob.getKey()).errorCode("Error_TransferToDisposalToBeHandledManually").send();
		} finally {
			MDC.remove("internalCorrelationId");
		}
	}
}
