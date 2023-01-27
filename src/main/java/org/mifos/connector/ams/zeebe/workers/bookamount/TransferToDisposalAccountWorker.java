package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.util.Map;

import org.jboss.logging.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;

@Component
public class TransferToDisposalAccountWorker extends AbstractMoneyInOutWorker {
	
	@Value("${fineract.paymentType.paymentTypeExchangeFiatCurrencyId}")
	private Integer paymentTypeExchangeFiatCurrencyId;
	
	@Value("${fineract.paymentType.paymentTypeIssuingECurrencyId}")
	private Integer paymentTypeIssuingECurrencyId;

	@Override
	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		try {
			Map<String, Object> variables = activatedJob.getVariablesAsMap();
		
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
