package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.io.StringReader;
import java.math.BigDecimal;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;

import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.nets.realtime247.ri_2015_10.ObjectFactory;
import hu.dpc.rt.utils.mapstruct.Pain001Camt052Mapper;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import iso.std.iso._20022.tech.xsd.pacs_002_001.Document;
import iso.std.iso20022plus.tech.json.camt_052_001.BankToCustomerAccountReportV08;
import iso.std.iso20022plus.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;

@Component
public class BookOnConversionAccountInAmsWorker extends AbstractMoneyInOutWorker {
	
	@Autowired
	private Pain001Camt052Mapper camt052Mapper;
	
	private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);
	
	@Override
	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		Map<String, Object> variables = activatedJob.getVariablesAsMap();
		
		String originalPain001 = (String) variables.get("originalPain001");
		ObjectMapper om = new ObjectMapper();
		Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = om.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
		
		String internalCorrelationId = (String) variables.get("internalCorrelationId");
		MDC.put("internalCorrelationId", internalCorrelationId);
		
		Integer conversionAccountAmsId = (Integer) variables.get("conversionAccountAmsId");
		
		String originalPacs002 = (String) variables.get("originalPacs002");

		logger.info("Starting book debit on fiat account worker with currency account Id {} with incoming pacs.002 {}", conversionAccountAmsId, originalPacs002);
		
		JAXBContext jc = JAXBContext.newInstance(ObjectFactory.class);
		JAXBElement<Document> jaxbObject = (JAXBElement<Document>) jc.createUnmarshaller().unmarshal(new StringReader(originalPacs002));
		Document pacs_002 = jaxbObject.getValue();
		
		String interbankSettlementDate = pacs_002.getFIToFIPmtStsRpt().getTxInfAndSts().get(0).getOrgnlTxRef().getIntrBkSttlmDt().toGregorianCalendar().toZonedDateTime().toLocalDate().format(PATTERN);
		
		if (interbankSettlementDate == null) {
			logger.error("Book debit on fiat account has failed due to Interbank settlement date not being present in pacs.002");
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send().join();
			return;
		}
		
		BigDecimal amount = new BigDecimal((String) variables.get("amount"));
		BigDecimal fee = new BigDecimal(variables.get("transactionFeeAmount").toString());
		
		String tenantId = (String) variables.get("tenantIdentifier");
		logger.info("Withdrawing amount {} from conversion account {} of tenant {}", amount, conversionAccountAmsId, tenantId);
	
		ResponseEntity<Object> responseObject = withdraw(interbankSettlementDate, amount, conversionAccountAmsId, 1, tenantId);
			
		if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
			return;
		}
		
		logger.info("Withdrawing fee {} from conversion account {}", fee, conversionAccountAmsId);
			
		responseObject = withdraw(interbankSettlementDate, fee, conversionAccountAmsId, 1, tenantId);
		
		BankToCustomerAccountReportV08 convertedCamt052 = camt052Mapper.toCamt052(pain001.getDocument());
		String camt052 = om.writeValueAsString(convertedCamt052);
		
		logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>  camt.052  <<<<<<<<<<<<<<<<<<<<<<<<");
		logger.info("The following camt.052 will be inserted into the data table: {}", camt052);
		
			
		if (!HttpStatus.OK.equals(responseObject.getStatusCode())) {
			jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
			ResponseEntity<Object> sagaResponseObject = deposit(interbankSettlementDate, amount, conversionAccountAmsId, 1, tenantId);
			if (!HttpStatus.OK.equals(sagaResponseObject.getStatusCode())) {
				jobClient.newFailCommand(activatedJob.getKey()).retries(0).send();
			}
			return;
		}
		
		jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
			
		logger.info("Book debit on fiat account has finished  successfully");
		
		MDC.remove("internalCorrelationId");
	}
}
