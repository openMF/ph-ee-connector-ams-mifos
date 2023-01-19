package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.datatype.XMLGregorianCalendar;

import org.jboss.logging.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import eu.nets.realtime247.ri_2015_10.ObjectFactory;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import iso.std.iso._20022.tech.xsd.pacs_002_001.Document;

@Component
public class BookCreditedAmountToConversionAccountWorker extends AbstractMoneyInOutWorker {
	
	private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);

	@Override
	@SuppressWarnings("unchecked")
	public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
		try {
			Map<String, Object> variables = activatedJob.getVariablesAsMap();
		
			logger.info("Incoming money worker started with variables");
			variables.keySet().forEach(logger::info);
		
			String internalCorrelationId = (String) variables.get("internalCorrelationId");
			MDC.put("internalCorrelationId", internalCorrelationId);
		
			logger.info("Worker to book incoming money in AMS has started");
			
			String originalPacs002 = (String) variables.get("originalPacs002");
			JAXBContext jc = JAXBContext.newInstance(ObjectFactory.class);
			JAXBElement<Document> jaxbObject = (JAXBElement<Document>) jc.createUnmarshaller().unmarshal(new StringReader(originalPacs002));
			Document pacs_002 = jaxbObject.getValue();
			
			XMLGregorianCalendar interbankSettlementDate = pacs_002.getFIToFIPmtStsRpt().getTxInfAndSts().get(0).getOrgnlTxRef().getIntrBkSttlmDt();
			String transactionDate = interbankSettlementDate.toGregorianCalendar().toZonedDateTime().toLocalDate().format(PATTERN);
			
//			String transactionDate = Optional.ofNullable(interbankSettlementDate)
//					.map(Object::toString)
//					.orElse(LocalDateTime.now().format(PATTERN));
			Object amount = variables.get("amount");
		
			Integer conversionAccountAmsId = (Integer) variables.get("conversionAccountAmsId");
		
			ResponseEntity<Object> responseObject = deposit(transactionDate, amount, conversionAccountAmsId, 1);
		
			if (HttpStatus.OK.equals(responseObject.getStatusCode())) {
				logger.info("Worker to book incoming money in AMS has finished successfully");
				jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
			} else {
				logger.error("Worker to book incoming money in AMS has failed, dispatching user task to handle fiat deposit");
				jobClient.newThrowErrorCommand(activatedJob.getKey()).errorCode("Error_BookToConversionToBeHandledManually").send();
			}
		} catch (Exception e) {
			logger.error("Worker to book incoming money in AMS has failed, dispatching user task to handle fiat deposit", e);
			jobClient.newThrowErrorCommand(activatedJob.getKey()).errorCode("Error_BookToConversionToBeHandledManually").send();
		} finally {
			MDC.remove("internalCorrelationId");
		}
	}
}
