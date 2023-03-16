package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.io.StringReader;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;

import org.jboss.logging.MDC;
import org.mifos.connector.ams.mapstruct.Pacs008Camt052Mapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.nets.realtime247.ri_2015_10.ObjectFactory;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import iso.std.iso._20022.tech.json.camt_052_001.BankToCustomerAccountReportV08;

@Component
public class BookCreditedAmountToConversionAccountWorker extends AbstractMoneyInOutWorker {

    @Autowired
    private Pacs008Camt052Mapper camt052Mapper;
    
    @Override
    @SuppressWarnings("unchecked")
    public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
        try {
            Map<String, Object> variables = activatedJob.getVariablesAsMap();

            logger.info("Incoming money worker started with variables");
            variables.keySet().forEach(logger::info);

            String originalPacs008 = (String) variables.get("originalPacs008");
            JAXBContext jc = JAXBContext.newInstance(ObjectFactory.class,
                    iso.std.iso._20022.tech.xsd.pacs_008_001.ObjectFactory.class,
                    iso.std.iso._20022.tech.xsd.pacs_002_001.ObjectFactory.class);
            JAXBElement<iso.std.iso._20022.tech.xsd.pacs_008_001.Document> object = (JAXBElement<iso.std.iso._20022.tech.xsd.pacs_008_001.Document>) jc.createUnmarshaller().unmarshal(new StringReader(originalPacs008));
            iso.std.iso._20022.tech.xsd.pacs_008_001.Document pacs008 = object.getValue();
            String transactionDate = (String) variables.get("transactionDate");

            String internalCorrelationId = (String) variables.get("internalCorrelationId");
            MDC.put("internalCorrelationId", internalCorrelationId);
            String tenantId = (String) variables.get("tenantIdentifier");
            String paymentScheme = (String) variables.get("paymentScheme");

            Object amount = variables.get("amount");

            Integer conversionAccountAmsId = (Integer) variables.get("conversionAccountAmsId");

            ResponseEntity<Object> responseObject = deposit(
            		transactionDate, 
            		amount, 
            		conversionAccountAmsId, 
            		paymentScheme,
            		"MoneyInAmountConversionDeposit",
            		tenantId, 
            		internalCorrelationId);
            
            
            if (HttpStatus.OK.equals(responseObject.getStatusCode())) {
                logger.info("Worker to book incoming money in AMS has finished successfully");
                
                ObjectMapper om = new ObjectMapper();
                BankToCustomerAccountReportV08 convertedCamt052 = camt052Mapper.toCamt052(pacs008);
                String camt052 = om.writeValueAsString(convertedCamt052);
                
                postCamt052(tenantId, camt052, internalCorrelationId, responseObject);
                
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