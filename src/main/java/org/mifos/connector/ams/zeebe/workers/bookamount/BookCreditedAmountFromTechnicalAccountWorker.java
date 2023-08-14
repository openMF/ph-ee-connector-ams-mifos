package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.mifos.connector.ams.fineract.Config;
import org.mifos.connector.ams.fineract.ConfigFactory;
import org.mifos.connector.ams.mapstruct.Pacs008Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.BatchItemBuilder;
import org.mifos.connector.ams.zeebe.workers.utils.JAXBUtils;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionBody;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionDetails;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.baasflow.commons.events.EventService;
import com.baasflow.commons.events.EventStatus;
import com.baasflow.commons.events.EventType;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import iso.std.iso._20022.tech.json.camt_053_001.ReportEntry10;
import jakarta.xml.bind.JAXBException;

@Component
public class BookCreditedAmountFromTechnicalAccountWorker extends AbstractMoneyInOutWorker {

	private Logger logger = LoggerFactory.getLogger(getClass());
	
	@Value("${fineract.incoming-money-api}")
	private String incomingMoneyApi;
	
	@Value("${fineract.locale}")
	private String locale;
	
	@Autowired
	private BatchItemBuilder batchItemBuilder;
	
	@Autowired
    private ConfigFactory paymentTypeConfigFactory;
	
	@Autowired
	private ConfigFactory technicalAccountConfigFactory;
	
	@Autowired
	private JAXBUtils jaxbUtils;
	
	@Autowired
    private Pacs008Camt053Mapper camt053Mapper;
	
	@Autowired
	private EventService eventService;
	
	private static final String FORMAT = "yyyyMMdd";

	@JobWorker
	public void bookCreditedAmountFromTechnicalAccount(JobClient jobClient, 
    		ActivatedJob activatedJob,
    		@Variable String originalPacs008,
			@Variable String amount,
			@Variable String tenantIdentifier,
			@Variable String paymentScheme,
			@Variable String transactionDate,
			@Variable String currency,
			@Variable String internalCorrelationId,
			@Variable String transactionGroupId,
			@Variable String transactionCategoryPurposeCode,
			@Variable String caseIdentifier
			) {
		
		try {
            ObjectMapper objectMapper = new ObjectMapper();
            
            objectMapper.setSerializationInclusion(Include.NON_NULL);
            
            List<TransactionItem> items = new ArrayList<>();

            iso.std.iso._20022.tech.xsd.pacs_008_001.Document pacs008 = jaxbUtils.unmarshalPacs008(originalPacs008);

            MDC.put("internalCorrelationId", internalCorrelationId);

            batchItemBuilder.tenantId(tenantIdentifier);
            
            Config technicalAccountConfig = technicalAccountConfigFactory.getConfig(tenantIdentifier);
            
            String taLookup = String.format("%s.%s", paymentScheme, caseIdentifier);
            logger.debug("Looking up account id for {}", taLookup);
			Integer recallTechnicalAccountId = technicalAccountConfig.findByOperation(taLookup);
    		
    		String technicalAccountWithdrawalRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), recallTechnicalAccountId, "withdrawal");
    		
    		Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
    		Integer paymentTypeId = paymentTypeConfig.findByOperation(String.format("%s.%s.%s", paymentScheme, "bookFromTechnicalAccount", caseIdentifier));
    		
    		TransactionBody body = new TransactionBody(
    				transactionDate,
    				amount,
    				paymentTypeId,
    				"",
    				FORMAT,
    				locale);
    		
    		String bodyItem = objectMapper.writeValueAsString(body);
    		
    		batchItemBuilder.add(items, technicalAccountWithdrawalRelativeUrl, bodyItem, false);
    	
    		ReportEntry10 convertedCamt053Entry = camt053Mapper.toCamt053Entry(pacs008);
    		String camt053Entry = objectMapper.writeValueAsString(convertedCamt053Entry);
    		
    		String camt053RelativeUrl = "datatables/transaction_details/$.resourceId";
    		
    		TransactionDetails td = new TransactionDetails(
    				internalCorrelationId,
    				camt053Entry,
    				null,
    				transactionDate,
    				FORMAT,
    				locale,
    				transactionGroupId,
    				transactionCategoryPurposeCode);
    		
    		String camt053Body = objectMapper.writeValueAsString(td);

    		batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);

            doBatch(items, tenantIdentifier, internalCorrelationId);
            
            eventService.sendEvent(
    				"ams_connector", 
    				"bookCreditedAmountFromTechnicalAccount has finished", 
    				EventType.audit, 
    				EventStatus.success, 
    				null,
    				null,
    				Map.of(
    						"processInstanceKey", "" + activatedJob.getProcessInstanceKey(),
    						"internalCorrelationId", internalCorrelationId,
    						"transactionGroupId", transactionGroupId
    						));
        } catch (JsonProcessingException | JAXBException e) {
            logger.error("Worker to book incoming money in AMS has failed, dispatching user task to handle conversion account deposit", e);
            
            eventService.sendEvent(
    				"ams_connector", 
    				"bookCreditedAmountFromTechnicalAccount has finished", 
    				EventType.audit, 
    				EventStatus.failure, 
    				null,
    				null,
    				Map.of(
    						"processInstanceKey", "" + activatedJob.getProcessInstanceKey(),
    						"internalCorrelationId", internalCorrelationId,
    						"transactionGroupId", transactionGroupId
    						));
            throw new ZeebeBpmnError("Error_BookToConversionToBeHandledManually", e.getMessage());
        } finally {
            MDC.remove("internalCorrelationId");
        }
	}
}
