package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.io.StringReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

//import javax.xml.bind.JAXBContext;
//import javax.xml.bind.JAXBElement;

import org.jboss.logging.MDC;
import org.mifos.connector.ams.fineract.PaymentTypeConfig;
import org.mifos.connector.ams.fineract.PaymentTypeConfigFactory;
//import org.mifos.connector.ams.mapstruct.Pacs008Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.BatchItemBuilder;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionBody;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionDetails;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

//import eu.nets.realtime247.ri_2015_10.ObjectFactory;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
//import iso.std.iso._20022.tech.json.camt_053_001.BankToCustomerStatementV08;

@Component
public class BookCreditedAmountToConversionAccountWorker extends AbstractMoneyInOutWorker {

    @Autowired
//    private Pacs008Camt053Mapper camt053Mapper;
    
    @Value("${fineract.incoming-money-api}")
	protected String incomingMoneyApi;
	
	@Value("${fineract.auth-token}")
	private String authToken;
	
	@Autowired
    private PaymentTypeConfigFactory paymentTypeConfigFactory;
    
	@JobWorker
    @SuppressWarnings("unchecked")
    public void bookCreditedAmountToConversionAccount(JobClient jobClient, 
    		ActivatedJob activatedJob,
    		@Variable String originalPacs008,
    		@Variable String transactionDate,
    		@Variable String transactionCategoryPurposeCode,
    		@Variable String transactionGroupId,
    		@Variable String internalCorrelationId,
    		@Variable String tenantIdentifier,
    		@Variable String paymentScheme,
    		@Variable BigDecimal amount,
    		@Variable Integer conversionAccountAmsId) throws Exception {
        try {
            logger.info("Incoming money worker started with variables");

//            JAXBContext jc = JAXBContext.newInstance(ObjectFactory.class,
//                    iso.std.iso._20022.tech.xsd.pacs_008_001.ObjectFactory.class,
//                    iso.std.iso._20022.tech.xsd.pacs_002_001.ObjectFactory.class);
//            JAXBElement<iso.std.iso._20022.tech.xsd.pacs_008_001.Document> object = (JAXBElement<iso.std.iso._20022.tech.xsd.pacs_008_001.Document>) jc.createUnmarshaller().unmarshal(new StringReader(originalPacs008));
//            iso.std.iso._20022.tech.xsd.pacs_008_001.Document pacs008 = object.getValue();
//
            MDC.put("internalCorrelationId", internalCorrelationId);

            BatchItemBuilder biBuilder = new BatchItemBuilder(tenantIdentifier);
    		
    		String conversionAccountWithdrawalRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "deposit");
    		
    		PaymentTypeConfig paymentTypeConfig = paymentTypeConfigFactory.getPaymentTypeConfig(tenantIdentifier);
    		Integer paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "bookCreditedAmountToConversionAccount.ConversionAccount.DepositTransactionAmount"));
    		
    		TransactionBody body = new TransactionBody(
    				transactionDate,
    				amount,
    				paymentTypeId,
    				"",
    				FORMAT,
    				locale);
    		
    		ObjectMapper om = new ObjectMapper();
    		
    		String bodyItem = om.writeValueAsString(body);
    		
    		List<TransactionItem> items = new ArrayList<>();
    		
    		biBuilder.add(items, conversionAccountWithdrawalRelativeUrl, bodyItem, false);
    	
//    		BankToCustomerStatementV08 convertedCamt053 = camt053Mapper.toCamt053(pacs008);
//    		String camt053 = om.writeValueAsString(convertedCamt053);
    		
    		String camt053RelativeUrl = String.format("datatables/transaction_details/%d", conversionAccountAmsId);
    		
    		TransactionDetails td = new TransactionDetails(
    				"$.resourceId",
    				internalCorrelationId,
    				null,
    				transactionGroupId,
    				transactionCategoryPurposeCode);
    		
    		String camt053Body = om.writeValueAsString(td);

    		biBuilder.add(items, camt053RelativeUrl, camt053Body, true);

            doBatch(items, tenantIdentifier, internalCorrelationId);
        } catch (Exception e) {
            logger.error("Worker to book incoming money in AMS has failed, dispatching user task to handle fiat deposit", e);
            throw new ZeebeBpmnError("Error_BookToConversionToBeHandledManually", e.getMessage());
        } finally {
            MDC.remove("internalCorrelationId");
        }
    }
}