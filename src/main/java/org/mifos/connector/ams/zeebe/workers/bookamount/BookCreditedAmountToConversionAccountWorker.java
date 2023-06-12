package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.mifos.connector.ams.fineract.PaymentTypeConfig;
import org.mifos.connector.ams.fineract.PaymentTypeConfigFactory;
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

import com.fasterxml.jackson.databind.ObjectMapper;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import iso.std.iso._20022.tech.json.camt_053_001.BankToCustomerStatementV08;

@Component
public class BookCreditedAmountToConversionAccountWorker {
	
	private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Pacs008Camt053Mapper camt053Mapper;
    
    @Value("${fineract.incoming-money-api}")
	private String incomingMoneyApi;
    
    @Value("${fineract.locale}")
	private String locale;
	
	@Autowired
    private PaymentTypeConfigFactory paymentTypeConfigFactory;
	
	@Autowired
	private JAXBUtils jaxbUtils;
	
	@Autowired
	private BatchItemBuilder batchItemBuilder;
	
	@Autowired
	private MoneyInOutHelperWorker moneyInOutHelperWorker;
    
	@JobWorker
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

            iso.std.iso._20022.tech.xsd.pacs_008_001.Document pacs008 = jaxbUtils.unmarshalPacs008(originalPacs008);

            MDC.put("internalCorrelationId", internalCorrelationId);

            ObjectMapper objectMapper = new ObjectMapper();
            BankToCustomerStatementV08 convertedCamt053 = camt053Mapper.toCamt053(pacs008);
            String camt053 = objectMapper.writeValueAsString(convertedCamt053);

            batchItemBuilder.tenantId(tenantIdentifier);
    		
    		String conversionAccountWithdrawalRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "deposit");
    		
    		PaymentTypeConfig paymentTypeConfig = paymentTypeConfigFactory.getPaymentTypeConfig(tenantIdentifier);
    		Integer paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "bookCreditedAmountToConversionAccount.ConversionAccount.DepositTransactionAmount"));
    		
    		TransactionBody body = new TransactionBody(
    				transactionDate,
    				amount,
    				paymentTypeId,
    				"",
    				MoneyInOutHelperWorker.FORMAT,
    				locale);
    		
    		
    		String bodyItem = objectMapper.writeValueAsString(body);
    		
    		List<TransactionItem> items = new ArrayList<>();
    		
    		batchItemBuilder.add(items, conversionAccountWithdrawalRelativeUrl, bodyItem, false);
    	
    		
    		String camt053RelativeUrl = String.format("datatables/transaction_details/%d", conversionAccountAmsId);
    		
    		TransactionDetails td = new TransactionDetails(
    				"$.resourceId",
    				internalCorrelationId,
    				camt053,
    				transactionGroupId,
    				transactionCategoryPurposeCode);
    		
    		String camt053Body = objectMapper.writeValueAsString(td);

    		batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);

    		moneyInOutHelperWorker.doBatch(items, tenantIdentifier, internalCorrelationId);
        } catch (Exception e) {
            logger.error("Worker to book incoming money in AMS has failed, dispatching user task to handle fiat deposit", e);
            throw new ZeebeBpmnError("Error_BookToConversionToBeHandledManually", e.getMessage());
        } finally {
            MDC.remove("internalCorrelationId");
        }
    }
}