package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.mifos.connector.ams.fineract.Config;
import org.mifos.connector.ams.fineract.ConfigFactory;
import org.mifos.connector.ams.mapstruct.Pacs008Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.BatchItemBuilder;
import org.mifos.connector.ams.zeebe.workers.utils.JAXBUtils;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionBody;
import org.mifos.connector.ams.zeebe.workers.utils.DtSavingsTransactionDetails;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import iso.std.iso._20022.tech.json.camt_053_001.AccountStatement9;
import iso.std.iso._20022.tech.json.camt_053_001.BankToCustomerStatementV08;
import iso.std.iso._20022.tech.json.camt_053_001.ReportEntry10;
import iso.std.iso._20022.tech.json.camt_053_001.ActiveOrHistoricCurrencyAndAmountRange2.CreditDebitCode;

@Component
public class BookCreditedAmountToConversionAccountWorker extends AbstractMoneyInOutWorker {

    @Autowired
    private Pacs008Camt053Mapper camt053Mapper;

    @Value("${fineract.incoming-money-api}")
    protected String incomingMoneyApi;

    @Autowired
    private ConfigFactory paymentTypeConfigFactory;
	
	@Autowired
	private JAXBUtils jaxbUtils;
	
	@Autowired
	private BatchItemBuilder batchItemBuilder;
    
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
    		@Variable Integer conversionAccountAmsId,
    		@Variable String creditorIban) throws Exception {
        try {
            MDC.put("internalCorrelationId", internalCorrelationId);
            logger.info("book to conversion account in payment (pacs.008) {} started for {} on {} ", internalCorrelationId, paymentScheme, tenantIdentifier);
            if (logger.isDebugEnabled()) {
                logger.debug("activated job type {} with key {} at element {} of workflow {} with instance key {}\nheaders: {}\nvariables: {})",
                        activatedJob.getType(),
                        activatedJob.getKey(),
                        activatedJob.getElementId(),
                        activatedJob.getBpmnProcessId(),
                        activatedJob.getProcessInstanceKey(),
                        activatedJob.getCustomHeaders(),
                        activatedJob.getVariables());
            }

            iso.std.iso._20022.tech.xsd.pacs_008_001.Document pacs008 = jaxbUtils.unmarshalPacs008(originalPacs008);

            batchItemBuilder.tenantId(tenantIdentifier);
    		
    		String conversionAccountWithdrawalRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "deposit");
    		
    		Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
    		String depositAmountOperation = "bookCreditedAmountToConversionAccount.ConversionAccount.DepositTransactionAmount";
			String configOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
			Integer paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(configOperationKey);
			String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(configOperationKey);
    		
    		TransactionBody body = new TransactionBody(
    				transactionDate,
    				amount,
    				paymentTypeId,
    				"",
    				FORMAT,
    				locale);
    		
    		ObjectMapper objectMapper = new ObjectMapper();
    		
    		objectMapper.setSerializationInclusion(Include.NON_NULL);
    		
    		String bodyItem = objectMapper.writeValueAsString(body);
    		
    		List<TransactionItem> items = new ArrayList<>();
    		
    		batchItemBuilder.add(items, conversionAccountWithdrawalRelativeUrl, bodyItem, false);
    	
    		ReportEntry10 convertedCamt053Entry = camt053Mapper.toCamt053Entry(pacs008);
    		convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.CRDT);
    		String camt053Entry = objectMapper.writeValueAsString(convertedCamt053Entry);
    		
    		String camt053RelativeUrl = "datatables/transaction_details/$.resourceId";
    		
    		DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
    				internalCorrelationId,
    				camt053Entry,
    				creditorIban,
    				paymentTypeCode,
    				transactionGroupId,
    				pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtr().getNm(),
    				transactionCategoryPurposeCode);
    		
    		String camt053Body = objectMapper.writeValueAsString(td);

            batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);

            doBatch(items, tenantIdentifier, internalCorrelationId);
        } catch (Exception e) {
            logger.error("Worker to book incoming money in AMS has failed, dispatching user task to handle conversion account deposit", e);
            throw new ZeebeBpmnError("Error_BookToConversionToBeHandledManually", e.getMessage());
        } finally {
            MDC.remove("internalCorrelationId");
        }
    }
	
	@JobWorker
    public void bookCreditedAmountToConversionAccountInRecall(JobClient jobClient, 
    		ActivatedJob activatedJob,
    		@Variable String originalPacs008,
    		@Variable String transactionDate,
    		@Variable String transactionCategoryPurposeCode,
    		@Variable String transactionGroupId,
    		@Variable String internalCorrelationId,
    		@Variable String tenantIdentifier,
    		@Variable String paymentScheme,
    		@Variable BigDecimal amount,
    		@Variable Integer conversionAccountAmsId,
    		@Variable String pacs004,
    		@Variable String creditorIban) throws Exception {
        try {
            MDC.put("internalCorrelationId", internalCorrelationId);
            logger.info("book to conversion account in recall (pacs.004) {} started for {} on {} ", internalCorrelationId, paymentScheme, tenantIdentifier);
            if (logger.isDebugEnabled()) {
                logger.debug("activated job type {} with key {} at element {} of workflow {} with instance key {}\nheaders: {}\nvariables: {})",
                        activatedJob.getType(),
                        activatedJob.getKey(),
                        activatedJob.getElementId(),
                        activatedJob.getBpmnProcessId(),
                        activatedJob.getProcessInstanceKey(),
                        activatedJob.getCustomHeaders(),
                        activatedJob.getVariables());
            }

            iso.std.iso._20022.tech.xsd.pacs_008_001.Document pacs008 = jaxbUtils.unmarshalPacs008(originalPacs008);


            batchItemBuilder.tenantId(tenantIdentifier);
    		
    		String conversionAccountWithdrawalRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "deposit");
    		
    		Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
    		String depositAmountOperation = "bookCreditedAmountToConversionAccount.ConversionAccount.DepositTransactionAmount";
			String configOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
			Integer paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(configOperationKey);
			String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(configOperationKey);
    		
    		TransactionBody body = new TransactionBody(
    				transactionDate,
    				amount,
    				paymentTypeId,
    				"",
    				FORMAT,
    				locale);
    		
    		ObjectMapper objectMapper = new ObjectMapper();
    		
    		objectMapper.setSerializationInclusion(Include.NON_NULL);
    		
    		String bodyItem = objectMapper.writeValueAsString(body);
    		
    		List<TransactionItem> items = new ArrayList<>();
    		
    		batchItemBuilder.add(items, conversionAccountWithdrawalRelativeUrl, bodyItem, false);
    	
    		ReportEntry10 convertedCamt053Entry = camt053Mapper.toCamt053Entry(pacs008);
    		convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.CRDT);
    		String camt053Entry = objectMapper.writeValueAsString(convertedCamt053Entry);
    		
    		String camt053RelativeUrl = "datatables/transaction_details/$.resourceId";
    		
    		DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
    				internalCorrelationId,
    				camt053Entry,
    				creditorIban,
    				paymentTypeCode,
    				transactionGroupId,
    				pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtr().getNm(),
    				transactionCategoryPurposeCode);
    		
    		String camt053Body = objectMapper.writeValueAsString(td);

            batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);

            doBatch(items, tenantIdentifier, internalCorrelationId);
        } catch (Exception e) {
            logger.error("Worker to book incoming money in AMS has failed, dispatching user task to handle conversion account deposit", e);
            throw new ZeebeBpmnError("Error_BookToConversionToBeHandledManually", e.getMessage());
        } finally {
            MDC.remove("internalCorrelationId");
        }
    }

    @JobWorker
    public void bookCreditedAmountToConversionAccountInReturn(JobClient jobClient,
                                                              ActivatedJob activatedJob,
                                                              @Variable String pacs004,
                                                              @Variable String transactionDate,
                                                              @Variable String transactionCategoryPurposeCode,
                                                              @Variable String transactionGroupId,
                                                              @Variable String internalCorrelationId,
                                                              @Variable String tenantIdentifier,
                                                              @Variable String paymentScheme,
                                                              @Variable BigDecimal amount,
                                                              @Variable Integer conversionAccountAmsId,
                                                              @Variable String creditorIban) throws Exception {
        try {
            MDC.put("internalCorrelationId", internalCorrelationId);
            logger.info("book to conversion account in return (pacs.004) {} started for {} on {} ", internalCorrelationId, paymentScheme, tenantIdentifier);
            if (logger.isDebugEnabled()) {
                logger.debug("activated job type {} with key {} at element {} of workflow {} with instance key {}\nheaders: {}\nvariables: {})",
                        activatedJob.getType(),
                        activatedJob.getKey(),
                        activatedJob.getElementId(),
                        activatedJob.getBpmnProcessId(),
                        activatedJob.getProcessInstanceKey(),
                        activatedJob.getCustomHeaders(),
                        activatedJob.getVariables());
            }

            batchItemBuilder.tenantId(tenantIdentifier);

            String conversionAccountWithdrawalRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "deposit");

            Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
            String depositAmountOperation = "bookCreditedAmountToConversionAccount.ConversionAccount.DepositTransactionAmount";
			String configOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
			Integer paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(configOperationKey);
			String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(configOperationKey);
			
			iso.std.iso._20022.tech.xsd.pacs_004_001.Document pacs_004 = jaxbUtils.unmarshalPacs004(pacs004);

            TransactionBody body = new TransactionBody(
                    transactionDate,
                    amount,
                    paymentTypeId,
                    "",
                    FORMAT,
                    locale);

            ObjectMapper objectMapper = new ObjectMapper();

            String bodyItem = objectMapper.writeValueAsString(body);

            List<TransactionItem> items = new ArrayList<>();

            batchItemBuilder.add(items, conversionAccountWithdrawalRelativeUrl, bodyItem, false);

            // TODO make proper pacs.004 -> camt.053 converter
            BankToCustomerStatementV08 camt053 = new BankToCustomerStatementV08();
            camt053.getStatement().add(new AccountStatement9());
            camt053.getStatement().get(0).getEntry().add(new ReportEntry10());
            ReportEntry10 convertedCamt053Entry = camt053.getStatement().get(0).getEntry().get(0);

            String camt053Entry = objectMapper.writeValueAsString(convertedCamt053Entry);

            String camt053RelativeUrl = "datatables/transaction_details/$.resourceId";

            DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
                    internalCorrelationId,
                    camt053Entry,
                    creditorIban,
                    paymentTypeCode,
                    transactionGroupId,
                    pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getCdtr().getNm(),
                    transactionCategoryPurposeCode);

            String camt053Body = objectMapper.writeValueAsString(td);

            batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);

            doBatch(items, tenantIdentifier, internalCorrelationId);
        } catch (Exception e) {
            logger.error("Worker to book incoming money in AMS has failed, dispatching user task to handle conversion account deposit", e);
            throw new ZeebeBpmnError("Error_BookToConversionToBeHandledManually", e.getMessage());
        } finally {
            MDC.remove("internalCorrelationId");
        }
    }
}