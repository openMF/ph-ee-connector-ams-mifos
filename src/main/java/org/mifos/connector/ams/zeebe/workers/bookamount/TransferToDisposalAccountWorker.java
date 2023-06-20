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
public class TransferToDisposalAccountWorker extends AbstractMoneyInOutWorker {
	
	@Autowired
	private Pacs008Camt053Mapper camt053Mapper;
	
	@Value("${fineract.incoming-money-api}")
	protected String incomingMoneyApi;
	
	@Autowired
    private PaymentTypeConfigFactory paymentTypeConfigFactory;
	
	@Autowired
	private JAXBUtils jaxbUtils;
	
	@Autowired
	private BatchItemBuilder batchItemBuilder;

	@JobWorker
	public void transferToDisposalAccount(JobClient jobClient, 
			ActivatedJob activatedJob,
			@Variable String originalPacs008,
			@Variable String internalCorrelationId,
			@Variable String paymentScheme,
			@Variable String transactionDate,
			@Variable String transactionGroupId,
			@Variable String transactionCategoryPurposeCode,
			@Variable BigDecimal amount,
			@Variable Integer conversionAccountAmsId,
			@Variable Integer disposalAccountAmsId,
			@Variable String tenantIdentifier) throws Exception {
		try {
			iso.std.iso._20022.tech.xsd.pacs_008_001.Document pacs008 = jaxbUtils.unmarshalPacs008(originalPacs008);
		
			MDC.put("internalCorrelationId", internalCorrelationId);
			
			logger.info("Exchange to e-currency worker has started");

			ObjectMapper objectMapper = new ObjectMapper();
			
			batchItemBuilder.tenantId(tenantIdentifier);
			
			String conversionAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");
			
			PaymentTypeConfig paymentTypeConfig = paymentTypeConfigFactory.getPaymentTypeConfig(tenantIdentifier);
			Integer paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "transferToDisposalAccount.ConversionAccount.WithdrawTransactionAmount"));
			
			TransactionBody body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			String bodyItem = objectMapper.writeValueAsString(body);
			
			List<TransactionItem> items = new ArrayList<>();
			
			batchItemBuilder.add(items, conversionAccountWithdrawRelativeUrl, bodyItem, false);
			
			BankToCustomerStatementV08 convertedCamt053 = camt053Mapper.toCamt053(pacs008);
			String camt053 = objectMapper.writeValueAsString(convertedCamt053);
			
			String camt053RelativeUrl = String.format("datatables/transaction_details/%d", conversionAccountAmsId);
			
			TransactionDetails td = new TransactionDetails(
					"$.resourceId",
					internalCorrelationId,
					camt053,
					transactionGroupId,
					transactionCategoryPurposeCode);
			
			String camt053Body = objectMapper.writeValueAsString(td);

			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
			
			String disposalAccountDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "deposit");
			paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "transferToDisposalAccount.DisposalAccount.DepositTransactionAmount"));
			
			body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			bodyItem = objectMapper.writeValueAsString(body);
			
			batchItemBuilder.add(items, disposalAccountDepositRelativeUrl, bodyItem, false);
			
			camt053RelativeUrl = String.format("datatables/transaction_details/%d", disposalAccountAmsId);
			
			td = new TransactionDetails(
					"$.resourceId",
					internalCorrelationId,
					camt053,
					transactionGroupId,
					transactionCategoryPurposeCode);
			
			camt053Body = objectMapper.writeValueAsString(td);
			
			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
		
			doBatch(items, tenantIdentifier, internalCorrelationId);
			
			logger.info("Exchange to e-currency worker has finished successfully");
		} catch (Exception e) {
			logger.error("Exchange to e-currency worker has failed, dispatching user task to handle exchange", e);
			throw new ZeebeBpmnError("Error_TransferToDisposalToBeHandledManually", e.getMessage());
		} finally {
			MDC.remove("internalCorrelationId");
		}
	}
	
	@JobWorker
	public void transferToDisposalAccountInRecall(JobClient jobClient, 
			ActivatedJob activatedJob,
			@Variable String originalPacs008,
			@Variable String internalCorrelationId,
			@Variable String paymentScheme,
			@Variable String transactionDate,
			@Variable String transactionGroupId,
			@Variable String transactionCategoryPurposeCode,
			@Variable BigDecimal amount,
			@Variable Integer conversionAccountAmsId,
			@Variable Integer disposalAccountAmsId,
			@Variable String tenantIdentifier,
			@Variable String pacs004) throws Exception {
		try {
			iso.std.iso._20022.tech.xsd.pacs_008_001.Document pacs008 = jaxbUtils.unmarshalPacs008(originalPacs008);
		
			MDC.put("internalCorrelationId", internalCorrelationId);
			
			logger.info("Exchange to e-currency worker has started");

			ObjectMapper objectMapper = new ObjectMapper();
			
			batchItemBuilder.tenantId(tenantIdentifier);
			
			String conversionAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");
			
			PaymentTypeConfig paymentTypeConfig = paymentTypeConfigFactory.getPaymentTypeConfig(tenantIdentifier);
			Integer paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "transferToDisposalAccount.ConversionAccount.WithdrawTransactionAmount"));
			
			TransactionBody body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			String bodyItem = objectMapper.writeValueAsString(body);
			
			List<TransactionItem> items = new ArrayList<>();
			
			batchItemBuilder.add(items, conversionAccountWithdrawRelativeUrl, bodyItem, false);
			
			BankToCustomerStatementV08 convertedCamt053 = camt053Mapper.toCamt053(pacs008);
			String camt053 = objectMapper.writeValueAsString(convertedCamt053);
			
			String camt053RelativeUrl = String.format("datatables/transaction_details/%d", conversionAccountAmsId);
			
			TransactionDetails td = new TransactionDetails(
					"$.resourceId",
					internalCorrelationId,
					camt053,
					transactionGroupId,
					transactionCategoryPurposeCode);
			
			String camt053Body = objectMapper.writeValueAsString(td);

			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
			
			String disposalAccountDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "deposit");
			paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "transferToDisposalAccount.DisposalAccount.DepositTransactionAmount"));
			
			body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			bodyItem = objectMapper.writeValueAsString(body);
			
			batchItemBuilder.add(items, disposalAccountDepositRelativeUrl, bodyItem, false);
			
			camt053RelativeUrl = String.format("datatables/transaction_details/%d", disposalAccountAmsId);
			
			td = new TransactionDetails(
					"$.resourceId",
					internalCorrelationId,
					camt053,
					transactionGroupId,
					transactionCategoryPurposeCode);
			
			camt053Body = objectMapper.writeValueAsString(td);
			
			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
		
			doBatch(items, tenantIdentifier, internalCorrelationId);
			
			logger.info("Exchange to e-currency worker has finished successfully");
		} catch (Exception e) {
			logger.error("Exchange to e-currency worker has failed, dispatching user task to handle exchange", e);
			throw new ZeebeBpmnError("Error_TransferToDisposalToBeHandledManually", e.getMessage());
		} finally {
			MDC.remove("internalCorrelationId");
		}
	}
}
