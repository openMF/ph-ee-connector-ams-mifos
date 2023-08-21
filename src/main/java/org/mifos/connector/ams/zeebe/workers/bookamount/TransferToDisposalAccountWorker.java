package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import iso.std.iso._20022.tech.json.camt_053_001.AccountStatement9;
import iso.std.iso._20022.tech.json.camt_053_001.BankToCustomerStatementV08;
import org.mifos.connector.ams.fineract.Config;
import org.mifos.connector.ams.fineract.ConfigFactory;
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

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import iso.std.iso._20022.tech.json.camt_053_001.ReportEntry10;

@Component
public class TransferToDisposalAccountWorker extends AbstractMoneyInOutWorker {
	
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
			@Variable String tenantIdentifier,
			@Variable String creditorIban) throws Exception {
		try {
			MDC.put("internalCorrelationId", internalCorrelationId);
			logger.info("transfer to disposal account in payment (pacs.008) {} started for {} on {} ", internalCorrelationId, paymentScheme, tenantIdentifier);
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

			ObjectMapper objectMapper = new ObjectMapper();
			
			objectMapper.setSerializationInclusion(Include.NON_NULL);
			
			batchItemBuilder.tenantId(tenantIdentifier);
			
			String conversionAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");
			
			Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
			Integer paymentTypeId = paymentTypeConfig.findByOperation(String.format("%s.%s", paymentScheme, "transferToDisposalAccount.ConversionAccount.WithdrawTransactionAmount"));
			
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
			
			ReportEntry10 convertedCamt053Entry = camt053Mapper.toCamt053Entry(pacs008);
			String camt053Entry = objectMapper.writeValueAsString(convertedCamt053Entry);
			
			String camt053RelativeUrl = "datatables/transaction_details/$.resourceId";
			
			TransactionDetails td = new TransactionDetails(
					internalCorrelationId,
					camt053Entry,
					creditorIban,
					transactionDate,
					FORMAT,
					locale,
					transactionGroupId,
					transactionCategoryPurposeCode);
			
			String camt053Body = objectMapper.writeValueAsString(td);

			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
			
			String disposalAccountDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "deposit");
			paymentTypeId = paymentTypeConfig.findByOperation(String.format("%s.%s", paymentScheme, "transferToDisposalAccount.DisposalAccount.DepositTransactionAmount"));
			
			body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			bodyItem = objectMapper.writeValueAsString(body);
			
			batchItemBuilder.add(items, disposalAccountDepositRelativeUrl, bodyItem, false);
			
			td = new TransactionDetails(
					internalCorrelationId,
					camt053Entry,
					creditorIban,
					transactionDate,
					FORMAT,
					locale,
					transactionGroupId,
					transactionCategoryPurposeCode);
			
			camt053Body = objectMapper.writeValueAsString(td);
			
			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
		
			doBatch(items, tenantIdentifier, internalCorrelationId);
			
			logger.info("Exchange to disposal worker has finished successfully");
		} catch (Exception e) {
			logger.error("Exchange to disposal worker has failed, dispatching user task to handle exchange", e);
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
			@Variable String pacs004,
			@Variable String creditorIban) throws Exception {
		try {
			MDC.put("internalCorrelationId", internalCorrelationId);
			logger.info("transfer to disposal account in recall (pacs.004) {} started for {} on {} ", internalCorrelationId, paymentScheme, tenantIdentifier);
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
		
			ObjectMapper objectMapper = new ObjectMapper();
			
			objectMapper.setSerializationInclusion(Include.NON_NULL);
			
			batchItemBuilder.tenantId(tenantIdentifier);
			
			String conversionAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");
			
			Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
			Integer paymentTypeId = paymentTypeConfig.findByOperation(String.format("%s.%s", paymentScheme, "transferToDisposalAccount.ConversionAccount.WithdrawTransactionAmount"));
			
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
			
			ReportEntry10 convertedCamt053Entry = camt053Mapper.toCamt053Entry(pacs008);
			String camt053Entry = objectMapper.writeValueAsString(convertedCamt053Entry);
			
			String camt053RelativeUrl = "datatables/transaction_details/$.resourceId";
			
			TransactionDetails td = new TransactionDetails(
					internalCorrelationId,
					camt053Entry,
					creditorIban,
					transactionDate,
					FORMAT,
					locale,
					transactionGroupId,
					transactionCategoryPurposeCode);
			
			String camt053Body = objectMapper.writeValueAsString(td);

			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
			
			String disposalAccountDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "deposit");
			paymentTypeId = paymentTypeConfig.findByOperation(String.format("%s.%s", paymentScheme, "transferToDisposalAccount.DisposalAccount.DepositTransactionAmount"));
			
			body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			bodyItem = objectMapper.writeValueAsString(body);
			
			batchItemBuilder.add(items, disposalAccountDepositRelativeUrl, bodyItem, false);
			
			td = new TransactionDetails(
					internalCorrelationId,
					camt053Entry,
					creditorIban,
					transactionDate,
					FORMAT,
					locale,
					transactionGroupId,
					transactionCategoryPurposeCode);
			
			camt053Body = objectMapper.writeValueAsString(td);
			
			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
		
			doBatch(items, tenantIdentifier, internalCorrelationId);
			
			logger.info("Exchange to disposal worker has finished successfully");
		} catch (Exception e) {
			logger.error("Exchange to disposal worker has failed, dispatching user task to handle exchange", e);
			throw new ZeebeBpmnError("Error_TransferToDisposalToBeHandledManually", e.getMessage());
		} finally {
			MDC.remove("internalCorrelationId");
		}
	}

	@JobWorker
	public void transferToDisposalAccountInReturn(JobClient jobClient,
												  ActivatedJob activatedJob,
												  @Variable String pacs004,
												  @Variable String internalCorrelationId,
												  @Variable String paymentScheme,
												  @Variable String transactionDate,
												  @Variable String transactionGroupId,
												  @Variable String transactionCategoryPurposeCode,
												  @Variable BigDecimal amount,
												  @Variable Integer conversionAccountAmsId,
												  @Variable Integer disposalAccountAmsId,
												  @Variable String tenantIdentifier,
												  @Variable String creditorIban) throws Exception {
		try {
			MDC.put("internalCorrelationId", internalCorrelationId);
			logger.info("transfer to disposal account in return (pacs.004) {} started for {} on {} ", internalCorrelationId, paymentScheme, tenantIdentifier);
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

			ObjectMapper objectMapper = new ObjectMapper();

			batchItemBuilder.tenantId(tenantIdentifier);

			String conversionAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");

			Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
			Integer paymentTypeId = paymentTypeConfig.findByOperation(String.format("%s.%s", paymentScheme, "transferToDisposalAccount.ConversionAccount.WithdrawTransactionAmount"));

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

			// TODO make proper pacs.004 -> camt.053 converter
			BankToCustomerStatementV08 camt053 = new BankToCustomerStatementV08();
			camt053.getStatement().add(new AccountStatement9());
			camt053.getStatement().get(0).getEntry().add(new ReportEntry10());
			ReportEntry10 convertedCamt053Entry = camt053.getStatement().get(0).getEntry().get(0);

			String camt053Entry = objectMapper.writeValueAsString(convertedCamt053Entry);

			String camt053RelativeUrl = "datatables/transaction_details/$.resourceId";

			TransactionDetails td = new TransactionDetails(
					internalCorrelationId,
					camt053Entry,
					creditorIban,
					transactionDate,
					FORMAT,
					locale,
					transactionGroupId,
					transactionCategoryPurposeCode);

			String camt053Body = objectMapper.writeValueAsString(td);

			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);


			String disposalAccountDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "deposit");
			paymentTypeId = paymentTypeConfig.findByOperation(String.format("%s.%s", paymentScheme, "transferToDisposalAccount.DisposalAccount.DepositTransactionAmount"));

			body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);

			bodyItem = objectMapper.writeValueAsString(body);

			batchItemBuilder.add(items, disposalAccountDepositRelativeUrl, bodyItem, false);

			camt053RelativeUrl = "datatables/transaction_details/$.resourceId";

			td = new TransactionDetails(
					internalCorrelationId,
					camt053Entry,
					creditorIban,
					transactionDate,
					FORMAT,
					locale,
					transactionGroupId,
					transactionCategoryPurposeCode);

			camt053Body = objectMapper.writeValueAsString(td);

			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);

			doBatch(items, tenantIdentifier, internalCorrelationId);

			logger.info("Exchange to disposal worker has finished successfully");
		} catch (Exception e) {
			logger.error("Exchange to disposal worker has failed, dispatching user task to handle exchange", e);
			throw new ZeebeBpmnError("Error_TransferToDisposalToBeHandledManually", e.getMessage());
		} finally {
			MDC.remove("internalCorrelationId");
		}
	}
}