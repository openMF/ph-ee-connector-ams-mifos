package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.mifos.connector.ams.fineract.Config;
import org.mifos.connector.ams.fineract.ConfigFactory;
import org.mifos.connector.ams.mapstruct.Pain001Camt053Mapper;
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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import hu.dpc.rt.utils.converter.Camt056ToCamt053Converter;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import iso.std.iso._20022.tech.json.camt_053_001.BankToCustomerStatementV08;
import iso.std.iso._20022.tech.json.camt_053_001.ReportEntry10;
import iso.std.iso._20022.tech.json.camt_053_001.ActiveOrHistoricCurrencyAndAmountRange2.CreditDebitCode;
import iso.std.iso._20022.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;
import iso.std.iso._20022.tech.xsd.camt_056_001.PaymentTransactionInformation31;
import jakarta.xml.bind.JAXBException;

@Component
public class RevertInAmsWorker extends AbstractMoneyInOutWorker {
	
	@Autowired
	private Pain001Camt053Mapper camt053Mapper;
	
	@Value("${fineract.incoming-money-api}")
	protected String incomingMoneyApi;
	
	@Autowired
    private ConfigFactory paymentTypeConfigFactory;
	
	@Autowired
	private JAXBUtils jaxbUtils;
	
	@Autowired
	private BatchItemBuilder batchItemBuilder;
	
	private final ObjectMapper objectMapper = new ObjectMapper();
	
	@JobWorker
	public void revertInAms(JobClient jobClient, 
			ActivatedJob activatedJob,
			@Variable String internalCorrelationId,
			@Variable String originalPain001,
			@Variable Integer conversionAccountAmsId,
			@Variable Integer disposalAccountAmsId,
			@Variable String transactionDate,
			@Variable String paymentScheme,
			@Variable String transactionGroupId,
			@Variable String transactionCategoryPurposeCode,
			@Variable BigDecimal amount,
			@Variable String transactionFeeCategoryPurposeCode,
			@Variable BigDecimal transactionFeeAmount,
			@Variable String tenantIdentifier,
			@Variable String debtorIban) throws Exception {
		MDC.put("internalCorrelationId", internalCorrelationId);
		
		Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = objectMapper.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
		
		logger.debug("Withdrawing amount {} from conversion account {}", amount, conversionAccountAmsId);
		
		batchItemBuilder.tenantId(tenantIdentifier);
		
		String conversionAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");
		
		Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
		String withdrawAmountOperation = "revertInAms.ConversionAccount.WithdrawTransactionAmount";
		String withdrawAmountConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
		Integer paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawAmountConfigOperationKey);
		String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawAmountConfigOperationKey);
		
		TransactionBody body = new TransactionBody(
				transactionDate,
				amount,
				paymentTypeId,
				"",
				FORMAT,
				locale);
		
		objectMapper.setSerializationInclusion(Include.NON_NULL);
		
		String bodyItem = objectMapper.writeValueAsString(body);
		
		List<TransactionItem> items = new ArrayList<>();
		
		batchItemBuilder.add(items, conversionAccountWithdrawRelativeUrl, bodyItem, false);
		
		ReportEntry10 convertedcamt053Entry = camt053Mapper.toCamt053Entry(pain001.getDocument());
		convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.DBIT);
		String camt053Entry = objectMapper.writeValueAsString(convertedcamt053Entry);
		
		String camt053RelativeUrl = "datatables/transaction_details/$.resourceId";
		
		DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
				internalCorrelationId,
				camt053Entry,
				debtorIban,
				paymentTypeCode,
				transactionGroupId,
				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getName(),
				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditorAccount().getIdentification().getIban(),
				null,
				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getContactDetails().toString(),
				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getRemittanceInformation().getUnstructured().toString(),
				transactionCategoryPurposeCode);
		
		String camt053Body = objectMapper.writeValueAsString(td);

		batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
		
		if (!BigDecimal.ZERO.equals(transactionFeeAmount)) {
			logger.debug("Withdrawing fee {} from conversion account {}", transactionFeeAmount, conversionAccountAmsId);
			
			String withdrawFeeOperation = "revertInAms.ConversionAccount.WithdrawTransactionFee";
			String withdrawFeeConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawFeeOperation);
			paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawFeeConfigOperationKey);
			paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawFeeConfigOperationKey);
			
			body = new TransactionBody(
					transactionDate,
					transactionFeeAmount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			bodyItem = objectMapper.writeValueAsString(body);
			
			batchItemBuilder.add(items, conversionAccountWithdrawRelativeUrl, bodyItem, false);
			
			td = new DtSavingsTransactionDetails(
					internalCorrelationId,
					camt053Entry,
					debtorIban,
					paymentTypeCode,
					transactionGroupId,
					pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getName(),
					pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditorAccount().getIdentification().getIban(),
					null,
					pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getContactDetails().toString(),
					pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getRemittanceInformation().getUnstructured().toString(),
					transactionFeeCategoryPurposeCode);
			camt053Body = objectMapper.writeValueAsString(td);
			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
		}

		logger.debug("Re-depositing amount {} in disposal account {}", amount, disposalAccountAmsId);
		
		String disposalAccountDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "deposit");
		
		String depositAmountOperation = "revertInAms.DisposalAccount.DepositTransactionAmount";
		String depositAmountConfigOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
		paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(depositAmountConfigOperationKey);
		paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(depositAmountConfigOperationKey);
		
		body = new TransactionBody(
				transactionDate,
				amount,
				paymentTypeId,
				"",
				FORMAT,
				locale);
		
		bodyItem = objectMapper.writeValueAsString(body);
		
		batchItemBuilder.add(items, disposalAccountDepositRelativeUrl, bodyItem, false);
		
		td = new DtSavingsTransactionDetails(
				internalCorrelationId,
				camt053Entry,
				debtorIban,
				paymentTypeCode,
				transactionGroupId,
				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getName(),
				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditorAccount().getIdentification().getIban(),
				null,
				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getContactDetails().toString(),
				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getRemittanceInformation().getUnstructured().toString(),
				transactionCategoryPurposeCode);
		
		camt053Body = objectMapper.writeValueAsString(td);
		
		batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
		
		if (!BigDecimal.ZERO.equals(transactionFeeAmount)) {
			logger.debug("Re-depositing fee {} in disposal account {}", transactionFeeAmount, disposalAccountAmsId);
			
			String depositFeeOperation = "revertInAms.DisposalAccount.DepositTransactionFee";
			String depositFeeConfigOperationKey = String.format("%s.%s", paymentScheme, depositFeeOperation);
			paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(depositFeeConfigOperationKey);
			paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(depositFeeConfigOperationKey);
			
			body = new TransactionBody(
					transactionDate,
					transactionFeeAmount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			bodyItem = objectMapper.writeValueAsString(body);
			
			batchItemBuilder.add(items, disposalAccountDepositRelativeUrl, bodyItem, false);
			
			td = new DtSavingsTransactionDetails(
					internalCorrelationId,
					camt053Entry,
					debtorIban,
					paymentTypeCode,
					transactionGroupId,
					pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getName(),
					pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditorAccount().getIdentification().getIban(),
					null,
					pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getContactDetails().toString(),
					pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getRemittanceInformation().getUnstructured().toString(),
					transactionFeeCategoryPurposeCode);
			camt053Body = objectMapper.writeValueAsString(td);
			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
		}
		
		doBatch(items, tenantIdentifier, internalCorrelationId);
		
		MDC.remove("internalCorrelationId");
	}
	
	@JobWorker
	public void revertWithoutFeeInAms(JobClient jobClient, 
			ActivatedJob activatedJob,
			@Variable String internalCorrelationId,
			@Variable String originalPain001,
			@Variable Integer conversionAccountAmsId,
			@Variable Integer disposalAccountAmsId,
			@Variable String paymentScheme,
			@Variable String transactionGroupId,
			@Variable String transactionCategoryPurposeCode,
			@Variable BigDecimal amount,
			@Variable String transactionFeeCategoryPurposeCode,
			@Variable BigDecimal transactionFeeAmount,
			@Variable String tenantIdentifier,
			@Variable String debtorIban) throws Exception {
		MDC.put("internalCorrelationId", internalCorrelationId);
		
		String transactionDate = LocalDate.now().format(DateTimeFormatter.ofPattern(FORMAT));
		
		objectMapper.setSerializationInclusion(Include.NON_NULL);
		
		Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = objectMapper.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
		
		logger.debug("Withdrawing amount {} from conversion account {}", amount, conversionAccountAmsId);
		
		batchItemBuilder.tenantId(tenantIdentifier);
		
		String conversionAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");
		
		Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
		String withdrawAmountOperation = "revertInAms.ConversionAccount.WithdrawTransactionAmount";
		String withdrawAmountConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
		Integer paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawAmountConfigOperationKey);
		String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawAmountConfigOperationKey);
		
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
		
		ReportEntry10 convertedcamt053Entry = camt053Mapper.toCamt053Entry(pain001.getDocument());
		convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.DBIT);
		String camt053Entry = objectMapper.writeValueAsString(convertedcamt053Entry);
		
		String camt053RelativeUrl = "datatables/transaction_details/$.resourceId";
		
		DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
				internalCorrelationId,
				camt053Entry,
				debtorIban,
				paymentTypeCode,
				transactionGroupId,
				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getName(),
				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditorAccount().getIdentification().getIban(),
				null,
				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getContactDetails().toString(),
				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getRemittanceInformation().getUnstructured().toString(),
				transactionCategoryPurposeCode);
		
		String camt053Body = objectMapper.writeValueAsString(td);

		batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
		
		if (!BigDecimal.ZERO.equals(transactionFeeAmount)) {
			logger.debug("Withdrawing fee {} from conversion account {}", transactionFeeAmount, conversionAccountAmsId);
			
			String withdrawFeeOperation = "revertInAms.ConversionAccount.WithdrawTransactionFee";
			String withdrawFeeConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawFeeOperation);
			paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawFeeConfigOperationKey);
			paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawFeeConfigOperationKey);
			
			body = new TransactionBody(
					transactionDate,
					transactionFeeAmount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			bodyItem = objectMapper.writeValueAsString(body);
			
			batchItemBuilder.add(items, conversionAccountWithdrawRelativeUrl, bodyItem, false);
			
			td = new DtSavingsTransactionDetails(
					internalCorrelationId,
					camt053Entry,
					debtorIban,
					paymentTypeCode,
					transactionGroupId,
					pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getName(),
					pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditorAccount().getIdentification().getIban(),
					null,
					pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getContactDetails().toString(),
					pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getRemittanceInformation().getUnstructured().toString(),
					transactionFeeCategoryPurposeCode);
			camt053Body = objectMapper.writeValueAsString(td);
			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
		}

		logger.debug("Re-depositing amount {} in disposal account {}", amount, disposalAccountAmsId);
		
		String disposalAccountDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "deposit");
		
		String depositAmountOperation = "revertInAms.DisposalAccount.DepositTransactionAmount";
		String depositAmountConfigOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
		paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(depositAmountConfigOperationKey);
		paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(depositAmountConfigOperationKey);
		
		body = new TransactionBody(
				transactionDate,
				amount,
				paymentTypeId,
				"",
				FORMAT,
				locale);
		
		bodyItem = objectMapper.writeValueAsString(body);
		
		batchItemBuilder.add(items, disposalAccountDepositRelativeUrl, bodyItem, false);
		
		td = new DtSavingsTransactionDetails(
				internalCorrelationId,
				camt053Entry,
				debtorIban,
				paymentTypeCode,
				transactionGroupId,
				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getName(),
				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditorAccount().getIdentification().getIban(),
				null,
				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getContactDetails().toString(),
				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getRemittanceInformation().getUnstructured().toString(),
				transactionCategoryPurposeCode);
		
		camt053Body = objectMapper.writeValueAsString(td);
		
		batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
		
		doBatch(items, tenantIdentifier, internalCorrelationId);
		
		MDC.remove("internalCorrelationId");
	}
	
	@JobWorker
	public void depositTheAmountOnDisposalInAms(JobClient client,
			ActivatedJob job,
			@Variable BigDecimal amount,
			@Variable Integer conversionAccountAmsId,
			@Variable Integer disposalAccountAmsId,
			@Variable String tenantIdentifier,
			@Variable String paymentScheme,
			@Variable String transactionCategoryPurposeCode,
			@Variable String camt056,
			@Variable String debtorIban) {
		try {
			batchItemBuilder.tenantId(tenantIdentifier);
			
			String transactionDate = LocalDate.now().format(DateTimeFormatter.ofPattern(FORMAT));
			
			String conversionAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");
			
			Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
			String withdrawAmountOperation = "revertInAms.ConversionAccount.WithdrawTransactionAmount";
			String withdrawAmountConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
			Integer paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawAmountConfigOperationKey);
			String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawAmountConfigOperationKey);
			
			TransactionBody body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			objectMapper.setSerializationInclusion(Include.NON_NULL);
			
			String bodyItem = objectMapper.writeValueAsString(body);
			
			List<TransactionItem> items = new ArrayList<>();
			
			batchItemBuilder.add(items, conversionAccountWithdrawRelativeUrl, bodyItem, false);
			
			iso.std.iso._20022.tech.xsd.camt_056_001.Document document = jaxbUtils.unmarshalCamt056(camt056);
			Camt056ToCamt053Converter converter = new Camt056ToCamt053Converter();
			BankToCustomerStatementV08 statement = converter.convert(document);
			
			PaymentTransactionInformation31 paymentTransactionInformation = document
					.getFIToFIPmtCxlReq()
					.getUndrlyg().get(0)
					.getTxInf().get(0);
			
			String originalDebtorBic = paymentTransactionInformation
					.getOrgnlTxRef()
					.getDbtrAgt()
					.getFinInstnId()
					.getBIC();
			String originalCreationDate = paymentTransactionInformation
					.getOrgnlGrpInf()
					.getOrgnlCreDtTm()
					.toGregorianCalendar()
					.toZonedDateTime()
					.toLocalDate()
					.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
			String originalEndToEndId = paymentTransactionInformation
					.getOrgnlEndToEndId();
			
			String internalCorrelationId = String.format("%s_%s_%s", originalDebtorBic, originalCreationDate, originalEndToEndId);
			String camt053 = objectMapper.writeValueAsString(statement);
			
			String camt053RelativeUrl = "datatables/transaction_details/$.resourceId";
			
			DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
					internalCorrelationId,
					camt053,
					debtorIban,
					paymentTypeCode,
					internalCorrelationId,
					document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getCdtr().getNm(),
					document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getCdtrAcct().getId().getIBAN(),
					null,
					document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getCdtr().getCtctDtls().toString(),
					document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getRmtInf().getUstrd().toString(),
					transactionCategoryPurposeCode);
			
			String camt053Body = objectMapper.writeValueAsString(td);

			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
			String disposalAccountDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "deposit");
			
			String depositAmountOperation = "revertInAms.DisposalAccount.DepositTransactionAmount";
			String depositAmountConfigOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
			paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(depositAmountConfigOperationKey);
			paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(depositAmountConfigOperationKey);
			
			body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			bodyItem = objectMapper.writeValueAsString(body);
			
			batchItemBuilder.add(items, disposalAccountDepositRelativeUrl, bodyItem, false);
			
			td = new DtSavingsTransactionDetails(
					internalCorrelationId,
					camt053,
					debtorIban,
					paymentTypeCode,
					internalCorrelationId,
					document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getCdtr().getNm(),
					document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getCdtrAcct().getId().getIBAN(),
					null,
					document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getCdtr().getCtctDtls().toString(),
					document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getRmtInf().getUstrd().toString(),
					transactionCategoryPurposeCode);
			
			camt053Body = objectMapper.writeValueAsString(td);
			
			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
			doBatch(items, tenantIdentifier, internalCorrelationId);
		} catch (JAXBException | JsonProcessingException e) {
			logger.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}

}
