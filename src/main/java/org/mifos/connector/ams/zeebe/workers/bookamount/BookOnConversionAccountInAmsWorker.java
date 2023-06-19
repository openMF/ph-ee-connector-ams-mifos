package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import jakarta.xml.bind.JAXBException;

import org.mifos.connector.ams.fineract.PaymentTypeConfig;
import org.mifos.connector.ams.fineract.PaymentTypeConfigFactory;
import org.mifos.connector.ams.mapstruct.Pain001Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.BatchItemBuilder;
import org.mifos.connector.ams.zeebe.workers.utils.JAXBUtils;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionBody;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionDetails;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import hu.dpc.rt.utils.converter.Camt056ToCamt053Converter;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import iso.std.iso._20022.tech.json.camt_053_001.BankToCustomerStatementV08;
import iso.std.iso._20022.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;
import iso.std.iso._20022.tech.xsd.camt_056_001.PaymentTransactionInformation31;

@Component
public class BookOnConversionAccountInAmsWorker extends AbstractMoneyInOutWorker {
	
	@Autowired
	private Pain001Camt053Mapper camt053Mapper;
	
	@Value("${fineract.incoming-money-api}")
	protected String incomingMoneyApi;
	
	@Autowired
    private PaymentTypeConfigFactory paymentTypeConfigFactory;
	
	@Autowired
	private JAXBUtils jaxbUtils;
	
	@Autowired
	private BatchItemBuilder batchItemBuilder;
	
	private final ObjectMapper objectMapper = new ObjectMapper();
	
	@JobWorker
	public void bookOnConversionAccountInAms(JobClient jobClient,
			ActivatedJob activatedJob,
			@Variable String originalPain001,
			@Variable String internalCorrelationId,
			@Variable String paymentScheme,
			@Variable String transactionDate,
			@Variable Integer conversionAccountAmsId,
			@Variable String transactionGroupId,
			@Variable String transactionCategoryPurposeCode,
			@Variable String transactionFeeCategoryPurposeCode,
			@Variable BigDecimal amount,
			@Variable BigDecimal transactionFeeAmount,
			@Variable String tenantIdentifier) throws Exception {
		
		Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = objectMapper.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
		
		MDC.put("internalCorrelationId", internalCorrelationId);
		
		logger.info("Starting book debit on fiat account worker");
		
		logger.info("Withdrawing amount {} from conversion account {} of tenant {}", amount, conversionAccountAmsId, tenantIdentifier);
		
		batchItemBuilder.tenantId(tenantIdentifier);
		
		String conversionAccountWithdrawalRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");
		
		PaymentTypeConfig paymentTypeConfig = paymentTypeConfigFactory.getPaymentTypeConfig(tenantIdentifier);
		Integer paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "bookOnConversionAccountInAms.ConversionAccount.WithdrawTransactionAmount"));
		
		TransactionBody body = new TransactionBody(
				transactionDate,
				amount,
				paymentTypeId,
				"",
				FORMAT,
				locale);
		
		String bodyItem = objectMapper.writeValueAsString(body);
		
		List<TransactionItem> items = new ArrayList<>();
		
		batchItemBuilder.add(items, conversionAccountWithdrawalRelativeUrl, bodyItem, false);
	
		BankToCustomerStatementV08 convertedcamt053 = camt053Mapper.toCamt053(pain001.getDocument());
		String camt053 = objectMapper.writeValueAsString(convertedcamt053);
		
		String camt053RelativeUrl = String.format("datatables/transaction_details/%d", conversionAccountAmsId);
		
		TransactionDetails td = new TransactionDetails(
				"$.resourceId",
				internalCorrelationId,
				camt053,
				transactionGroupId,
				transactionCategoryPurposeCode);
		
		String camt053Body = objectMapper.writeValueAsString(td);

		batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
		
		if (!BigDecimal.ZERO.equals(transactionFeeAmount)) {
				
			logger.info("Withdrawing fee {} from conversion account {}", transactionFeeAmount, conversionAccountAmsId);
			
			paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "bookOnConversionAccountInAms.ConversionAccount.WithdrawTransactionFee"));
			
			body = new TransactionBody(
					transactionDate,
					transactionFeeAmount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			bodyItem = objectMapper.writeValueAsString(body);
			
			batchItemBuilder.add(items, conversionAccountWithdrawalRelativeUrl, bodyItem, false);
		
			td = new TransactionDetails(
					"$.resourceId",
					internalCorrelationId,
					camt053,
					transactionGroupId,
					transactionFeeCategoryPurposeCode);
			camt053Body = objectMapper.writeValueAsString(td);
			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
		}
		
		doBatch(items, tenantIdentifier, internalCorrelationId);
		
		logger.info("Book debit on fiat account has finished  successfully");
		
		MDC.remove("internalCorrelationId");
	}
	
	@JobWorker
	public void withdrawTheAmountFromConversionAccountInAms(JobClient client,
			ActivatedJob job,
			@Variable BigDecimal amount,
			@Variable Integer conversionAccountAmsId,
			@Variable String tenantIdentifier,
			@Variable String paymentScheme,
			@Variable String transactionCategoryPurposeCode,
			@Variable String camt056
			) {
		try {
			logger.info("Withdrawing amount {} from conversion account {} of tenant {}", amount, conversionAccountAmsId, tenantIdentifier);
			
			String transactionDate = LocalDate.now().format(DateTimeFormatter.ofPattern(FORMAT));
			
			batchItemBuilder.tenantId(tenantIdentifier);
			
			String conversionAccountWithdrawalRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");
			
			PaymentTypeConfig paymentTypeConfig = paymentTypeConfigFactory.getPaymentTypeConfig(tenantIdentifier);
			Integer paymentTypeId = paymentTypeConfig.findPaymentTypeByOperation(String.format("%s.%s", paymentScheme, "bookOnConversionAccountInAms.ConversionAccount.WithdrawTransactionAmount"));
			
			TransactionBody body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			String bodyItem = objectMapper.writeValueAsString(body);
			
			List<TransactionItem> items = new ArrayList<>();
			
			batchItemBuilder.add(items, conversionAccountWithdrawalRelativeUrl, bodyItem, false);
		
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
			
			String camt053RelativeUrl = String.format("datatables/transaction_details/%d", conversionAccountAmsId);
			
			TransactionDetails td = new TransactionDetails(
					"$.resourceId",
					internalCorrelationId,
					camt053,
					internalCorrelationId,
					transactionCategoryPurposeCode);
			
			String camt053Body = objectMapper.writeValueAsString(td);
	
			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
			doBatch(items, tenantIdentifier, internalCorrelationId);
		} catch (JAXBException | JsonProcessingException e) {
			logger.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}
}
