package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.mifos.connector.ams.fineract.Config;
import org.mifos.connector.ams.fineract.ConfigFactory;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.log.LogInternalCorrelationId;
import org.mifos.connector.ams.log.TraceZeebeArguments;
import org.mifos.connector.ams.mapstruct.Pain001Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.BatchItemBuilder;
import org.mifos.connector.ams.zeebe.workers.utils.ContactDetailsUtil;
import org.mifos.connector.ams.zeebe.workers.utils.DtSavingsTransactionDetails;
import org.mifos.connector.ams.zeebe.workers.utils.JAXBUtils;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionBody;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.baasflow.commons.events.Event;
import com.baasflow.commons.events.EventService;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import hu.dpc.rt.utils.converter.Camt056ToCamt053Converter;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import iso.std.iso._20022.tech.json.camt_053_001.ActiveOrHistoricCurrencyAndAmountRange2.CreditDebitCode;
import iso.std.iso._20022.tech.json.camt_053_001.BankToCustomerStatementV08;
import iso.std.iso._20022.tech.json.camt_053_001.ReportEntry10;
import iso.std.iso._20022.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;
import iso.std.iso._20022.tech.xsd.camt_056_001.PaymentTransactionInformation31;
import jakarta.xml.bind.JAXBException;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class BookOnConversionAccountInAmsWorker extends AbstractMoneyInOutWorker {

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
    
    @Autowired
    private ContactDetailsUtil contactDetailsUtil;

    @Autowired
    private EventService eventService;

    @Autowired
    private ObjectMapper objectMapper;

    @JobWorker
    @LogInternalCorrelationId
    @TraceZeebeArguments
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
                                             @Variable String tenantIdentifier,
                                             @Variable String debtorIban) {
        log.info("bookOnConversionAccountInAms");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "bookOnConversionAccountInAms", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> bookOnConversionAccountInAms(originalPain001,
                        internalCorrelationId,
                        paymentScheme,
                        transactionDate,
                        conversionAccountAmsId,
                        transactionGroupId,
                        transactionCategoryPurposeCode,
                        transactionFeeCategoryPurposeCode,
                        amount,
                        transactionFeeAmount,
                        tenantIdentifier,
                        debtorIban,
                        eventBuilder));
    }

    private Void bookOnConversionAccountInAms(String originalPain001,
                                              String internalCorrelationId,
                                              String paymentScheme,
                                              String transactionDate,
                                              Integer conversionAccountAmsId,
                                              String transactionGroupId,
                                              String transactionCategoryPurposeCode,
                                              String transactionFeeCategoryPurposeCode,
                                              BigDecimal amount,
                                              BigDecimal transactionFeeAmount,
                                              String tenantIdentifier,
                                              String debtorIban,
                                              Event.Builder eventBuilder) {
    	try {
    		transactionDate = transactionDate.replaceAll("-", "");
		
    		objectMapper.setSerializationInclusion(Include.NON_NULL);
		
    		Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001;
			pain001 = objectMapper.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
		
			MDC.put("internalCorrelationId", internalCorrelationId);
			
			log.info("Starting book debit on conversion account worker");
			
			log.info("Withdrawing amount {} from conversion account {} of tenant {}", amount, conversionAccountAmsId, tenantIdentifier);
			
			batchItemBuilder.tenantId(tenantIdentifier);
			
			String conversionAccountWithdrawalRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");
			
			Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
			String withdrawAmountOperation = "bookOnConversionAccountInAms.ConversionAccount.WithdrawTransactionAmount";
			String configOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
			Integer paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(configOperationKey);
			String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(configOperationKey);
			
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
		
			ReportEntry10 convertedcamt053Entry = camt053Mapper.toCamt053Entry(pain001.getDocument());
			convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.DBIT);
			String camt053Entry = objectMapper.writeValueAsString(convertedcamt053Entry);
			
			String camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";
			
			DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
					internalCorrelationId,
					camt053Entry,
					pain001.getDocument().getPaymentInformation().get(0).getDebtorAccount().getIdentification().getIban(),
					paymentTypeCode,
					transactionGroupId,
					pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getName(),
					pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditorAccount().getIdentification().getIban(),
					null,
					contactDetailsUtil.getId(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getContactDetails()),
					Optional.ofNullable(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getRemittanceInformation())
							.map(iso.std.iso._20022.tech.json.pain_001_001.RemittanceInformation16::getUnstructured).map(List::toString).orElse(""),
					transactionCategoryPurposeCode);
			
			String camt053Body = objectMapper.writeValueAsString(td);
	
			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
			if (!BigDecimal.ZERO.equals(transactionFeeAmount)) {
					
				log.info("Withdrawing fee {} from conversion account {}", transactionFeeAmount, conversionAccountAmsId);
				
				String withdrawFeeOperation = "bookOnConversionAccountInAms.ConversionAccount.WithdrawTransactionFee";
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
				
				batchItemBuilder.add(items, conversionAccountWithdrawalRelativeUrl, bodyItem, false);
			
				td = new DtSavingsTransactionDetails(
						internalCorrelationId,
						camt053Entry,
						pain001.getDocument().getPaymentInformation().get(0).getDebtorAccount().getIdentification().getIban(),
						paymentTypeCode,
						transactionGroupId,
						pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getName(),
						pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditorAccount().getIdentification().getIban(),
						null,
						contactDetailsUtil.getId(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getContactDetails()),
						Optional.ofNullable(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getRemittanceInformation())
								.map(iso.std.iso._20022.tech.json.pain_001_001.RemittanceInformation16::getUnstructured).map(List::toString).orElse(""),
						transactionFeeCategoryPurposeCode);
				camt053Body = objectMapper.writeValueAsString(td);
				batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			}
			
			doBatch(items,
	                tenantIdentifier,
	                -1,
	                conversionAccountAmsId,
	                internalCorrelationId,
	                "bookOnConversionAccountInAms");
			
			log.info("Book debit on conversion account has finished  successfully");
			
			MDC.remove("internalCorrelationId");
    	} catch (JsonProcessingException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}

        return null;
    }

    @JobWorker
    @TraceZeebeArguments
    public void withdrawTheAmountFromConversionAccountInAms(JobClient client,
                                                            ActivatedJob activatedJob,
                                                            @Variable BigDecimal amount,
                                                            @Variable Integer conversionAccountAmsId,
                                                            @Variable String tenantIdentifier,
                                                            @Variable String paymentScheme,
                                                            @Variable String transactionCategoryPurposeCode,
                                                            @Variable String camt056,
                                                            @Variable String debtorIban) {
        log.info("withdrawTheAmountFromConversionAccountInAms");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob,
                        "withdrawTheAmountFromConversionAccountInAms",
                        null,
                        null,
                        eventBuilder),
                eventBuilder -> withdrawTheAmountFromConversionAccountInAms(amount,
                        conversionAccountAmsId,
                        tenantIdentifier,
                        paymentScheme,
                        transactionCategoryPurposeCode,
                        camt056,
                        debtorIban,
                        eventBuilder));
    }

    private Void withdrawTheAmountFromConversionAccountInAms(BigDecimal amount,
                                                             Integer conversionAccountAmsId,
                                                             String tenantIdentifier,
                                                             String paymentScheme,
                                                             String transactionCategoryPurposeCode,
                                                             String camt056,
                                                             String debtorIban,
                                                             Event.Builder eventBuilder) {
    	try {
			log.info("Withdrawing amount {} from conversion account {} of tenant {}", amount, conversionAccountAmsId, tenantIdentifier);
			
			String transactionDate = LocalDate.now().format(DateTimeFormatter.ofPattern(FORMAT));
			
			batchItemBuilder.tenantId(tenantIdentifier);
			
			String conversionAccountWithdrawalRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");
			
			Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
			String withdrawAmountOperation = "bookOnConversionAccountInAms.ConversionAccount.WithdrawTransactionAmount";
			String configOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
			Integer paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(configOperationKey);
			String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(configOperationKey);
			
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
			
			String camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";
			
			DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
					internalCorrelationId,
					camt053,
					document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getCdtrAcct().getId().getIBAN(),
					paymentTypeCode,
					internalCorrelationId,
					document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getDbtr().getNm(),
					document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getDbtrAcct().getId().getIBAN(),
					null,
					contactDetailsUtil.getId(document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getDbtr().getCtctDtls()),
					Optional.ofNullable(document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getRmtInf())
							.map(iso.std.iso._20022.tech.xsd.camt_056_001.RemittanceInformation5::getUstrd).map(List::toString).orElse(""),
					transactionCategoryPurposeCode);
			
			String camt053Body = objectMapper.writeValueAsString(td);
	
			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
			doBatch(items,
                    tenantIdentifier,
                    -1,
                    conversionAccountAmsId,
                    internalCorrelationId,
                    "withdrawTheAmountFromConversionAccountInAms");
		} catch (JAXBException | JsonProcessingException e) {
			log.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}
        return null;
    }
}