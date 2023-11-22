package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.mifos.connector.ams.fineract.Config;
import org.mifos.connector.ams.fineract.ConfigFactory;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.log.LogInternalCorrelationId;
import org.mifos.connector.ams.log.TraceZeebeArguments;
import org.mifos.connector.ams.mapstruct.Pacs008Camt053Mapper;
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
import com.fasterxml.jackson.databind.ObjectMapper;

import hu.dpc.rt.utils.converter.Pacs004ToCamt053Converter;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import iso.std.iso._20022.tech.json.camt_053_001.ActiveOrHistoricCurrencyAndAmountRange2.CreditDebitCode;
import iso.std.iso._20022.tech.json.camt_053_001.AccountStatement9;
import iso.std.iso._20022.tech.json.camt_053_001.BankToCustomerStatementV08;
import iso.std.iso._20022.tech.json.camt_053_001.EntryDetails9;
import iso.std.iso._20022.tech.json.camt_053_001.EntryTransaction10;
import iso.std.iso._20022.tech.json.camt_053_001.ReportEntry10;
import iso.std.iso._20022.tech.xsd.pacs_008_001.RemittanceInformation5;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class TransferToDisposalAccountWorker extends AbstractMoneyInOutWorker {

    @Autowired
    private Pacs008Camt053Mapper pacs008Camt053Mapper;
    
    private Pacs004ToCamt053Converter pacs004Camt053Mapper = new Pacs004ToCamt053Converter();

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
                                          @Variable String creditorIban) {
        log.info("transferToDisposalAccount");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "transferToDisposalAccount", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> transferToDisposalAccount(originalPacs008,
                        internalCorrelationId,
                        paymentScheme,
                        transactionDate,
                        transactionGroupId,
                        transactionCategoryPurposeCode,
                        amount,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        tenantIdentifier,
                        creditorIban,
                        eventBuilder));
    }

    private Void transferToDisposalAccount(String originalPacs008,
                                           String internalCorrelationId,
                                           String paymentScheme,
                                           String transactionDate,
                                           String transactionGroupId,
                                           String transactionCategoryPurposeCode,
                                           BigDecimal amount,
                                           Integer conversionAccountAmsId,
                                           Integer disposalAccountAmsId,
                                           String tenantIdentifier,
                                           String creditorIban,
                                           Event.Builder eventBuilder) {
    	try {
			MDC.put("internalCorrelationId", internalCorrelationId);
			log.info("transfer to disposal account in payment (pacs.008) {} started for {} on {} ", internalCorrelationId, paymentScheme, tenantIdentifier);

			iso.std.iso._20022.tech.xsd.pacs_008_001.Document pacs008 = jaxbUtils.unmarshalPacs008(originalPacs008);

			objectMapper.setSerializationInclusion(Include.NON_NULL);
			
			batchItemBuilder.tenantId(tenantIdentifier);
			
			Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
			
			List<TransactionItem> items = new ArrayList<>();
			
			String disposalAccountDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "deposit");
			String depositAmountOperation = "transferToDisposalAccount.DisposalAccount.DepositTransactionAmount";
			String depositAmountConfigOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
			var paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(depositAmountConfigOperationKey);
			var paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(depositAmountConfigOperationKey);
			
			var body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			var bodyItem = objectMapper.writeValueAsString(body);
			
			batchItemBuilder.add(items, disposalAccountDepositRelativeUrl, bodyItem, false);
			
			ReportEntry10 convertedCamt053Entry = pacs008Camt053Mapper.toCamt053Entry(pacs008).getStatement().get(0).getEntry().get(0);
			convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.CRDT);
			convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
			convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
			String camt053Entry = objectMapper.writeValueAsString(convertedCamt053Entry);
			
			var td = new DtSavingsTransactionDetails(
					internalCorrelationId,
					camt053Entry,
					pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getCdtrAcct().getId().getIBAN(),
					paymentTypeCode,
					transactionGroupId,
					pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtr().getNm(),
					pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtrAcct().getId().getIBAN(),
					null,
					contactDetailsUtil.getId(pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtr().getCtctDtls()),
					Optional.ofNullable(pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getRmtInf()).map(RemittanceInformation5::getUstrd).map(List::toString).orElse(""),
					transactionCategoryPurposeCode,
					paymentScheme,
					conversionAccountAmsId,
					disposalAccountAmsId);
			
			var camt053Body = objectMapper.writeValueAsString(td);
			
			String camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";
			
			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
			String conversionAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");
			
			String withdrawAmountOperation = "transferToDisposalAccount.ConversionAccount.WithdrawTransactionAmount";
			String withdrawAmountConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
			paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawAmountConfigOperationKey);
			paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawAmountConfigOperationKey);
			convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
			camt053Entry = objectMapper.writeValueAsString(convertedCamt053Entry);
			
			body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			bodyItem = objectMapper.writeValueAsString(body);
			
			batchItemBuilder.add(items, conversionAccountWithdrawRelativeUrl, bodyItem, false);
			
			td = new DtSavingsTransactionDetails(
					internalCorrelationId,
					camt053Entry,
					pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getCdtrAcct().getId().getIBAN(),
					paymentTypeCode,
					transactionGroupId,
					pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtr().getNm(),
					pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtrAcct().getId().getIBAN(),
					null,
					contactDetailsUtil.getId(pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtr().getCtctDtls()),
					Optional.ofNullable(pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getRmtInf()).map(RemittanceInformation5::getUstrd).map(List::toString).orElse(""),
					transactionCategoryPurposeCode,
					paymentScheme,
					conversionAccountAmsId,
					disposalAccountAmsId);
			
			camt053Body = objectMapper.writeValueAsString(td);

			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
			doBatch(items,
                    tenantIdentifier,
                    disposalAccountAmsId,
                    conversionAccountAmsId,
                    internalCorrelationId,
                    "transferToDisposalAccount");
			
			log.info("Exchange to disposal worker has finished successfully");
		} catch (Exception e) {
			log.error("Exchange to disposal worker has failed, dispatching user task to handle exchange", e);
			throw new ZeebeBpmnError("Error_TransferToDisposalToBeHandledManually", e.getMessage());
		} finally {
			MDC.remove("internalCorrelationId");
		}

        return null;
    }

    @JobWorker
    @LogInternalCorrelationId
    @TraceZeebeArguments
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
                                                  @Variable String creditorIban) {
        log.info("transferToDisposalAccountInRecall");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "transferToDisposalAccountInRecall", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> transferToDisposalAccountInRecall(originalPacs008,
                        internalCorrelationId,
                        paymentScheme,
                        transactionDate,
                        transactionGroupId,
                        transactionCategoryPurposeCode,
                        amount,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        tenantIdentifier,
                        pacs004,
                        creditorIban,
                        eventBuilder));
    }

    private Void transferToDisposalAccountInRecall(String originalPacs008,
                                                   String internalCorrelationId,
                                                   String paymentScheme,
                                                   String transactionDate,
                                                   String transactionGroupId,
                                                   String transactionCategoryPurposeCode,
                                                   BigDecimal amount,
                                                   Integer conversionAccountAmsId,
                                                   Integer disposalAccountAmsId,
                                                   String tenantIdentifier,
                                                   String originalPacs004,
                                                   String creditorIban,
                                                   Event.Builder eventBuilder) {
    	try {
			MDC.put("internalCorrelationId", internalCorrelationId);
			log.info("transfer to disposal account in recall (pacs.004) {} started for {} on {} ", internalCorrelationId, paymentScheme, tenantIdentifier);

			iso.std.iso._20022.tech.xsd.pacs_008_001.Document pacs008 = jaxbUtils.unmarshalPacs008(originalPacs008);
			
			iso.std.iso._20022.tech.xsd.pacs_004_001.Document pacs004 = jaxbUtils.unmarshalPacs004(originalPacs004);
		
			objectMapper.setSerializationInclusion(Include.NON_NULL);
			
			batchItemBuilder.tenantId(tenantIdentifier);
			
			Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
			
			String disposalAccountDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "deposit");
			String depositAmountOperation = "transferToDisposalAccountInRecall.DisposalAccount.DepositTransactionAmount";
			String depositAmountConfigOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
			var paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(depositAmountConfigOperationKey);
			var paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(depositAmountConfigOperationKey);
			
			var body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			var bodyItem = objectMapper.writeValueAsString(body);
			
			List<TransactionItem> items = new ArrayList<>();
			
			batchItemBuilder.add(items, disposalAccountDepositRelativeUrl, bodyItem, false);
			
			BankToCustomerStatementV08 intermediateCamt053 = pacs008Camt053Mapper.toCamt053Entry(pacs008);
			ReportEntry10 convertedCamt053Entry = pacs004Camt053Mapper.convert(pacs004, intermediateCamt053).getStatement().get(0).getEntry().get(0);
			convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.CRDT);
			convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
			String camt053Entry = objectMapper.writeValueAsString(convertedCamt053Entry);
			
			var td = new DtSavingsTransactionDetails(
					internalCorrelationId,
					camt053Entry,
					pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getCdtrAcct().getId().getIBAN(),
					paymentTypeCode,
					transactionGroupId,
					pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtr().getNm(),
					pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtrAcct().getId().getIBAN(),
					null,
					contactDetailsUtil.getId(pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtr().getCtctDtls()),
					Optional.ofNullable(pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getRmtInf()).map(RemittanceInformation5::getUstrd).map(List::toString).orElse(""),
					transactionCategoryPurposeCode,
					paymentScheme,
					conversionAccountAmsId,
					disposalAccountAmsId);
			
			var camt053Body = objectMapper.writeValueAsString(td);
			
			String camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";
			
			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
			String conversionAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");
			
			String withdrawAmountOperation = "transferToDisposalAccountInRecall.ConversionAccount.WithdrawTransactionAmount";
			String withdrawAmountConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
			paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawAmountConfigOperationKey);
			paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawAmountConfigOperationKey);
			convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
			camt053Entry = objectMapper.writeValueAsString(convertedCamt053Entry);
			
			body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);
			
			bodyItem = objectMapper.writeValueAsString(body);
			
			batchItemBuilder.add(items, conversionAccountWithdrawRelativeUrl, bodyItem, false);
			
			td = new DtSavingsTransactionDetails(
					internalCorrelationId,
					camt053Entry,
					pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getCdtrAcct().getId().getIBAN(),
					paymentTypeCode,
					transactionGroupId,
					pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtr().getNm(),
					pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtrAcct().getId().getIBAN(),
					null,
					contactDetailsUtil.getId(pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtr().getCtctDtls()),
					Optional.ofNullable(pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getRmtInf()).map(RemittanceInformation5::getUstrd).map(List::toString).orElse(""),
					transactionCategoryPurposeCode,
					paymentScheme,
					conversionAccountAmsId,
					disposalAccountAmsId);
			
			camt053Body = objectMapper.writeValueAsString(td);

			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
			doBatch(items,
                    tenantIdentifier,
                    disposalAccountAmsId,
                    conversionAccountAmsId,
                    internalCorrelationId,
                    "transferToDisposalAccountInRecall");
			
			log.info("Exchange to disposal worker has finished successfully");
		} catch (Exception e) {
			log.error("Exchange to disposal worker has failed, dispatching user task to handle exchange", e);
			throw new ZeebeBpmnError("Error_TransferToDisposalToBeHandledManually", e.getMessage());
		} finally {
			MDC.remove("internalCorrelationId");
		}

        return null;
    }

    @JobWorker
    @LogInternalCorrelationId
    @TraceZeebeArguments
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
                                                  @Variable String creditorIban) {
        log.info("transferToDisposalAccountInReturn");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "transferToDisposalAccountInReturn", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> transferToDisposalAccountInReturn(pacs004,
                        internalCorrelationId,
                        paymentScheme,
                        transactionDate,
                        transactionGroupId,
                        transactionCategoryPurposeCode,
                        amount,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        tenantIdentifier,
                        creditorIban,
                        eventBuilder));
    }

    private Void transferToDisposalAccountInReturn(String pacs004,
                                                   String internalCorrelationId,
                                                   String paymentScheme,
                                                   String transactionDate,
                                                   String transactionGroupId,
                                                   String transactionCategoryPurposeCode,
                                                   BigDecimal amount,
                                                   Integer conversionAccountAmsId,
                                                   Integer disposalAccountAmsId,
                                                   String tenantIdentifier,
                                                   String creditorIban,
                                                   Event.Builder eventBuilder) {
    	try {
			MDC.put("internalCorrelationId", internalCorrelationId);
			log.info("transfer to disposal account in return (pacs.004) {} started for {} on {} ", internalCorrelationId, paymentScheme, tenantIdentifier);

			batchItemBuilder.tenantId(tenantIdentifier);
			
			String disposalAccountDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "deposit");
			String depositAmountOperation = "transferToDisposalAccountInReturn.DisposalAccount.DepositTransactionAmount";
			String depositAmountConfigOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
			Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
			var paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(depositAmountConfigOperationKey);
			var paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(depositAmountConfigOperationKey);

			var body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);

			var bodyItem = objectMapper.writeValueAsString(body);
			
			List<TransactionItem> items = new ArrayList<>();

			batchItemBuilder.add(items, disposalAccountDepositRelativeUrl, bodyItem, false);

			var camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";
			
			iso.std.iso._20022.tech.xsd.pacs_004_001.Document pacs_004 = jaxbUtils.unmarshalPacs004(pacs004);
			BankToCustomerStatementV08 camt053 = pacs004Camt053Mapper.convert(pacs_004, 
            		new BankToCustomerStatementV08()
    				.withStatement(List.of(new AccountStatement9()
    						.withEntry(List.of(new ReportEntry10()
    								.withEntryDetails(List.of(new EntryDetails9()
    										.withTransactionDetails(List.of(new EntryTransaction10())))))))));
			ReportEntry10 convertedCamt053Entry = camt053.getStatement().get(0).getEntry().get(0);
			convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);

			String camt053Entry = objectMapper.writeValueAsString(convertedCamt053Entry);
			

			var td = new DtSavingsTransactionDetails(
					internalCorrelationId,
					camt053Entry,
					pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getDbtrAcct().getId().getIBAN(),
					paymentTypeCode,
					transactionGroupId,
					pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getCdtr().getNm(),
					pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getCdtrAcct().getId().getIBAN(),
					null,
					contactDetailsUtil.getId(pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getCdtr().getCtctDtls()),
					Optional.ofNullable(pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getRmtInf())
                    		.map(iso.std.iso._20022.tech.xsd.pacs_004_001.RemittanceInformation5::getUstrd).map(List::toString).orElse(""),
					transactionCategoryPurposeCode,
					paymentScheme,
					conversionAccountAmsId,
					disposalAccountAmsId);

			var camt053Body = objectMapper.writeValueAsString(td);

			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);

			String conversionAccountWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");

			String withdrawAmountOperation = "transferToDisposalAccountInReturn.ConversionAccount.WithdrawTransactionAmount";
			String withdrawAmountConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
			paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawAmountConfigOperationKey);
			paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawAmountConfigOperationKey);
			
			convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
			camt053Entry = objectMapper.writeValueAsString(convertedCamt053Entry);
			
			body = new TransactionBody(
					transactionDate,
					amount,
					paymentTypeId,
					"",
					FORMAT,
					locale);

			bodyItem = objectMapper.writeValueAsString(body);

			batchItemBuilder.add(items, conversionAccountWithdrawRelativeUrl, bodyItem, false);

			camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";

			td = new DtSavingsTransactionDetails(
					internalCorrelationId,
					camt053Entry,
					pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getDbtrAcct().getId().getIBAN(),
					paymentTypeCode,
					transactionGroupId,
					pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getCdtr().getNm(),
					pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getCdtrAcct().getId().getIBAN(),
					null,
					contactDetailsUtil.getId(pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getCdtr().getCtctDtls()),
					Optional.ofNullable(pacs_004.getPmtRtr().getTxInf().get(0).getOrgnlTxRef().getRmtInf())
                    		.map(iso.std.iso._20022.tech.xsd.pacs_004_001.RemittanceInformation5::getUstrd).map(List::toString).orElse(""),
					transactionCategoryPurposeCode,
					paymentScheme,
					conversionAccountAmsId,
					disposalAccountAmsId);

			camt053Body = objectMapper.writeValueAsString(td);

			batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);


			doBatch(items,
                    tenantIdentifier,
                    disposalAccountAmsId,
                    conversionAccountAmsId,
                    internalCorrelationId,
                    "transferToDisposalAccountInReturn");

			log.info("Exchange to disposal worker has finished successfully");
		} catch (Exception e) {
			log.error("Exchange to disposal worker has failed, dispatching user task to handle exchange", e);
			throw new ZeebeBpmnError("Error_TransferToDisposalToBeHandledManually", e.getMessage());
		} finally {
			MDC.remove("internalCorrelationId");
		}

        return null;
    }
}