package org.mifos.connector.ams.zeebe.workers.bookamount;

import com.baasflow.commons.events.Event;
import com.baasflow.commons.events.EventService;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import iso.std.iso._20022.tech.json.camt_053_001.ActiveOrHistoricCurrencyAndAmountRange2.CreditDebitCode;
import iso.std.iso._20022.tech.xsd.pacs_008_001.ContactDetails2;
import iso.std.iso._20022.tech.json.camt_053_001.ReportEntry10;
import lombok.extern.slf4j.Slf4j;
import org.mifos.connector.ams.fineract.Config;
import org.mifos.connector.ams.fineract.ConfigFactory;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.log.LogInternalCorrelationId;
import org.mifos.connector.ams.log.TraceZeebeArguments;
import org.mifos.connector.ams.mapstruct.Pacs008Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.*;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Component
@Slf4j
public class BookCreditedAmountToTechnicalAccountWorker extends AbstractMoneyInOutWorker {

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
    @LogInternalCorrelationId
    @TraceZeebeArguments
    public void bookCreditedAmountToTechnicalAccount(JobClient jobClient,
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
                                                     @Variable String caseIdentifier) {
        log.info("bookCreditedAmountToTechnicalAccount");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "bookCreditedAmountToTechnicalAccount", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> bookCreditedAmountToTechnicalAccount(originalPacs008,
                        amount,
                        tenantIdentifier,
                        paymentScheme,
                        transactionDate,
                        currency,
                        internalCorrelationId,
                        transactionGroupId,
                        transactionCategoryPurposeCode,
                        caseIdentifier,
                        eventBuilder));
    }

    private Void bookCreditedAmountToTechnicalAccount(String originalPacs008,
                                                      String amount,
                                                      String tenantIdentifier,
                                                      String paymentScheme,
                                                      String transactionDate,
                                                      String currency,
                                                      String internalCorrelationId,
                                                      String transactionGroupId,
                                                      String transactionCategoryPurposeCode,
                                                      String caseIdentifier,
                                                      Event.Builder eventBuilder) {
    	try {
            log.info("Incoming money worker started with variables");

            iso.std.iso._20022.tech.xsd.pacs_008_001.Document pacs008 = jaxbUtils.unmarshalPacs008(originalPacs008);

            MDC.put("internalCorrelationId", internalCorrelationId);

            batchItemBuilder.tenantId(tenantIdentifier);
            
            Config technicalAccountConfig = technicalAccountConfigFactory.getConfig(tenantIdentifier);
            
            String taLookup = String.format("%s.%s", paymentScheme, caseIdentifier);
            log.debug("Looking up account id for {}", taLookup);
			Integer recallTechnicalAccountId = technicalAccountConfig.findPaymentTypeIdByOperation(taLookup);
    		
    		String conversionAccountWithdrawalRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), recallTechnicalAccountId, "deposit");
    		
    		Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
    		String configOperationKey = String.format("%s.%s.%s", paymentScheme, "bookToTechnicalAccount", caseIdentifier);
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
    		
    		String camt053RelativeUrl = "datatables/dt_savings_transaction_details /$.resourceId";
    		
    		DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
    				internalCorrelationId,
    				camt053Entry,
    				null,
    				paymentTypeCode,
    				transactionGroupId,
    				pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtr().getNm(),
    				pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtrAcct().getId().getIBAN(),
    				null,
    				Optional.ofNullable(pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getDbtr().getCtctDtls()).map(ContactDetails2::toString).orElse(""),
    				pacs008.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getRmtInf().getUstrd().toString(),
    				transactionCategoryPurposeCode);
    		
    		String camt053Body = objectMapper.writeValueAsString(td);

    		batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);

    		doBatch(items,
                    tenantIdentifier,
                    -1,
                    -1,
                    internalCorrelationId,
                    "bookCreditedAmountToTechnicalAccount");
        } catch (Exception e) {
            log.error("Worker to book incoming money in AMS has failed, dispatching user task to handle conversion account deposit", e);
            throw new ZeebeBpmnError("Error_BookToConversionToBeHandledManually", e.getMessage());
        } finally {
            MDC.remove("internalCorrelationId");
        }
        return null;
    }
}