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
import iso.std.iso._20022.tech.json.camt_053_001.ReportEntry10;
import org.mifos.connector.ams.fineract.Config;
import org.mifos.connector.ams.fineract.ConfigFactory;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.mapstruct.Pacs008Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class BookCreditedAmountToTechnicalAccountWorker extends AbstractMoneyInOutWorker {

    private Logger logger = LoggerFactory.getLogger(getClass());

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
        MDC.put("internalCorrelationId", internalCorrelationId);
        try {
            eventService.auditedEvent(
                    eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "bookCreditedAmountToTechnicalAccount", eventBuilder),
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
        } finally {
            MDC.remove("internalCorrelationId");
        }
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
        logger.info("bookCreditedAmountToTechnicalAccount");
        logger.debug("{} {}", tenantIdentifier, paymentScheme);
        eventBuilder.getCorrelationIds().put("internalCorrelationId", internalCorrelationId);
        eventBuilder.getCorrelationIds().put("transactionGroupId", transactionGroupId);

        try {
            iso.std.iso._20022.tech.xsd.pacs_008_001.Document pacs008 = jaxbUtils.unmarshalPacs008(originalPacs008);

            batchItemBuilder.tenantId(tenantIdentifier);

            Config technicalAccountConfig = technicalAccountConfigFactory.getConfig(tenantIdentifier);

            String taLookup = String.format("%s.%s", paymentScheme, caseIdentifier);
            logger.debug("Looking up account id for {}", taLookup);
            Integer recallTechnicalAccountId = technicalAccountConfig.findByOperation(taLookup);

            String conversionAccountWithdrawalRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), recallTechnicalAccountId, "deposit");

            Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
            Integer paymentTypeId = paymentTypeConfig.findByOperation(String.format("%s.%s.%s", paymentScheme, "bookToTechnicalAccount", caseIdentifier));

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
            String camt053Entry = objectMapper.writeValueAsString(convertedCamt053Entry);

            String camt053RelativeUrl = "datatables/transaction_details/$.resourceId";

            TransactionDetails td = new TransactionDetails(
                    internalCorrelationId,
                    camt053Entry,
                    null,
                    transactionDate,
                    FORMAT,
                    locale,
                    transactionGroupId,
                    transactionCategoryPurposeCode);

            String camt053Body = objectMapper.writeValueAsString(td);

            batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);

            doBatch(items, tenantIdentifier, internalCorrelationId);

        } catch (Exception e) {
            logger.error("Worker to book incoming money in AMS has failed, dispatching user task to handle conversion account deposit", e);
            throw new ZeebeBpmnError("Error_BookToConversionToBeHandledManually", e.getMessage());
        }
        return null;
    }
}