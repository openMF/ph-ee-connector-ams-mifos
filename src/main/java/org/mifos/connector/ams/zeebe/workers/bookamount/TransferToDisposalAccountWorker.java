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
import org.mifos.connector.ams.log.LogInternalCorrelationId;
import org.mifos.connector.ams.log.TraceZeebeArguments;
import org.mifos.connector.ams.mapstruct.Pacs008Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

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

    @Autowired
    private EventService eventService;

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
        logger.info("transferToDisposalAccount");
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

        } catch (Exception e) {
            // TODO technical error handling
            logger.error("Exchange to disposal worker has failed, dispatching user task to handle exchange", e);
            throw new ZeebeBpmnError("Error_TransferToDisposalToBeHandledManually", e.getMessage());
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
        logger.info("transferToDisposalAccountInRecall");
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
                                                   String pacs004,
                                                   String creditorIban,
                                                   Event.Builder eventBuilder) {
        try {
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

        } catch (Exception e) {
            // TODO technical error handling
            logger.error("Exchange to disposal worker has failed, dispatching user task to handle exchange", e);
            throw new ZeebeBpmnError("Error_TransferToDisposalToBeHandledManually", e.getMessage());
        }
        return null;
    }
}