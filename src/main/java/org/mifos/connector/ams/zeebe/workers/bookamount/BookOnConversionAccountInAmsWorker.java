package org.mifos.connector.ams.zeebe.workers.bookamount;

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
import iso.std.iso._20022.tech.json.camt_053_001.BankToCustomerStatementV08;
import iso.std.iso._20022.tech.json.camt_053_001.ReportEntry10;
import iso.std.iso._20022.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;
import iso.std.iso._20022.tech.xsd.camt_056_001.PaymentTransactionInformation31;
import jakarta.xml.bind.JAXBException;
import org.mifos.connector.ams.fineract.Config;
import org.mifos.connector.ams.fineract.ConfigFactory;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.mapstruct.Pain001Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.*;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Component
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
    private EventService eventService;

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
                                             @Variable String tenantIdentifier,
                                             @Variable String debtorIban) {
        MDC.put("internalCorrelationId", internalCorrelationId);
        try {
            eventService.auditedEvent(
                    eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "bookOnConversionAccountInAms", eventBuilder),
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
        } finally {
            MDC.remove("internalCorrelationId");
        }
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
        logger.info("bookOnConversionAccountInAms");
        logger.debug("Withdrawing amount {} from conversion account {} of tenant {}", amount, conversionAccountAmsId, tenantIdentifier);
        eventBuilder.getCorrelationIds().put("internalCorrelationId", internalCorrelationId);
        eventBuilder.getCorrelationIds().put("transactionGroupId", transactionGroupId);

        transactionDate = transactionDate.replaceAll("-", "");

        objectMapper.setSerializationInclusion(Include.NON_NULL);

        try {
            Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = objectMapper.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);

            batchItemBuilder.tenantId(tenantIdentifier);

            String conversionAccountWithdrawalRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");

            Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
            Integer paymentTypeId = paymentTypeConfig.findByOperation(String.format("%s.%s", paymentScheme, "bookOnConversionAccountInAms.ConversionAccount.WithdrawTransactionAmount"));

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
            String camt053Entry = objectMapper.writeValueAsString(convertedcamt053Entry);

            String camt053RelativeUrl = "datatables/transaction_details/$.resourceId";

            TransactionDetails td = new TransactionDetails(
                    internalCorrelationId,
                    camt053Entry,
                    debtorIban,
                    transactionDate,
                    FORMAT,
                    locale,
                    transactionGroupId,
                    transactionCategoryPurposeCode);

            String camt053Body = objectMapper.writeValueAsString(td);

            batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);

            if (!BigDecimal.ZERO.equals(transactionFeeAmount)) {

                logger.info("Withdrawing fee {} from conversion account {}", transactionFeeAmount, conversionAccountAmsId);

                paymentTypeId = paymentTypeConfig.findByOperation(String.format("%s.%s", paymentScheme, "bookOnConversionAccountInAms.ConversionAccount.WithdrawTransactionFee"));

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
                        internalCorrelationId,
                        camt053Entry,
                        debtorIban,
                        transactionDate,
                        FORMAT,
                        locale,
                        transactionGroupId,
                        transactionFeeCategoryPurposeCode);
                camt053Body = objectMapper.writeValueAsString(td);
                batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
            }

            doBatch(items, tenantIdentifier, internalCorrelationId);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("failed to create camt.053", e);
        }

        return null;
    }

    @JobWorker
    public void withdrawTheAmountFromConversionAccountInAms(JobClient client,
                                                            ActivatedJob activatedJob,
                                                            @Variable BigDecimal amount,
                                                            @Variable Integer conversionAccountAmsId,
                                                            @Variable String tenantIdentifier,
                                                            @Variable String paymentScheme,
                                                            @Variable String transactionCategoryPurposeCode,
                                                            @Variable String camt056,
                                                            @Variable String debtorIban) {
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "withdrawTheAmountFromConversionAccountInAms", eventBuilder),
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
        logger.info("withdrawTheAmountFromConversionAccountInAms");
        logger.debug("Withdrawing amount {} from conversion account {} of tenant {}", amount, conversionAccountAmsId, tenantIdentifier);

        try {

            String transactionDate = LocalDate.now().format(DateTimeFormatter.ofPattern(FORMAT));

            batchItemBuilder.tenantId(tenantIdentifier);

            String conversionAccountWithdrawalRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");

            Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
            Integer paymentTypeId = paymentTypeConfig.findByOperation(String.format("%s.%s", paymentScheme, "bookOnConversionAccountInAms.ConversionAccount.WithdrawTransactionAmount"));

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

            String camt053RelativeUrl = "datatables/transaction_details/$.resourceId";

            TransactionDetails td = new TransactionDetails(
                    internalCorrelationId,
                    camt053,
                    debtorIban,
                    transactionDate,
                    FORMAT,
                    locale,
                    internalCorrelationId,
                    transactionCategoryPurposeCode);

            String camt053Body = objectMapper.writeValueAsString(td);

            batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);

            doBatch(items, tenantIdentifier, internalCorrelationId);
        } catch (JAXBException | JsonProcessingException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
        return null;
    }
}