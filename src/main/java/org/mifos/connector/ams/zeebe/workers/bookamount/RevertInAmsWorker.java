package org.mifos.connector.ams.zeebe.workers.bookamount;

import com.baasflow.commons.events.EventLogLevel;
import com.baasflow.commons.events.EventService;
import com.baasflow.commons.events.EventType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import hu.dpc.rt.utils.converter.Pacs004ToCamt053Converter;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import iso.std.iso._20022.tech.json.camt_053_001.*;
import iso.std.iso._20022.tech.json.camt_053_001.ActiveOrHistoricCurrencyAndAmountRange2.CreditDebitCode;
import iso.std.iso._20022.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;
import iso.std.iso._20022.tech.xsd.pacs_004_001.PaymentTransactionInformation27;
import jakarta.xml.bind.JAXBException;
import lombok.extern.slf4j.Slf4j;
import org.mifos.connector.ams.fineract.Config;
import org.mifos.connector.ams.fineract.ConfigFactory;
import org.mifos.connector.ams.fineract.savingsaccounttransaction.request.*;
import org.mifos.connector.ams.fineract.savingsaccounttransaction.response.TransactionQueryContent;
import org.mifos.connector.ams.fineract.savingsaccounttransaction.response.TransactionQueryPayload;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.log.LogInternalCorrelationId;
import org.mifos.connector.ams.log.TraceZeebeArguments;
import org.mifos.connector.ams.mapstruct.Pain001Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.*;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import javax.xml.datatype.DatatypeFactory;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Component
@Slf4j
public class RevertInAmsWorker extends AbstractMoneyInOutWorker {

    @Autowired
    Pain001Camt053Mapper pain001Camt053Mapper;

    private Pacs004ToCamt053Converter pacs004Camt053Mapper = new Pacs004ToCamt053Converter();

    @Value("${fineract.incoming-money-api}")
    protected String incomingMoneyApi;

    @Autowired
    ConfigFactory paymentTypeConfigFactory;

    @Autowired
    private JAXBUtils jaxbUtils;

    @Autowired
    BatchItemBuilder batchItemBuilder;

    @Autowired
    ContactDetailsUtil contactDetailsUtil;

    @Autowired
    AuthTokenHelper authTokenHelper;

    @Autowired
    EventService eventService;

    @Autowired
    @Qualifier("painMapper")
    private ObjectMapper painMapper;

    @JobWorker
    @LogInternalCorrelationId
    @TraceZeebeArguments
    public Map<String, Object> revertInAms(JobClient jobClient,
                                           ActivatedJob activatedJob,
                                           @Variable String internalCorrelationId,
                                           @Variable String transactionFeeInternalCorrelationId,
                                           @Variable String originalPain001,
                                           @Variable String conversionAccountAmsId,
                                           @Variable String disposalAccountAmsId,
                                           @Variable String transactionDate,
                                           @Variable String paymentScheme,
                                           @Variable String transactionGroupId,
                                           @Variable String transactionCategoryPurposeCode,
                                           @Variable BigDecimal amount,
                                           @Variable String transactionFeeCategoryPurposeCode,
                                           @Variable BigDecimal transactionFeeAmount,
                                           @Variable String tenantIdentifier,
                                           @Variable String accountProductType
    ) {
        log.info("revertInAms");
        return eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "revertInAms", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> revertInAms(internalCorrelationId,
                        transactionFeeInternalCorrelationId,
                        originalPain001,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        transactionDate,
                        paymentScheme,
                        transactionGroupId,
                        transactionCategoryPurposeCode,
                        amount,
                        transactionFeeCategoryPurposeCode,
                        transactionFeeAmount,
                        tenantIdentifier,
                        accountProductType
                ));
    }

    Map<String, Object> revertInAms(String internalCorrelationId,
                                    String transactionFeeInternalCorrelationId,
                                    String originalPain001,
                                    String conversionAccountAmsId,
                                    String disposalAccountAmsId,
                                    String hyphenatedTransactionDate,
                                    String paymentScheme,
                                    String transactionGroupId,
                                    String transactionCategoryPurposeCode,
                                    BigDecimal amount,
                                    String transactionFeeCategoryPurposeCode,
                                    BigDecimal transactionFeeAmount,
                                    String tenantIdentifier,
                                    String accountProductType
    ) {
        try {
            MDC.put("internalCorrelationId", internalCorrelationId);

            String transactionDate = hyphenatedTransactionDate.replaceAll("-", "");

            Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = painMapper.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);

            log.debug("Withdrawing amount {} from conversion account {}", amount, conversionAccountAmsId);

            List<TransactionItem> items = new ArrayList<>();

            log.debug("Re-depositing amount {} in disposal account {}", amount, disposalAccountAmsId);

            String disposalAccountDepositRelativeUrl = String.format("%s%s/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "deposit");

            Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);

            String depositAmountOperation = "revertInAms.DisposalAccount.DepositTransactionAmount";
            String depositAmountConfigOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
            var paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(depositAmountConfigOperationKey);
            var paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(depositAmountConfigOperationKey);

            var body = new TransactionBody(transactionDate, amount, paymentTypeId, "", FORMAT, locale);
            var bodyItem = painMapper.writeValueAsString(body);
            batchItemBuilder.add(tenantIdentifier, items, disposalAccountDepositRelativeUrl, bodyItem, false);

            ReportEntry10 savingsAccountsCamt053Entry = null;
            EntryTransaction10 camt053Fragment;
            String camt053Entry;
            if ("CURRENT".equalsIgnoreCase(accountProductType)) {
                log.debug("Current account - mapping to Camt053 fragment");
                camt053Fragment = pain001Camt053Mapper.toCamt053Fragment(pain001.getDocument());
                camt053Fragment.setCreditDebitIndicator(CreditDebitCode.CRDT);
                // do not set AdditionalTransactionInformation for current account
            } else {
                log.debug("Savings account - mapping to Camt053 entry");
                BankToCustomerStatementV08 convertedStatement = pain001Camt053Mapper.toCamt053Entry(pain001.getDocument());
                savingsAccountsCamt053Entry = convertedStatement.getStatement().get(0).getEntry().get(0);
                camt053Fragment = savingsAccountsCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0);
                camt053Fragment.setCreditDebitIndicator(CreditDebitCode.CRDT);
                savingsAccountsCamt053Entry.setCreditDebitIndicator(CreditDebitCode.CRDT);
                savingsAccountsCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
                camt053Fragment.setAdditionalTransactionInformation(paymentTypeCode);
            }
            camt053Entry = serializeCamt053orFragment(accountProductType, camt053Fragment, savingsAccountsCamt053Entry);

            var td = new DtSavingsTransactionDetails(
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
                    transactionCategoryPurposeCode,
                    paymentScheme,
                    conversionAccountAmsId,
                    disposalAccountAmsId,
                    pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getPaymentIdentification().getEndToEndIdentification());

            var camt053Body = painMapper.writeValueAsString(td);

            String camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";

            batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);

            if (!BigDecimal.ZERO.equals(transactionFeeAmount)) {
                log.debug("Re-depositing fee {} in disposal account {}", transactionFeeAmount, disposalAccountAmsId);

                String depositFeeOperation = "revertInAms.DisposalAccount.DepositTransactionFee";
                String depositFeeConfigOperationKey = String.format("%s.%s", paymentScheme, depositFeeOperation);
                paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(depositFeeConfigOperationKey);
                paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(depositFeeConfigOperationKey);
                if ("CURRENT".equalsIgnoreCase(accountProductType)) {
                    // do not set AdditionalTransactionInformation for current account
                } else {
                    camt053Fragment.setAdditionalTransactionInformation(paymentTypeCode);
                }
                camt053Fragment.getSupplementaryData().clear();
                pain001Camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001.getDocument(), camt053Fragment, transactionFeeCategoryPurposeCode);
                pain001Camt053Mapper.refillOtherIdentification(pain001.getDocument(), camt053Fragment);
                camt053Entry = serializeCamt053orFragment(accountProductType, camt053Fragment, savingsAccountsCamt053Entry);

                body = new TransactionBody(transactionDate, transactionFeeAmount, paymentTypeId, "", FORMAT, locale);
                bodyItem = painMapper.writeValueAsString(body);

                batchItemBuilder.add(tenantIdentifier, items, disposalAccountDepositRelativeUrl, bodyItem, false);

                td = new DtSavingsTransactionDetails(
                        transactionFeeInternalCorrelationId,
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
                        transactionFeeCategoryPurposeCode,
                        paymentScheme,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getPaymentIdentification().getEndToEndIdentification());
                camt053Body = painMapper.writeValueAsString(td);
                batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);
            }

            String conversionAccountWithdrawRelativeUrl = String.format("%s%s/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");

            String withdrawAmountOperation = "revertInAms.ConversionAccount.WithdrawTransactionAmount";
            String withdrawAmountConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
            paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawAmountConfigOperationKey);
            paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawAmountConfigOperationKey);
            camt053Fragment.setCreditDebitIndicator(CreditDebitCode.DBIT);
            if ("CURRENT".equalsIgnoreCase(accountProductType)) {
                // do not set AdditionalTransactionInformation for current account
            } else {
                camt053Fragment.setAdditionalTransactionInformation(paymentTypeCode);
            }
            camt053Fragment.getSupplementaryData().clear();
            pain001Camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001.getDocument(), camt053Fragment, transactionCategoryPurposeCode);
            pain001Camt053Mapper.refillOtherIdentification(pain001.getDocument(), camt053Fragment);

            if (!"Current".equalsIgnoreCase(accountProductType)) {
                savingsAccountsCamt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
                savingsAccountsCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            }
            camt053Entry = serializeCamt053orFragment(accountProductType, camt053Fragment, savingsAccountsCamt053Entry);

            body = new TransactionBody(transactionDate, amount, paymentTypeId, "", FORMAT, locale);
            bodyItem = painMapper.writeValueAsString(body);

            batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, bodyItem, false);

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
                    transactionCategoryPurposeCode,
                    paymentScheme,
                    conversionAccountAmsId,
                    disposalAccountAmsId,
                    pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getPaymentIdentification().getEndToEndIdentification());

            camt053Body = painMapper.writeValueAsString(td);

            batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);

            if (!BigDecimal.ZERO.equals(transactionFeeAmount)) {
                log.debug("Withdrawing fee {} from conversion account {}", transactionFeeAmount, conversionAccountAmsId);

                String withdrawFeeOperation = "revertInAms.ConversionAccount.WithdrawTransactionFee";
                String withdrawFeeConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawFeeOperation);
                paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawFeeConfigOperationKey);
                paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawFeeConfigOperationKey);
                if ("CURRENT".equalsIgnoreCase(accountProductType)) {
                    // do not set AdditionalTransactionInformation for current account
                } else {
                    camt053Fragment.setAdditionalTransactionInformation(paymentTypeCode);
                }
                camt053Fragment.getSupplementaryData().clear();
                pain001Camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001.getDocument(), camt053Fragment, transactionFeeCategoryPurposeCode);
                pain001Camt053Mapper.refillOtherIdentification(pain001.getDocument(), camt053Fragment);
                camt053Entry = serializeCamt053orFragment(accountProductType, camt053Fragment, savingsAccountsCamt053Entry);

                body = new TransactionBody(transactionDate, transactionFeeAmount, paymentTypeId, "", FORMAT, locale);
                bodyItem = painMapper.writeValueAsString(body);

                batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, bodyItem, false);

                td = new DtSavingsTransactionDetails(
                        transactionFeeInternalCorrelationId,
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
                        transactionFeeCategoryPurposeCode,
                        paymentScheme,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getPaymentIdentification().getEndToEndIdentification());
                camt053Body = painMapper.writeValueAsString(td);
                batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);
            }

            String lastTransactionId = doBatch(items,
                    tenantIdentifier,
                    disposalAccountAmsId,
                    conversionAccountAmsId,
                    internalCorrelationId,
                    "revertInAms");

            TransactionQueryBody tqBody = TransactionQueryBody.builder()
                    .request(TransactionQueryRequest.builder()
                            .baseQuery(TransactionQueryBaseQuery.builder()
                                    .columnFilters(new TransactionQueryColumnFilter[]{TransactionQueryColumnFilter.builder()
                                            .column("id")
                                            .filters(new TransactionQueryFilter[]{TransactionQueryFilter.builder()
                                                    .operator("EQ")
                                                    .values(new String[]{lastTransactionId})
                                                    .build()})
                                            .build()})
                                    .resultColumns(new String[]{"running_balance_derived"})
                                    .build())
                            .build())
                    .dateFormat("yyyy-MM-dd")
                    .locale("en")
                    .page(0)
                    .size(1)
                    .sorts(new String[]{})
                    .build();

            HttpHeaders httpHeaders = new HttpHeaders();
            httpHeaders.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
            httpHeaders.set("Authorization", authTokenHelper.generateAuthToken());
            httpHeaders.set("Fineract-Platform-TenantId", tenantIdentifier);
            HttpEntity<TransactionQueryBody> tqEntity = new HttpEntity<>(tqBody, httpHeaders);
            eventService.sendEvent(builder -> builder
                    .setSourceModule("ams_connector")
                    .setEvent("revertInAms")
                    .setEventLogLevel(EventLogLevel.INFO)
                    .setEventType(EventType.audit)
                    .setPayload(tqEntity.toString())
                    .setCorrelationIds(Map.of("CorrelationId", internalCorrelationId)));
            TransactionQueryPayload tqResponse = restTemplate.exchange(
                            String.format("%s/%s%s/transactions/query", fineractApiUrl, incomingMoneyApi.substring(1), disposalAccountAmsId),
                            HttpMethod.POST,
                            tqEntity,
                            TransactionQueryPayload.class)
                    .getBody();
            eventService.sendEvent(builder -> builder
                    .setEvent("revertInAms")
                    .setSourceModule("ams_connector")
                    .setEventLogLevel(EventLogLevel.INFO)
                    .setEventType(EventType.audit)
                    .setPayload(tqResponse.toString())
                    .setCorrelationIds(Map.of("CorrelationId", internalCorrelationId)));

            List<TransactionQueryContent> content = tqResponse.content();
            if (content.isEmpty()) {
                return Map.of("availableBalance", -1);
            }
            BigDecimal runningBalanceDerived = content.get(0).runningBalanceDerived().setScale(2, RoundingMode.HALF_UP);
            return Map.of("availableBalance", runningBalanceDerived.toString());

        } catch (JsonProcessingException e) {
            // TODO technical error handling
            throw new RuntimeException("failed in revert", e);
        } finally {
            MDC.remove("internalCorrelationId");
        }
    }

    @JobWorker
    @LogInternalCorrelationId
    @TraceZeebeArguments
    public void revertWithoutFeeInAms(JobClient jobClient,
                                      ActivatedJob activatedJob,
                                      @Variable String internalCorrelationId,
                                      @Variable String originalPain001,
                                      @Variable String conversionAccountAmsId,
                                      @Variable String disposalAccountAmsId,
                                      @Variable String paymentScheme,
                                      @Variable String transactionGroupId,
                                      @Variable String transactionCategoryPurposeCode,
                                      @Variable BigDecimal amount,
                                      @Variable String transactionFeeCategoryPurposeCode,
                                      @Variable BigDecimal transactionFeeAmount,
                                      @Variable String tenantIdentifier,
                                      @Variable String accountProductType
    ) {
        log.info("revertWithoutFeeInAms");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "revertWithoutFeeInAms", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> revertWithoutFeeInAms(internalCorrelationId,
                        originalPain001,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        paymentScheme,
                        transactionGroupId,
                        transactionCategoryPurposeCode,
                        amount,
                        transactionFeeCategoryPurposeCode,
                        transactionFeeAmount,
                        tenantIdentifier,
                        accountProductType
                ));
    }

    private Void revertWithoutFeeInAms(String internalCorrelationId,
                                       String originalPain001,
                                       String conversionAccountAmsId,
                                       String disposalAccountAmsId,
                                       String paymentScheme,
                                       String transactionGroupId,
                                       String transactionCategoryPurposeCode,
                                       BigDecimal amount,
                                       String transactionFeeCategoryPurposeCode,
                                       BigDecimal transactionFeeAmount,
                                       String tenantIdentifier,
                                       String accountProductType) {
        try {
            MDC.put("internalCorrelationId", internalCorrelationId);

            String transactionDate = LocalDate.now().format(DateTimeFormatter.ofPattern(FORMAT));

            Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = painMapper.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);

            log.debug("Withdrawing amount {} from conversion account {}", amount, conversionAccountAmsId);

            List<TransactionItem> items = new ArrayList<>();

            String disposalAccountDepositRelativeUrl = String.format("%s%s/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "deposit");

            Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);

            String depositAmountOperation = "revertWithoutFeeInAms.DisposalAccount.DepositTransactionAmount";
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

            var bodyItem = painMapper.writeValueAsString(body);
            batchItemBuilder.add(tenantIdentifier, items, disposalAccountDepositRelativeUrl, bodyItem, false);

            EntryTransaction10 camt053Fragment;
            String camt053Entry;
            ReportEntry10 savingsAccountsCamt053Entry = null;
            if ("CURRENT".equalsIgnoreCase(accountProductType)) {
                log.debug("Current account - mapping to Camt053 fragment");
                camt053Fragment = pain001Camt053Mapper.toCamt053Fragment(pain001.getDocument());
                camt053Fragment.setCreditDebitIndicator(CreditDebitCode.CRDT);
                // do not set AdditionalTransactionInformation for current account
            } else {
                log.debug("Savings account - mapping to Camt053 entry");
                BankToCustomerStatementV08 convertedStatement = pain001Camt053Mapper.toCamt053Entry(pain001.getDocument());
                savingsAccountsCamt053Entry = convertedStatement.getStatement().get(0).getEntry().get(0);
                camt053Fragment = savingsAccountsCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0);

                savingsAccountsCamt053Entry.setCreditDebitIndicator(CreditDebitCode.CRDT);
                savingsAccountsCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            }
            camt053Entry = serializeCamt053orFragment(accountProductType, camt053Fragment, savingsAccountsCamt053Entry);

            var td = new DtSavingsTransactionDetails(
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
                    transactionCategoryPurposeCode,
                    paymentScheme,
                    conversionAccountAmsId,
                    disposalAccountAmsId,
                    pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getPaymentIdentification().getEndToEndIdentification());

            var camt053Body = painMapper.writeValueAsString(td);

            String camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";

            batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);

            String conversionAccountWithdrawRelativeUrl = String.format("%s%s/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");


            String withdrawAmountOperation = "revertWithoutFeeInAms.ConversionAccount.WithdrawTransactionAmount";
            String withdrawAmountConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
            paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawAmountConfigOperationKey);
            paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawAmountConfigOperationKey);
            camt053Fragment.setCreditDebitIndicator(CreditDebitCode.DBIT);
            if ("CURRENT".equalsIgnoreCase(accountProductType)) {
                // do not set AdditionalTransactionInformation for current account
            } else {
                camt053Fragment.setAdditionalTransactionInformation(paymentTypeCode);
            }

            camt053Entry = serializeCamt053orFragment(accountProductType, camt053Fragment, savingsAccountsCamt053Entry);

            body = new TransactionBody(transactionDate, amount, paymentTypeId, "", FORMAT, locale);

            bodyItem = painMapper.writeValueAsString(body);

            batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, bodyItem, false);

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
                    transactionCategoryPurposeCode,
                    paymentScheme,
                    conversionAccountAmsId,
                    disposalAccountAmsId,
                    pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getPaymentIdentification().getEndToEndIdentification());

            camt053Body = painMapper.writeValueAsString(td);

            batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);

            if (!BigDecimal.ZERO.equals(transactionFeeAmount)) {
                log.debug("Withdrawing fee {} from conversion account {}", transactionFeeAmount, conversionAccountAmsId);

                String withdrawFeeOperation = "revertWithoutFeeInAms.ConversionAccount.WithdrawTransactionFee";
                String withdrawFeeConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawFeeOperation);
                paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawFeeConfigOperationKey);
                paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawFeeConfigOperationKey);

                if ("CURRENT".equalsIgnoreCase(accountProductType)) {
                    // do not set AdditionalTransactionInformation for current account
                } else {
                    camt053Fragment.setAdditionalTransactionInformation(paymentTypeCode);
                }
                camt053Fragment.getSupplementaryData().clear();
                pain001Camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001.getDocument(), camt053Fragment, transactionFeeCategoryPurposeCode);
                pain001Camt053Mapper.refillOtherIdentification(pain001.getDocument(), camt053Fragment);

                camt053Entry = serializeCamt053orFragment(accountProductType, camt053Fragment, savingsAccountsCamt053Entry);

                body = new TransactionBody(transactionDate, transactionFeeAmount, paymentTypeId, "", FORMAT, locale);

                bodyItem = painMapper.writeValueAsString(body);

                batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, bodyItem, false);

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
                        transactionFeeCategoryPurposeCode,
                        paymentScheme,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getPaymentIdentification().getEndToEndIdentification());
                camt053Body = painMapper.writeValueAsString(td);
                batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);
            }

            doBatch(items,
                    tenantIdentifier,
                    disposalAccountAmsId,
                    conversionAccountAmsId,
                    internalCorrelationId,
                    "revertWithoutFeeInAms");
        } catch (JsonProcessingException e) {
            // TODO technical error handling
            throw new RuntimeException("failed in revertWithoutFeeInAms", e);
        } finally {
            MDC.remove("internalCorrelationId");
        }

        return null;
    }

    private String serializeCamt053orFragment(String accountProductType, EntryTransaction10 camt053Fragment, ReportEntry10 savingsAccountsCamt053Entry) throws JsonProcessingException {
        if ("CURRENT".equalsIgnoreCase(accountProductType)) {
            log.debug("serializeCamt053orFragment: Current account");
            return painMapper.writeValueAsString(camt053Fragment);
        } else {
            log.debug("serializeCamt053orFragment: Savings account");
            return painMapper.writeValueAsString(savingsAccountsCamt053Entry);
        }
    }

    @JobWorker
    @TraceZeebeArguments
    @LogInternalCorrelationId
    public void depositTheAmountOnDisposalInAms(JobClient client,
                                                ActivatedJob activatedJob,
                                                @Variable BigDecimal amount,
                                                @Variable String conversionAccountAmsId,
                                                @Variable String disposalAccountAmsId,
                                                @Variable String tenantIdentifier,
                                                @Variable String paymentScheme,
                                                @Variable String transactionCategoryPurposeCode,
                                                @Variable String generatedPacs004,
                                                @Variable String pacs002,
                                                @Variable String internalCorrelationId,
                                                @Variable String accountProductType) {
        log.info("depositTheAmountOnDisposalInAms");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "depositTheAmountOnDisposalInAms",
                        null,
                        null,
                        eventBuilder),
                eventBuilder -> depositTheAmountOnDisposalInAms(amount,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        tenantIdentifier,
                        paymentScheme,
                        transactionCategoryPurposeCode,
                        generatedPacs004,
                        pacs002,
                        internalCorrelationId,
                        accountProductType
                ));
    }

    private Void depositTheAmountOnDisposalInAms(BigDecimal amount,
                                                 String conversionAccountAmsId,
                                                 String disposalAccountAmsId,
                                                 String tenantIdentifier,
                                                 String paymentScheme,
                                                 String transactionCategoryPurposeCode,
                                                 String originalPacs004,
                                                 String originalPacs002,
                                                 String internalCorrelationId,
                                                 String accountProductType) {
        try {
            String transactionDate = LocalDate.now().format(DateTimeFormatter.ofPattern(FORMAT));
            List<TransactionItem> items = new ArrayList<>();

            String disposalAccountDepositRelativeUrl = String.format("%s%s/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "deposit");
            Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);

            String depositAmountOperation = "depositTheAmountOnDisposalInAms.DisposalAccount.DepositTransactionAmount";
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

            var bodyItem = painMapper.writeValueAsString(body);

            batchItemBuilder.add(tenantIdentifier, items, disposalAccountDepositRelativeUrl, bodyItem, false);

            String camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";

            iso.std.iso._20022.tech.xsd.pacs_004_001.Document pacs004 = jaxbUtils.unmarshalPacs004(originalPacs004);

            iso.std.iso._20022.tech.xsd.pacs_002_001.Document pacs002 = jaxbUtils.unmarshalPacs002(originalPacs002);

            PaymentTransactionInformation27 paymentTransactionInformation = pacs004
                    .getPmtRtr()
                    .getTxInf().get(0);

            BankToCustomerStatementV08 camt053Object = pacs004Camt053Mapper.convert(pacs004, new BankToCustomerStatementV08()
                    .withStatement(List.of(new AccountStatement9()
                            .withEntry(List.of(new ReportEntry10()
                                    .withEntryDetails(List.of(new EntryDetails9()
                                            .withTransactionDetails(List.of(new EntryTransaction10())))))))));
            ReportEntry10 camt053Entry = camt053Object.getStatement().get(0).getEntry().get(0);
            ZoneId zi = TimeZone.getTimeZone("Europe/Budapest").toZoneId();
            ZonedDateTime zdt = pacs002.getFIToFIPmtStsRpt().getTxInfAndSts().get(0).getAccptncDtTm().toGregorianCalendar().toZonedDateTime().withZoneSameInstant(zi);
            var copy = DatatypeFactory.newDefaultInstance().newXMLGregorianCalendar(GregorianCalendar.from(zdt));
            camt053Entry.getValueDate().setAdditionalProperty("Date", copy);
            EntryTransaction10 transactionDetails = camt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0);
            if ("CURRENT".equalsIgnoreCase(accountProductType)) {
                // do not set AdditionalTransactionInformation for current account
            } else {
                transactionDetails.setAdditionalTransactionInformation(paymentTypeCode);
            }
            transactionDetails.setCreditDebitIndicator(CreditDebitCode.CRDT);
            camt053Entry.setCreditDebitIndicator(CreditDebitCode.CRDT);
            camt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));

            String camt053 = serializeCamt053orFragment(accountProductType, transactionDetails, camt053Entry);

            var td = new DtSavingsTransactionDetails(
                    internalCorrelationId,
                    camt053,
                    paymentTransactionInformation.getOrgnlTxRef().getCdtrAcct().getId().getIBAN(),
                    paymentTypeCode,
                    internalCorrelationId,
                    paymentTransactionInformation.getOrgnlTxRef().getDbtr().getNm(),
                    paymentTransactionInformation.getOrgnlTxRef().getDbtrAcct().getId().getIBAN(),
                    null,
                    contactDetailsUtil.getId(paymentTransactionInformation.getOrgnlTxRef().getDbtr().getCtctDtls()),
                    paymentTransactionInformation.getOrgnlTxRef().getRmtInf().getUstrd().toString(),
                    transactionCategoryPurposeCode,
                    paymentScheme,
                    null,
                    disposalAccountAmsId,
                    paymentTransactionInformation.getOrgnlEndToEndId());

            var camt053Body = painMapper.writeValueAsString(td);

            batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);

            String conversionAccountWithdrawRelativeUrl = String.format("%s%s/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");

            String withdrawAmountOperation = "depositTheAmountOnDisposalInAms.ConversionAccount.WithdrawTransactionAmount";
            String withdrawAmountConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
            paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawAmountConfigOperationKey);
            paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawAmountConfigOperationKey);
            if ("CURRENT".equalsIgnoreCase(accountProductType)) {
                // do not set AdditionalTransactionInformation for current account
            } else {
                transactionDetails.setAdditionalTransactionInformation(paymentTypeCode);
            }
            transactionDetails.setCreditDebitIndicator(CreditDebitCode.DBIT);
            camt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
            camt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            camt053 = serializeCamt053orFragment(accountProductType, transactionDetails, camt053Entry);

            body = new TransactionBody(
                    transactionDate,
                    amount,
                    paymentTypeId,
                    "",
                    FORMAT,
                    locale);

            bodyItem = painMapper.writeValueAsString(body);


            batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, bodyItem, false);

            td = new DtSavingsTransactionDetails(
                    internalCorrelationId,
                    camt053,
                    paymentTransactionInformation.getOrgnlTxRef().getCdtrAcct().getId().getIBAN(),
                    paymentTypeCode,
                    internalCorrelationId,
                    paymentTransactionInformation.getOrgnlTxRef().getDbtr().getNm(),
                    paymentTransactionInformation.getOrgnlTxRef().getDbtrAcct().getId().getIBAN(),
                    null,
                    contactDetailsUtil.getId(paymentTransactionInformation.getOrgnlTxRef().getDbtr().getCtctDtls()),
                    Optional.ofNullable(paymentTransactionInformation.getOrgnlTxRef().getRmtInf())
                            .map(iso.std.iso._20022.tech.xsd.pacs_004_001.RemittanceInformation5::getUstrd).map(List::toString).orElse(""),
                    transactionCategoryPurposeCode,
                    paymentScheme,
                    null,
                    disposalAccountAmsId,
                    paymentTransactionInformation.getOrgnlEndToEndId());

            camt053Body = painMapper.writeValueAsString(td);

            batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);

            doBatch(items,
                    tenantIdentifier,
                    disposalAccountAmsId,
                    conversionAccountAmsId,
                    internalCorrelationId,
                    "depositTheAmountOnDisposalInAms");
        } catch (JAXBException | JsonProcessingException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
        return null;
    }
}