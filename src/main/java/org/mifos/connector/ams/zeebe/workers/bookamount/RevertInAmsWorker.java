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
import iso.std.iso._20022.tech.json.pain_001_001.RemittanceInformation16;
import iso.std.iso._20022.tech.json.pain_001_001.*;
import iso.std.iso._20022.tech.xsd.pacs_004_001.PaymentTransactionInformation27;
import jakarta.xml.bind.JAXBException;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;
import org.mifos.connector.ams.common.SerializationHelper;
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
import org.springframework.http.*;
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

    @Value("${fineract.current-account-api}")
    protected String currentAccountApi;

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
    private SerializationHelper serializationHelper;

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
                                           @Variable String currency,
                                           @Variable String transactionFeeCategoryPurposeCode,
                                           @Variable BigDecimal transactionFeeAmount,
                                           @Variable String tenantIdentifier,
                                           @Variable String accountProductType,
                                           @Variable String valueDated
    ) {
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
                        currency,
                        transactionFeeCategoryPurposeCode,
                        transactionFeeAmount,
                        tenantIdentifier,
                        accountProductType,
                        Boolean.parseBoolean(Optional.ofNullable(valueDated).orElse("false"))
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
                                    String currency,
                                    String transactionFeeCategoryPurposeCode,
                                    BigDecimal transactionFeeAmount,
                                    String tenantIdentifier,
                                    String accountProductType,
                                    boolean valueDated
    ) {
        try {
            // STEP 0 - collect / extract information
            MDC.put("internalCorrelationId", internalCorrelationId);
            String transactionDate = hyphenatedTransactionDate.replaceAll("-", "");
            Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = painMapper.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
            String apiPath = accountProductType.equalsIgnoreCase("SAVINGS") ? incomingMoneyApi.substring(1) : currentAccountApi.substring(1);
            String conversionAccountWithdrawRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, conversionAccountAmsId, "withdrawal");
            String disposalAccountDepositRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, disposalAccountAmsId, "deposit");
            Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
            CustomerCreditTransferInitiationV10 pain001Document = pain001.getDocument();
            PaymentInstruction34 paymentInstruction = pain001Document.getPaymentInformation().get(0);
            CreditTransferTransaction40 creditTransferTransaction = paymentInstruction.getCreditTransferTransactionInformation().get(0);
            String transactionCreationChannel = batchItemBuilder.findTransactionCreationChannel(creditTransferTransaction.getSupplementaryData());

            BankToCustomerStatementV08 convertedStatement = pain001Camt053Mapper.toCamt053Entry(pain001Document);
            ReportEntry10 camt053Entry = convertedStatement.getStatement().get(0).getEntry().get(0);
            EntryTransaction10 camt053Fragment = camt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0);
            String debtorIban = paymentInstruction.getDebtorAccount().getIdentification().getIban();
            String unstructured = Optional.ofNullable(creditTransferTransaction.getRemittanceInformation())
                    .map(RemittanceInformation16::getUnstructured).map(List::toString).orElse("");
            String creditorId = contactDetailsUtil.getId(creditTransferTransaction.getCreditor().getContactDetails());
            String creditorName = creditTransferTransaction.getCreditor().getName();
            String creditorIban = creditTransferTransaction.getCreditorAccount().getIdentification().getIban();
            String endToEndId = creditTransferTransaction.getPaymentIdentification().getEndToEndIdentification();
            String partnerAccountSecondaryIdentifier = contactDetailsUtil.getId(pain001Document.getPaymentInformation().get(0).getDebtor().getContactDetails());

            List<TransactionItem> items = new ArrayList<>();

            // STEP 1a - re-deposit amount in disposal account
            log.debug("re-deposit amount {} in disposal account: {}", amount, disposalAccountAmsId);
            String depositAmountOperation = "revertInAms.DisposalAccount.DepositTransactionAmount";
            String depositAmountConfigOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
            String depositAmountPaymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(depositAmountConfigOperationKey);
            String depositAmountPaymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(depositAmountConfigOperationKey);
            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                String depositAmountCamt053 = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, depositAmountPaymentTypeId, "", FORMAT, locale));
                batchItemBuilder.add(tenantIdentifier, items, disposalAccountDepositRelativeUrl, depositAmountCamt053, false);
            } // // CURRENT account executes deposit and details in one step

            camt053Fragment.setAdditionalTransactionInformation(depositAmountPaymentTypeCode);
            camt053Entry.setCreditDebitIndicator(CreditDebitCode.CRDT);
            camt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            String camt053 = serializationHelper.writeCamt053AsString(accountProductType, camt053Entry);

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                String depositAmountTransactionBody = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, camt053, debtorIban, depositAmountPaymentTypeCode, transactionGroupId, creditorName, creditorIban, null, creditorId, unstructured, transactionCategoryPurposeCode, paymentScheme, conversionAccountAmsId, disposalAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, items, "datatables/dt_savings_transaction_details/$.resourceId", depositAmountTransactionBody, true);
            } else {
                String depositAmountTransactionBody = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, locale, depositAmountPaymentTypeId, currency, List.of(
                        new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                                debtorIban,
                                camt053,
                                internalCorrelationId,
                                creditorName,
                                creditorIban,
                                transactionGroupId,
                                endToEndId,
                                transactionCategoryPurposeCode,
                                paymentScheme,
                                unstructured,
                                conversionAccountAmsId,
                                disposalAccountAmsId,
                                transactionCreationChannel,
                                partnerAccountSecondaryIdentifier,
                                null,
                                valueDated
                        )), "dt_current_transaction_details"))));
                batchItemBuilder.add(tenantIdentifier, items, disposalAccountDepositRelativeUrl, depositAmountTransactionBody, false);
            }
            // STEP 1b - re-deposit fee in disposal account
            if (!BigDecimal.ZERO.equals(transactionFeeAmount)) {
                log.debug("Re-depositing fee {} in disposal account {}", transactionFeeAmount, disposalAccountAmsId);
                String depositFeeOperation = "revertInAms.DisposalAccount.DepositTransactionFee";
                String depositFeeConfigOperationKey = String.format("%s.%s", paymentScheme, depositFeeOperation);
                String depositFeePaymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(depositFeeConfigOperationKey);
                String depositFeePaymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(depositFeeConfigOperationKey);
                camt053Fragment.setAdditionalTransactionInformation(depositFeePaymentTypeCode);
                camt053Fragment.setSupplementaryData(new ArrayList<>());
                pain001Camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001Document, camt053Fragment, transactionFeeCategoryPurposeCode);
                pain001Camt053Mapper.refillOtherIdentification(pain001Document, camt053Fragment);
                camt053 = serializationHelper.writeCamt053AsString(accountProductType, camt053Entry);

                if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                    String depositFeeBodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, transactionFeeAmount, depositFeePaymentTypeId, "", FORMAT, locale));
                    batchItemBuilder.add(tenantIdentifier, items, disposalAccountDepositRelativeUrl, depositFeeBodyItem, false);

                    String depositFeeDetailsBody = painMapper.writeValueAsString(new DtSavingsTransactionDetails(transactionFeeInternalCorrelationId, camt053, debtorIban, depositFeePaymentTypeCode, transactionGroupId, creditorName, creditorIban, null, creditorId, unstructured, transactionFeeCategoryPurposeCode, paymentScheme, conversionAccountAmsId, disposalAccountAmsId, endToEndId));
                    batchItemBuilder.add(tenantIdentifier, items, "datatables/dt_savings_transaction_details/$.resourceId", depositFeeDetailsBody, true);
                } else {  // CURRENT account executes withdrawal and details in one step
                    String depositFeeTransactionBody = painMapper.writeValueAsString(new CurrentAccountTransactionBody(transactionFeeAmount, FORMAT, locale, depositFeePaymentTypeId, currency, List.of(
                            new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                                    debtorIban,
                                    camt053,
                                    transactionFeeInternalCorrelationId,
                                    creditorName,
                                    creditorIban,
                                    transactionGroupId,
                                    endToEndId,
                                    transactionFeeCategoryPurposeCode,
                                    paymentScheme,
                                    unstructured,
                                    disposalAccountAmsId,
                                    conversionAccountAmsId,
                                    transactionCreationChannel,
                                    partnerAccountSecondaryIdentifier,
                                    null,
                                    valueDated
                            )), "dt_current_transaction_details")))
                    );
                    batchItemBuilder.add(tenantIdentifier, items, disposalAccountDepositRelativeUrl, depositFeeTransactionBody, false);
                }
            }

            // STEP 2a - withdraw amount from conversion account
            log.debug("Withdrawing {} from conversion account {}", amount, conversionAccountAmsId);
            String withdrawAmountOperation = "revertInAms.ConversionAccount.WithdrawTransactionAmount";
            String withdrawAmountConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
            String withdrawAmountPaymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawAmountConfigOperationKey);
            String withdrawAmountPaymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawAmountConfigOperationKey);
            camt053Fragment.setCreditDebitIndicator(CreditDebitCode.DBIT);
            camt053Fragment.setAdditionalTransactionInformation(withdrawAmountPaymentTypeCode);
            camt053Fragment.setSupplementaryData(new ArrayList<>());
            pain001Camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001Document, camt053Fragment, transactionCategoryPurposeCode);
            pain001Camt053Mapper.refillOtherIdentification(pain001Document, camt053Fragment);
            camt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
            camt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            camt053 = serializationHelper.writeCamt053AsString(accountProductType, camt053Entry);

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                String withdrawAmountBodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, withdrawAmountPaymentTypeId, "", FORMAT, locale));
                batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, withdrawAmountBodyItem, false);
                String withdrawAmountDetailsBody = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, camt053, debtorIban, withdrawAmountPaymentTypeCode, transactionGroupId, creditorName, creditorIban, null, creditorId, unstructured, transactionCategoryPurposeCode, paymentScheme, conversionAccountAmsId, disposalAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, items, "datatables/dt_savings_transaction_details/$.resourceId", withdrawAmountDetailsBody, true);
            } else { // CURRENT account executes withdrawal and details in one step
                String withdrawAmountTransactionBody = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, locale, withdrawAmountPaymentTypeId, currency, List.of(
                        new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                                debtorIban,
                                camt053,
                                internalCorrelationId,
                                creditorName,
                                creditorIban,
                                transactionGroupId,
                                endToEndId,
                                transactionCategoryPurposeCode,
                                paymentScheme,
                                unstructured,
                                disposalAccountAmsId,
                                conversionAccountAmsId,
                                transactionCreationChannel,
                                partnerAccountSecondaryIdentifier,
                                null,
                                valueDated
                        )), "dt_current_transaction_details")))
                );
                batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, withdrawAmountTransactionBody, false);
            }

            // STEP 2b - withdraw fee from conversion account
            if (!BigDecimal.ZERO.equals(transactionFeeAmount)) {
                log.debug("Withdrawing fee {} from conversion account {}", transactionFeeAmount, conversionAccountAmsId);

                String withdrawFeeOperation = "revertInAms.ConversionAccount.WithdrawTransactionFee";
                String withdrawFeeConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawFeeOperation);
                String withdrawFeePaymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawFeeConfigOperationKey);
                String withdrawFeePaymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawFeeConfigOperationKey);
                camt053Fragment.setAdditionalTransactionInformation(withdrawFeePaymentTypeCode);
                camt053Fragment.setSupplementaryData(new ArrayList<>());
                pain001Camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001Document, camt053Fragment, transactionFeeCategoryPurposeCode);
                pain001Camt053Mapper.refillOtherIdentification(pain001Document, camt053Fragment);
                camt053 = serializationHelper.writeCamt053AsString(accountProductType, camt053Entry);

                if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                    String withdrawFeeBodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, transactionFeeAmount, withdrawFeePaymentTypeId, "", FORMAT, locale));
                    batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, withdrawFeeBodyItem, false);

                    String withdrawAmountDetailsBody = painMapper.writeValueAsString(new DtSavingsTransactionDetails(transactionFeeInternalCorrelationId, camt053, debtorIban, withdrawFeePaymentTypeCode, transactionGroupId, creditorName, creditorIban, null, creditorId, unstructured, transactionFeeCategoryPurposeCode, paymentScheme, conversionAccountAmsId, disposalAccountAmsId, endToEndId));
                    batchItemBuilder.add(tenantIdentifier, items, "datatables/dt_savings_transaction_details/$.resourceId", withdrawAmountDetailsBody, true);
                } else {  // CURRENT account executes withdrawal and details in one step
                    String withdrawFeeTransactionBody = painMapper.writeValueAsString(new CurrentAccountTransactionBody(transactionFeeAmount, FORMAT, locale, withdrawFeePaymentTypeId, currency, List.of(
                            new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                                    debtorIban,
                                    camt053,
                                    transactionFeeInternalCorrelationId,
                                    creditorName,
                                    creditorIban,
                                    transactionGroupId,
                                    endToEndId,
                                    transactionFeeCategoryPurposeCode,
                                    paymentScheme,
                                    unstructured,
                                    disposalAccountAmsId,
                                    conversionAccountAmsId,
                                    transactionCreationChannel,
                                    partnerAccountSecondaryIdentifier,
                                    null,
                                    valueDated
                            )), "dt_current_transaction_details")))
                    );
                    batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, withdrawFeeTransactionBody, false);
                }
            }

            String lastTransactionId = doBatch(items, tenantIdentifier, transactionGroupId, disposalAccountAmsId, conversionAccountAmsId, internalCorrelationId, "revertInAms");

            log.debug("querying running balance for account {}", disposalAccountAmsId);
            BigDecimal runningBalanceDerived = accountProductType.equalsIgnoreCase("SAVINGS") ?
                    queryRunningBalance(internalCorrelationId, disposalAccountAmsId, tenantIdentifier, lastTransactionId)
                    : queryCurrentAccountBalance(apiPath, internalCorrelationId, disposalAccountAmsId, tenantIdentifier);

            return Map.of("availableBalance", runningBalanceDerived.toString());

        } catch (JsonProcessingException e) {
            throw new RuntimeException("failed in revert", e);
        } finally {
            MDC.remove("internalCorrelationId");
        }
    }

    private BigDecimal queryCurrentAccountBalance(String apiPath, String internalCorrelationId, String disposalAccountAmsId, String tenantIdentifier) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", authTokenHelper.generateAuthToken());
        headers.set("Fineract-Platform-TenantId", tenantIdentifier);
        headers.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
        String url = String.format("%s/%s/%s", fineractApiUrl, apiPath, disposalAccountAmsId);

        return eventService.auditedEvent(event -> event.setSourceModule("ams_connector")
                        .setEvent("revertInAms - availableBalanceRequest")
                        .setEventLogLevel(EventLogLevel.INFO)
                        .setEventType(EventType.audit)
                        .setPayload("GET " + url)
                        .setCorrelationIds(Map.of("CorrelationId", internalCorrelationId)), event ->
                {
                    ResponseEntity<String> responseEntity = restTemplate.exchange(url, HttpMethod.GET, new HttpEntity<>(headers), String.class);
                    String responseData = responseEntity.getBody();
                    event.setPayload(event.getPayload() + " -> " + responseData);
                    return new JSONObject(responseData).getBigDecimal("accountBalance");
                }
        );
    }

    @NotNull
    private BigDecimal queryRunningBalance(String internalCorrelationId, String disposalAccountAmsId, String tenantIdentifier, String lastTransactionId) {
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
            return BigDecimal.valueOf(-1);
        } else {
            return content.get(0).runningBalanceDerived().setScale(2, RoundingMode.HALF_UP);
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
                                      @Variable String currency,
                                      @Variable String transactionFeeCategoryPurposeCode,
                                      @Variable BigDecimal transactionFeeAmount,
                                      @Variable String tenantIdentifier,
                                      @Variable String accountProductType,
                                      @Variable String valueDated
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
                        currency,
                        transactionFeeCategoryPurposeCode,
                        transactionFeeAmount,
                        tenantIdentifier,
                        accountProductType,
                        Boolean.parseBoolean(Optional.ofNullable(valueDated).orElse("false"))
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
                                       String currency,
                                       String transactionFeeCategoryPurposeCode,
                                       BigDecimal transactionFeeAmount,
                                       String tenantIdentifier,
                                       String accountProductType,
                                       boolean valueDated) {
        try {
            // STEP 0 - collect / extract information
            MDC.put("internalCorrelationId", internalCorrelationId);
            log.debug("Withdrawing amount {} from conversion account {}", amount, conversionAccountAmsId);
            String apiPath = accountProductType.equalsIgnoreCase("SAVINGS") ? incomingMoneyApi.substring(1) : currentAccountApi.substring(1);
            String transactionDate = LocalDate.now().format(DateTimeFormatter.ofPattern(FORMAT));
            String conversionAccountWithdrawRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, conversionAccountAmsId, "withdrawal");
            String disposalAccountDepositRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, disposalAccountAmsId, "deposit");
            Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = painMapper.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
            Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
            String depositAmountOperation = "revertWithoutFeeInAms.DisposalAccount.DepositTransactionAmount";
            String depositAmountConfigOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
            PaymentInstruction34 paymentInstruction = pain001.getDocument().getPaymentInformation().get(0);
            CreditTransferTransaction40 creditTransferTransaction = paymentInstruction.getCreditTransferTransactionInformation().get(0);
            String debtorIban = paymentInstruction.getDebtorAccount().getIdentification().getIban();
            String depositPaymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(depositAmountConfigOperationKey);
            String depositPaymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(depositAmountConfigOperationKey);
            String creditorName = creditTransferTransaction.getCreditor().getName();
            String creditorIban = creditTransferTransaction.getCreditorAccount().getIdentification().getIban();
            String creditorContactDetails = contactDetailsUtil.getId(creditTransferTransaction.getCreditor().getContactDetails());
            String unstructured = Optional.ofNullable(creditTransferTransaction.getRemittanceInformation()).map(RemittanceInformation16::getUnstructured).map(List::toString).orElse("");
            String endToEndId = creditTransferTransaction.getPaymentIdentification().getEndToEndIdentification();
            String partnerAccountSecondaryIdentifier = contactDetailsUtil.getId(creditTransferTransaction.getCreditor().getContactDetails());
            List<TransactionItem> items = new ArrayList<>();

            // STEP 1 - batch: deposit amount
            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                var bodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, depositPaymentTypeId, "", FORMAT, locale));
                batchItemBuilder.add(tenantIdentifier, items, disposalAccountDepositRelativeUrl, bodyItem, false);
            } // CURRENT account sends a single call only at the details step

            // STEP 2 - batch: deposit details
            EntryTransaction10 camt053Fragment = pain001Camt053Mapper.toCamt053Fragment(pain001.getDocument());
            camt053Fragment.setCreditDebitIndicator(CreditDebitCode.CRDT);

            BankToCustomerStatementV08 camt053Entry = pain001Camt053Mapper.toCamt053Entry(pain001.getDocument());
            ReportEntry10 reportEntry = camt053Entry.getStatement().get(0).getEntry().get(0);
            reportEntry.setCreditDebitIndicator(CreditDebitCode.CRDT);
            reportEntry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            String depositCamt053 = serializationHelper.writeCamt053AsString(accountProductType, reportEntry);

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                var camt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, depositCamt053, debtorIban, depositPaymentTypeCode, transactionGroupId, creditorName, creditorIban, null, creditorContactDetails, unstructured, transactionCategoryPurposeCode, paymentScheme, conversionAccountAmsId, disposalAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, items, "datatables/dt_savings_transaction_details/$.resourceId", camt053Body, true);
            } else {
                var camt053Body = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, locale, depositPaymentTypeId, currency, List.of(new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(debtorIban, depositCamt053, internalCorrelationId, creditorName, creditorIban, transactionGroupId, endToEndId, transactionCategoryPurposeCode, paymentScheme, unstructured, conversionAccountAmsId, disposalAccountAmsId, null, partnerAccountSecondaryIdentifier, null, valueDated)), "dt_current_transaction_details"))));
                batchItemBuilder.add(tenantIdentifier, items, disposalAccountDepositRelativeUrl, camt053Body, false);
            }

            // STEP 3 - batch: withdraw
            String withdrawAmountOperation = "revertWithoutFeeInAms.ConversionAccount.WithdrawTransactionAmount";
            String withdrawAmountConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
            var withdrawPaymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawAmountConfigOperationKey);
            var withdrawPaymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawAmountConfigOperationKey);
            camt053Fragment.setCreditDebitIndicator(CreditDebitCode.DBIT);

            String withdrawCamt053 = serializationHelper.writeCamt053AsString(accountProductType, reportEntry);

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                String bodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, withdrawPaymentTypeId, "", FORMAT, locale));
                batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, bodyItem, false);

                var camt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, withdrawCamt053, debtorIban, withdrawPaymentTypeCode, transactionGroupId, creditorName, creditorIban, null, creditorContactDetails, unstructured, transactionCategoryPurposeCode, paymentScheme, conversionAccountAmsId, disposalAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, items, "datatables/dt_savings_transaction_details/$.resourceId", camt053Body, true);
            } else {
                var camt03Body = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, locale, withdrawPaymentTypeId, currency, List.of(new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                        debtorIban,
                        withdrawCamt053,
                        internalCorrelationId,
                        creditorName,
                        creditorIban,
                        transactionGroupId,
                        endToEndId,
                        transactionCategoryPurposeCode,
                        paymentScheme,
                        unstructured,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        null,
                        partnerAccountSecondaryIdentifier,
                        null,
                        valueDated
                )), "dt_current_transaction_details"))));
                batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, camt03Body, false);
            }

            // STEP 4 - batch: withdraw fee
            if (!BigDecimal.ZERO.equals(transactionFeeAmount)) {
                log.debug("Withdrawing fee {} from conversion account {}", transactionFeeAmount, conversionAccountAmsId);

                String withdrawFeeOperation = "revertWithoutFeeInAms.ConversionAccount.WithdrawTransactionFee";
                String withdrawFeeConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawFeeOperation);
                withdrawPaymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawFeeConfigOperationKey);
                withdrawPaymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawFeeConfigOperationKey);
                camt053Fragment.getSupplementaryData().clear();
                pain001Camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001.getDocument(), camt053Fragment, transactionFeeCategoryPurposeCode);
                pain001Camt053Mapper.refillOtherIdentification(pain001.getDocument(), camt053Fragment);

                String withdrawFeeCamt053 = serializationHelper.writeCamt053AsString(accountProductType, reportEntry);

                if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                    var bodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, transactionFeeAmount, withdrawPaymentTypeId, "", FORMAT, locale));
                    batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, bodyItem, false);

                    var camt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, withdrawFeeCamt053, debtorIban, withdrawPaymentTypeCode, transactionGroupId, creditorName, creditorIban, null, creditorContactDetails, unstructured, transactionFeeCategoryPurposeCode, paymentScheme, conversionAccountAmsId, disposalAccountAmsId, endToEndId));
                    batchItemBuilder.add(tenantIdentifier, items, "datatables/dt_savings_transaction_details/$.resourceId", camt053Body, true);
                } else {
                    var body = painMapper.writeValueAsString(new CurrentAccountTransactionBody(transactionFeeAmount, FORMAT, locale, withdrawPaymentTypeId, currency, List.of(
                            new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                                    debtorIban,
                                    withdrawFeeCamt053,
                                    internalCorrelationId,
                                    creditorName,
                                    creditorIban,
                                    transactionGroupId,
                                    endToEndId,
                                    transactionFeeCategoryPurposeCode,
                                    paymentScheme,
                                    unstructured,
                                    conversionAccountAmsId,
                                    disposalAccountAmsId,
                                    null,
                                    partnerAccountSecondaryIdentifier,
                                    null,
                                    valueDated
                            )), "dt_current_transaction_details")
                    )));
                    batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, body, false);
                }
            }

            doBatch(items, tenantIdentifier, transactionGroupId, disposalAccountAmsId, conversionAccountAmsId, internalCorrelationId, "revertWithoutFeeInAms");
        } catch (JsonProcessingException e) {
            throw new RuntimeException("failed in revertWithoutFeeInAms", e);
        } finally {
            MDC.remove("internalCorrelationId");
        }

        return null;
    }

    private String serializeCamt053orFragment(String accountProductType, EntryTransaction10 camt053Fragment, ReportEntry10 savingsAccountsCamt053Entry) throws
            JsonProcessingException {
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
                                                @Variable String transactionGroupId,
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
                        transactionGroupId,
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
                                                 String transactionGroupId,
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
            if ("SAVINGS".equalsIgnoreCase(accountProductType)) {
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
            if ("SAVINGS".equalsIgnoreCase(accountProductType)) {
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
                    transactionGroupId,
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

