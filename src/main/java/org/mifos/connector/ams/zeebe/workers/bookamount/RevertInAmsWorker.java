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
import iso.std.iso._20022.tech.xsd.pacs_004_001.RemittanceInformation5;
import jakarta.xml.bind.JAXBException;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;
import org.mifos.connector.ams.common.SerializationHelper;
import org.mifos.connector.ams.fineract.TenantConfigs;
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
import javax.xml.datatype.XMLGregorianCalendar;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.mifos.connector.ams.zeebe.workers.bookamount.MoneyInOutWorker.FORMAT;

@Component
@Slf4j
public class RevertInAmsWorker {

    @Autowired
    Pain001Camt053Mapper pain001Camt053Mapper;

    private Pacs004ToCamt053Converter pacs004Camt053Mapper = new Pacs004ToCamt053Converter();

    @Value("${fineract.incoming-money-api}")
    String incomingMoneyApi;

    @Value("${fineract.current-account-api}")
    String currentAccountApi;

    @Autowired
    TenantConfigs tenantConfigs;

    @Autowired
    private JAXBUtils jaxbUtils;

    @Autowired
    BatchItemBuilder batchItemBuilder;

    @Autowired
    ContactDetailsUtil contactDetailsUtil;

    @Autowired
    AuthTokenHelper authTokenHelper;

    @Autowired
    SerializationHelper serializationHelper;

    @Autowired
    EventService eventService;

    @Autowired
    @Qualifier("painMapper")
    ObjectMapper painMapper;

    @Autowired
    private MoneyInOutWorker moneyInOutWorker;

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
                                           @Variable String transactionId,
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
                        transactionId,
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
                                    String transactionId,
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
            CustomerCreditTransferInitiationV10 pain001Document = pain001.getDocument();
            PaymentInstruction34 paymentInstruction = pain001Document.getPaymentInformation().get(0);
            CreditTransferTransaction40 creditTransferTransaction = paymentInstruction.getCreditTransferTransactionInformation().get(0);
            String transactionCreationChannel = batchItemBuilder.findTransactionCreationChannel(creditTransferTransaction.getSupplementaryData());

            BankToCustomerStatementV08 convertedStatement = pain001Camt053Mapper.toCamt053Entry(pain001Document);
            ReportEntry10 camt053Entry = convertedStatement.getStatement().get(0).getEntry().get(0);
            EntryTransaction10 entryTransaction10 = camt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0);
            batchItemBuilder.setAmount(entryTransaction10, amount, currency);
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
            String depositAmountPaymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, depositAmountConfigOperationKey);
            String depositAmountPaymentTypeCode = tenantConfigs.findResourceCode(tenantIdentifier, depositAmountConfigOperationKey);
            String direction = tenantConfigs.findDirection(tenantIdentifier, depositAmountConfigOperationKey);

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                String depositAmountCamt053 = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, depositAmountPaymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                batchItemBuilder.add(tenantIdentifier, items, disposalAccountDepositRelativeUrl, depositAmountCamt053, false);
            } // // CURRENT account executes deposit and details in one step

            entryTransaction10.setAdditionalTransactionInformation(depositAmountPaymentTypeCode);
            camt053Entry.setCreditDebitIndicator(CreditDebitCode.CRDT);
            camt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            String camt053 = serializationHelper.writeCamt053AsString(accountProductType, camt053Entry);

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                String depositAmountTransactionBody = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, camt053, debtorIban, depositAmountPaymentTypeCode, transactionGroupId, creditorName, creditorIban, null, creditorId, unstructured, transactionCategoryPurposeCode, paymentScheme, conversionAccountAmsId, disposalAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, items, "datatables/dt_savings_transaction_details/$.resourceId", depositAmountTransactionBody, true);
            } else {
                String depositAmountTransactionBody = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, moneyInOutWorker.getLocale(), depositAmountPaymentTypeId, currency, List.of(
                        new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                                debtorIban,
                                camt053,
                                internalCorrelationId,
                                creditorName,
                                creditorIban,
                                transactionGroupId,
                                transactionId,
                                endToEndId,
                                transactionCategoryPurposeCode,
                                paymentScheme,
                                unstructured,
                                conversionAccountAmsId,
                                disposalAccountAmsId,
                                transactionCreationChannel,
                                partnerAccountSecondaryIdentifier,
                                null,
                                valueDated,
                                direction
                        )), "dt_current_transaction_details"))));
                batchItemBuilder.add(tenantIdentifier, items, disposalAccountDepositRelativeUrl, depositAmountTransactionBody, false);
            }
            // STEP 1b - re-deposit fee in disposal account
            if (!BigDecimal.ZERO.equals(transactionFeeAmount)) {
                log.debug("Re-depositing fee {} in disposal account {}", transactionFeeAmount, disposalAccountAmsId);
                String depositFeeOperation = "revertInAms.DisposalAccount.DepositTransactionFee";
                String depositFeeConfigOperationKey = String.format("%s.%s", paymentScheme, depositFeeOperation);
                String depositFeePaymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, depositFeeConfigOperationKey);
                String depositFeePaymentTypeCode = tenantConfigs.findResourceCode(tenantIdentifier, depositFeeConfigOperationKey);
                entryTransaction10.setAdditionalTransactionInformation(depositFeePaymentTypeCode);
                entryTransaction10.setSupplementaryData(new ArrayList<>());
                pain001Camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001Document, entryTransaction10, transactionFeeCategoryPurposeCode);
                pain001Camt053Mapper.refillOtherIdentification(pain001Document, entryTransaction10);
                camt053 = serializationHelper.writeCamt053AsString(accountProductType, camt053Entry);

                if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                    String depositFeeBodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, transactionFeeAmount, depositFeePaymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                    batchItemBuilder.add(tenantIdentifier, items, disposalAccountDepositRelativeUrl, depositFeeBodyItem, false);

                    String depositFeeDetailsBody = painMapper.writeValueAsString(new DtSavingsTransactionDetails(transactionFeeInternalCorrelationId, camt053, debtorIban, depositFeePaymentTypeCode, transactionGroupId, creditorName, creditorIban, null, creditorId, unstructured, transactionFeeCategoryPurposeCode, paymentScheme, conversionAccountAmsId, disposalAccountAmsId, endToEndId));
                    batchItemBuilder.add(tenantIdentifier, items, "datatables/dt_savings_transaction_details/$.resourceId", depositFeeDetailsBody, true);
                } else {  // CURRENT account executes withdrawal and details in one step
                    String depositFeeTransactionBody = painMapper.writeValueAsString(new CurrentAccountTransactionBody(transactionFeeAmount, FORMAT, moneyInOutWorker.getLocale(), depositFeePaymentTypeId, currency, List.of(
                            new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                                    debtorIban,
                                    camt053,
                                    transactionFeeInternalCorrelationId,
                                    creditorName,
                                    creditorIban,
                                    transactionGroupId,
                                    transactionId,
                                    endToEndId,
                                    transactionFeeCategoryPurposeCode,
                                    paymentScheme,
                                    unstructured,
                                    disposalAccountAmsId,
                                    conversionAccountAmsId,
                                    transactionCreationChannel,
                                    partnerAccountSecondaryIdentifier,
                                    null,
                                    valueDated,
                                    direction
                            )), "dt_current_transaction_details")))
                    );
                    batchItemBuilder.add(tenantIdentifier, items, disposalAccountDepositRelativeUrl, depositFeeTransactionBody, false);
                }
            }

            // STEP 2a - withdraw amount from conversion account
            log.debug("Withdrawing {} from conversion account {}", amount, conversionAccountAmsId);
            String withdrawAmountOperation = "revertInAms.ConversionAccount.WithdrawTransactionAmount";
            String withdrawAmountConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
            String withdrawAmountPaymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, withdrawAmountConfigOperationKey);
            String withdrawAmountPaymentTypeCode = tenantConfigs.findResourceCode(tenantIdentifier, withdrawAmountConfigOperationKey);
            entryTransaction10.setCreditDebitIndicator(CreditDebitCode.DBIT);
            entryTransaction10.setAdditionalTransactionInformation(withdrawAmountPaymentTypeCode);
            entryTransaction10.setSupplementaryData(new ArrayList<>());
            pain001Camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001Document, entryTransaction10, transactionCategoryPurposeCode);
            pain001Camt053Mapper.refillOtherIdentification(pain001Document, entryTransaction10);
            camt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
            camt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            camt053 = serializationHelper.writeCamt053AsString(accountProductType, camt053Entry);

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                String withdrawAmountBodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, withdrawAmountPaymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, withdrawAmountBodyItem, false);
                String withdrawAmountDetailsBody = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, camt053, debtorIban, withdrawAmountPaymentTypeCode, transactionGroupId, creditorName, creditorIban, null, creditorId, unstructured, transactionCategoryPurposeCode, paymentScheme, conversionAccountAmsId, disposalAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, items, "datatables/dt_savings_transaction_details/$.resourceId", withdrawAmountDetailsBody, true);
            } else { // CURRENT account executes withdrawal and details in one step
                String withdrawAmountTransactionBody = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, moneyInOutWorker.getLocale(), withdrawAmountPaymentTypeId, currency, List.of(
                        new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                                debtorIban,
                                camt053,
                                internalCorrelationId,
                                creditorName,
                                creditorIban,
                                transactionGroupId,
                                transactionId,
                                endToEndId,
                                transactionCategoryPurposeCode,
                                paymentScheme,
                                unstructured,
                                disposalAccountAmsId,
                                conversionAccountAmsId,
                                transactionCreationChannel,
                                partnerAccountSecondaryIdentifier,
                                null,
                                valueDated,
                                direction
                        )), "dt_current_transaction_details")))
                );
                batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, withdrawAmountTransactionBody, false);
            }

            // STEP 2b - withdraw fee from conversion account
            if (!BigDecimal.ZERO.equals(transactionFeeAmount)) {
                log.debug("Withdrawing fee {} from conversion account {}", transactionFeeAmount, conversionAccountAmsId);

                String withdrawFeeOperation = "revertInAms.ConversionAccount.WithdrawTransactionFee";
                String withdrawFeeConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawFeeOperation);
                String withdrawFeePaymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, withdrawFeeConfigOperationKey);
                String withdrawFeePaymentTypeCode = tenantConfigs.findResourceCode(tenantIdentifier, withdrawFeeConfigOperationKey);
                entryTransaction10.setAdditionalTransactionInformation(withdrawFeePaymentTypeCode);
                entryTransaction10.setSupplementaryData(new ArrayList<>());
                pain001Camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001Document, entryTransaction10, transactionFeeCategoryPurposeCode);
                pain001Camt053Mapper.refillOtherIdentification(pain001Document, entryTransaction10);
                camt053 = serializationHelper.writeCamt053AsString(accountProductType, camt053Entry);

                if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                    String withdrawFeeBodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, transactionFeeAmount, withdrawFeePaymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                    batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, withdrawFeeBodyItem, false);

                    String withdrawAmountDetailsBody = painMapper.writeValueAsString(new DtSavingsTransactionDetails(transactionFeeInternalCorrelationId, camt053, debtorIban, withdrawFeePaymentTypeCode, transactionGroupId, creditorName, creditorIban, null, creditorId, unstructured, transactionFeeCategoryPurposeCode, paymentScheme, conversionAccountAmsId, disposalAccountAmsId, endToEndId));
                    batchItemBuilder.add(tenantIdentifier, items, "datatables/dt_savings_transaction_details/$.resourceId", withdrawAmountDetailsBody, true);
                } else {  // CURRENT account executes withdrawal and details in one step
                    String withdrawFeeTransactionBody = painMapper.writeValueAsString(new CurrentAccountTransactionBody(transactionFeeAmount, FORMAT, moneyInOutWorker.getLocale(), withdrawFeePaymentTypeId, currency, List.of(
                            new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                                    debtorIban,
                                    camt053,
                                    transactionFeeInternalCorrelationId,
                                    creditorName,
                                    creditorIban,
                                    transactionGroupId,
                                    transactionId,
                                    endToEndId,
                                    transactionFeeCategoryPurposeCode,
                                    paymentScheme,
                                    unstructured,
                                    disposalAccountAmsId,
                                    conversionAccountAmsId,
                                    transactionCreationChannel,
                                    partnerAccountSecondaryIdentifier,
                                    null,
                                    valueDated,
                                    direction
                            )), "dt_current_transaction_details")))
                    );
                    batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, withdrawFeeTransactionBody, false);
                }
            }

            String lastTransactionId = moneyInOutWorker.doBatch(items, tenantIdentifier, transactionGroupId, disposalAccountAmsId, conversionAccountAmsId, internalCorrelationId, "revertInAms");

            log.debug("querying running balance for account {}", disposalAccountAmsId);
            BigDecimal runningBalanceDerived = accountProductType.equalsIgnoreCase("SAVINGS") ?
                    queryRunningBalance(internalCorrelationId, disposalAccountAmsId, tenantIdentifier, lastTransactionId, apiPath)
                    : queryCurrentAccountBalance(apiPath, internalCorrelationId, disposalAccountAmsId, tenantIdentifier);

            return Map.of("availableBalance", runningBalanceDerived.toString());

        } catch (JsonProcessingException e) {
            throw new RuntimeException("failed in revert", e);
        } finally {
            MDC.remove("internalCorrelationId");
        }
    }

    protected BigDecimal queryCurrentAccountBalance(String apiPath, String internalCorrelationId, String disposalAccountAmsId, String tenantIdentifier) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", authTokenHelper.generateAuthToken());
        headers.set("Fineract-Platform-TenantId", tenantIdentifier);
        headers.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
        String url = String.format("%s/%s/%s", moneyInOutWorker.getFineractApiUrl(), apiPath, disposalAccountAmsId);

        return eventService.auditedEvent(event -> event.setSourceModule("ams_connector")
                        .setEvent("revertInAms - availableBalanceRequest")
                        .setEventLogLevel(EventLogLevel.INFO)
                        .setEventType(EventType.audit)
                        .setPayload("GET " + url)
                        .setCorrelationIds(Map.of("CorrelationId", internalCorrelationId)), event ->
                {
                    ResponseEntity<String> responseEntity = moneyInOutWorker.getRestTemplate().exchange(url, HttpMethod.GET, new HttpEntity<>(headers), String.class);
                    String responseData = responseEntity.getBody();
                    event.setPayload(event.getPayload() + " -> " + responseData);
                    return new JSONObject(responseData).getBigDecimal("accountBalance");
                }
        );
    }

    @NotNull
    private BigDecimal queryRunningBalance(String internalCorrelationId, String disposalAccountAmsId, String tenantIdentifier, String lastTransactionId, String apiPath) {
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
        TransactionQueryPayload tqResponse = moneyInOutWorker.getRestTemplate().exchange(
                        String.format("%s/%s%s/transactions/query", moneyInOutWorker.getFineractApiUrl(), apiPath, disposalAccountAmsId),
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
                                      @Variable String transactionId,
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
                        transactionId,
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
                                       String transactionId,
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
            String depositAmountOperation = "revertWithoutFeeInAms.DisposalAccount.DepositTransactionAmount";
            String depositAmountConfigOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
            String depositPaymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, depositAmountConfigOperationKey);
            String depositPaymentTypeCode = tenantConfigs.findResourceCode(tenantIdentifier, depositAmountConfigOperationKey);
            String direction = tenantConfigs.findResourceCode(tenantIdentifier, depositAmountConfigOperationKey);
            PaymentInstruction34 paymentInstruction = pain001.getDocument().getPaymentInformation().get(0);
            CreditTransferTransaction40 creditTransferTransaction = paymentInstruction.getCreditTransferTransactionInformation().get(0);
            String debtorIban = paymentInstruction.getDebtorAccount().getIdentification().getIban();
            String creditorName = creditTransferTransaction.getCreditor().getName();
            String creditorIban = creditTransferTransaction.getCreditorAccount().getIdentification().getIban();
            String creditorContactDetails = contactDetailsUtil.getId(creditTransferTransaction.getCreditor().getContactDetails());
            String unstructured = Optional.ofNullable(creditTransferTransaction.getRemittanceInformation()).map(RemittanceInformation16::getUnstructured).map(List::toString).orElse("");
            String endToEndId = creditTransferTransaction.getPaymentIdentification().getEndToEndIdentification();
            String partnerAccountSecondaryIdentifier = contactDetailsUtil.getId(creditTransferTransaction.getCreditor().getContactDetails());
            List<TransactionItem> items = new ArrayList<>();

            // STEP 1 - batch: deposit amount
            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                var bodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, depositPaymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                batchItemBuilder.add(tenantIdentifier, items, disposalAccountDepositRelativeUrl, bodyItem, false);
            } // CURRENT account sends a single call only at the details step

            // STEP 2 - batch: deposit details
            EntryTransaction10 entryTransaction10 = pain001Camt053Mapper.toCamt053Fragment(pain001.getDocument());
            batchItemBuilder.setAmount(entryTransaction10, amount, currency);
            entryTransaction10.setCreditDebitIndicator(CreditDebitCode.CRDT);

            BankToCustomerStatementV08 camt053Entry = pain001Camt053Mapper.toCamt053Entry(pain001.getDocument());
            ReportEntry10 reportEntry = camt053Entry.getStatement().get(0).getEntry().get(0);
            reportEntry.setCreditDebitIndicator(CreditDebitCode.CRDT);
            reportEntry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            String depositCamt053 = serializationHelper.writeCamt053AsString(accountProductType, reportEntry);

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                var camt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, depositCamt053, debtorIban, depositPaymentTypeCode, transactionGroupId, creditorName, creditorIban, null, creditorContactDetails, unstructured, transactionCategoryPurposeCode, paymentScheme, conversionAccountAmsId, disposalAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, items, "datatables/dt_savings_transaction_details/$.resourceId", camt053Body, true);
            } else {
                var camt053Body = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, moneyInOutWorker.getLocale(), depositPaymentTypeId, currency, List.of(new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(debtorIban,
                        depositCamt053,
                        internalCorrelationId,
                        creditorName,
                        creditorIban,
                        transactionGroupId,
                        transactionId,
                        endToEndId,
                        transactionCategoryPurposeCode,
                        paymentScheme,
                        unstructured,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        null,
                        partnerAccountSecondaryIdentifier,
                        null,
                        valueDated,
                        direction)), "dt_current_transaction_details"))));
                batchItemBuilder.add(tenantIdentifier, items, disposalAccountDepositRelativeUrl, camt053Body, false);
            }

            // STEP 3 - batch: withdraw
            String withdrawAmountOperation = "revertWithoutFeeInAms.ConversionAccount.WithdrawTransactionAmount";
            String withdrawAmountConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
            var withdrawPaymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, withdrawAmountConfigOperationKey);
            var withdrawPaymentTypeCode = tenantConfigs.findResourceCode(tenantIdentifier, withdrawAmountConfigOperationKey);
            entryTransaction10.setCreditDebitIndicator(CreditDebitCode.DBIT);

            String withdrawCamt053 = serializationHelper.writeCamt053AsString(accountProductType, reportEntry);

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                String bodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, withdrawPaymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, bodyItem, false);

                var camt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, withdrawCamt053, debtorIban, withdrawPaymentTypeCode, transactionGroupId, creditorName, creditorIban, null, creditorContactDetails, unstructured, transactionCategoryPurposeCode, paymentScheme, conversionAccountAmsId, disposalAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, items, "datatables/dt_savings_transaction_details/$.resourceId", camt053Body, true);
            } else {
                var camt03Body = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, moneyInOutWorker.getLocale(), withdrawPaymentTypeId, currency, List.of(new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                        debtorIban,
                        withdrawCamt053,
                        internalCorrelationId,
                        creditorName,
                        creditorIban,
                        transactionGroupId,
                        transactionId,
                        endToEndId,
                        transactionCategoryPurposeCode,
                        paymentScheme,
                        unstructured,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        null,
                        partnerAccountSecondaryIdentifier,
                        null,
                        valueDated,
                        direction
                )), "dt_current_transaction_details"))));
                batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, camt03Body, false);
            }

            // STEP 4 - batch: withdraw fee
            if (!BigDecimal.ZERO.equals(transactionFeeAmount)) {
                log.debug("Withdrawing fee {} from conversion account {}", transactionFeeAmount, conversionAccountAmsId);

                String withdrawFeeOperation = "revertWithoutFeeInAms.ConversionAccount.WithdrawTransactionFee";
                String withdrawFeeConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawFeeOperation);
                withdrawPaymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, withdrawFeeConfigOperationKey);
                withdrawPaymentTypeCode = tenantConfigs.findResourceCode(tenantIdentifier, withdrawFeeConfigOperationKey);
                entryTransaction10.getSupplementaryData().clear();
                pain001Camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001.getDocument(), entryTransaction10, transactionFeeCategoryPurposeCode);
                pain001Camt053Mapper.refillOtherIdentification(pain001.getDocument(), entryTransaction10);

                String withdrawFeeCamt053 = serializationHelper.writeCamt053AsString(accountProductType, reportEntry);

                if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                    var bodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, transactionFeeAmount, withdrawPaymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                    batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, bodyItem, false);

                    var camt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, withdrawFeeCamt053, debtorIban, withdrawPaymentTypeCode, transactionGroupId, creditorName, creditorIban, null, creditorContactDetails, unstructured, transactionFeeCategoryPurposeCode, paymentScheme, conversionAccountAmsId, disposalAccountAmsId, endToEndId));
                    batchItemBuilder.add(tenantIdentifier, items, "datatables/dt_savings_transaction_details/$.resourceId", camt053Body, true);
                } else {
                    var body = painMapper.writeValueAsString(new CurrentAccountTransactionBody(transactionFeeAmount, FORMAT, moneyInOutWorker.getLocale(), withdrawPaymentTypeId, currency, List.of(
                            new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                                    debtorIban,
                                    withdrawFeeCamt053,
                                    internalCorrelationId,
                                    creditorName,
                                    creditorIban,
                                    transactionGroupId,
                                    transactionId,
                                    endToEndId,
                                    transactionFeeCategoryPurposeCode,
                                    paymentScheme,
                                    unstructured,
                                    conversionAccountAmsId,
                                    disposalAccountAmsId,
                                    null,
                                    partnerAccountSecondaryIdentifier,
                                    null,
                                    valueDated,
                                    direction
                            )), "dt_current_transaction_details")
                    )));
                    batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawRelativeUrl, body, false);
                }
            }

            moneyInOutWorker.doBatch(items, tenantIdentifier, transactionGroupId, disposalAccountAmsId, conversionAccountAmsId, internalCorrelationId, "revertWithoutFeeInAms");
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
                                                @Variable String currency,
                                                @Variable String conversionAccountAmsId,
                                                @Variable String disposalAccountAmsId,
                                                @Variable String tenantIdentifier,
                                                @Variable String transactionGroupId,
                                                @Variable String transactionId,
                                                @Variable String paymentScheme,
                                                @Variable String transactionCategoryPurposeCode,
                                                @Variable String originalPacs004,
                                                @Variable String originalPacs002,
                                                @Variable String internalCorrelationId,
                                                @Variable String accountProductType,
                                                @Variable String valueDated
    ) {
        log.info("depositTheAmountOnDisposalInAms");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "depositTheAmountOnDisposalInAms",
                        null,
                        null,
                        eventBuilder),
                eventBuilder -> depositTheAmountOnDisposalInAms(amount,
                        currency,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        tenantIdentifier,
                        transactionGroupId,
                        transactionId,
                        paymentScheme,
                        transactionCategoryPurposeCode,
                        originalPacs004,
                        originalPacs002,
                        internalCorrelationId,
                        accountProductType,
                        Boolean.parseBoolean(Optional.ofNullable(valueDated).orElse("false"))
                ));
    }

    private Void depositTheAmountOnDisposalInAms(BigDecimal amount,
                                                 String currency,
                                                 String conversionAccountAmsId,
                                                 String disposalAccountAmsId,
                                                 String tenantIdentifier,
                                                 String transactionGroupId,
                                                 String transactionId,
                                                 String paymentScheme,
                                                 String transactionCategoryPurposeCode,
                                                 String originalPacs004,
                                                 String originalPacs002,
                                                 String internalCorrelationId,
                                                 String accountProductType,
                                                 boolean valueDated) {
        try {
            // STEP 0 - collect / extract information
            String transactionDate = LocalDate.now().format(DateTimeFormatter.ofPattern(FORMAT));
            String apiPath = accountProductType.equalsIgnoreCase("SAVINGS") ? incomingMoneyApi.substring(1) : currentAccountApi.substring(1);
            String disposalAccountDepositRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, disposalAccountAmsId, "deposit");
            String depositAmountOperation = "depositTheAmountOnDisposalInAms.DisposalAccount.DepositTransactionAmount";
            String depositAmountConfigOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
            String depositPaymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, depositAmountConfigOperationKey);
            String depositPaymentTypeCode = tenantConfigs.findResourceCode(tenantIdentifier, depositAmountConfigOperationKey);
            String direction = tenantConfigs.findDirection(tenantIdentifier, depositAmountConfigOperationKey);
            String withdrawRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, conversionAccountAmsId, "withdrawal");
            String withdrawAmountOperation = "depositTheAmountOnDisposalInAms.ConversionAccount.WithdrawTransactionAmount";
            String withdrawAmountConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
            String withdrawPaymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, withdrawAmountConfigOperationKey);
            String withdrawPaymentTypeCode = tenantConfigs.findResourceCode(tenantIdentifier, withdrawAmountConfigOperationKey);

            iso.std.iso._20022.tech.xsd.pacs_004_001.Document pacs004 = jaxbUtils.unmarshalPacs004(originalPacs004);
            iso.std.iso._20022.tech.xsd.pacs_002_001.Document pacs002 = jaxbUtils.unmarshalPacs002(originalPacs002);
            PaymentTransactionInformation27 paymentTransactionInformation = pacs004.getPmtRtr().getTxInf().get(0);
            String unstructured = Optional.ofNullable(paymentTransactionInformation.getOrgnlTxRef().getRmtInf())
                    .map(RemittanceInformation5::getUstrd).map(List::toString).orElse("");
            List<TransactionItem> items = new ArrayList<>();

            // STEP 1 - deposit amount
            BankToCustomerStatementV08 camt053Object = pacs004Camt053Mapper.convert(pacs004, new BankToCustomerStatementV08()
                    .withStatement(List.of(new AccountStatement9()
                            .withEntry(List.of(new ReportEntry10()
                                    .withEntryDetails(List.of(new EntryDetails9()
                                            .withTransactionDetails(List.of(new EntryTransaction10())))))))));
            ReportEntry10 camt053Entry = camt053Object.getStatement().get(0).getEntry().get(0);
            XMLGregorianCalendar accptncDtTm = pacs002.getFIToFIPmtStsRpt().getTxInfAndSts().get(0).getAccptncDtTm();
            if (accptncDtTm != null) {
                ZoneId zoneId = TimeZone.getTimeZone("Europe/Budapest").toZoneId();
                ZonedDateTime zonedDateTime = accptncDtTm.toGregorianCalendar().toZonedDateTime().withZoneSameInstant(zoneId);
                var acceptanceDateWithZone = DatatypeFactory.newDefaultInstance().newXMLGregorianCalendar(GregorianCalendar.from(zonedDateTime));
                camt053Entry.getValueDate().setAdditionalProperty("Date", acceptanceDateWithZone);
            }
            EntryTransaction10 entryTransaction10 = camt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0);
            batchItemBuilder.setAmount(entryTransaction10, amount, currency);

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                var bodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, depositPaymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                batchItemBuilder.add(tenantIdentifier, items, disposalAccountDepositRelativeUrl, bodyItem, false);

                entryTransaction10.setAdditionalTransactionInformation(depositPaymentTypeCode);
            }
            entryTransaction10.setCreditDebitIndicator(CreditDebitCode.CRDT);
            camt053Entry.setCreditDebitIndicator(CreditDebitCode.CRDT);
            camt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));

            String camt053 = serializeCamt053orFragment(accountProductType, entryTransaction10, camt053Entry);
            String creditorIban = paymentTransactionInformation.getOrgnlTxRef().getCdtrAcct().getId().getIBAN();
            String debtorName = paymentTransactionInformation.getOrgnlTxRef().getDbtr().getNm();
            String debtorIban = paymentTransactionInformation.getOrgnlTxRef().getDbtrAcct().getId().getIBAN();
            String debtorContactDetails = contactDetailsUtil.getId(paymentTransactionInformation.getOrgnlTxRef().getDbtr().getCtctDtls());
            String endToEndId = paymentTransactionInformation.getOrgnlEndToEndId();

            // STEP 2 - deposit details
            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                var camt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, camt053, creditorIban, depositPaymentTypeCode, internalCorrelationId, debtorName, debtorIban, null, debtorContactDetails, unstructured, transactionCategoryPurposeCode, paymentScheme, null, disposalAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, items, "datatables/dt_savings_transaction_details/$.resourceId", camt053Body, true);

                entryTransaction10.setAdditionalTransactionInformation(withdrawPaymentTypeCode);
            } else {
                var camt053Body = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, moneyInOutWorker.getLocale(), depositPaymentTypeId, currency, List.of(new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                        creditorIban,
                        camt053,
                        internalCorrelationId,
                        debtorName,
                        debtorIban,
                        transactionGroupId,
                        transactionId,
                        endToEndId,
                        transactionCategoryPurposeCode,
                        paymentScheme,
                        unstructured,
                        null,
                        disposalAccountAmsId,
                        null,
                        debtorContactDetails,
                        null,
                        valueDated,
                        direction
                )), "dt_current_transaction_details"))));
                batchItemBuilder.add(tenantIdentifier, items, disposalAccountDepositRelativeUrl, camt053Body, false);
            }

            entryTransaction10.setCreditDebitIndicator(CreditDebitCode.DBIT);
            camt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
            camt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            camt053 = serializeCamt053orFragment(accountProductType, entryTransaction10, camt053Entry);

            // STEP 3 - withdraw amount and details
            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                var bodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, withdrawPaymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                batchItemBuilder.add(tenantIdentifier, items, withdrawRelativeUrl, bodyItem, false);

                var camt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, camt053, creditorIban, withdrawPaymentTypeCode, internalCorrelationId, debtorName, debtorIban, null, debtorContactDetails, unstructured, transactionCategoryPurposeCode, paymentScheme, null, disposalAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, items, "datatables/dt_savings_transaction_details/$.resourceId", camt053Body, true);
            } else {
                var camt053Body = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, moneyInOutWorker.getLocale(), withdrawPaymentTypeId, currency, List.of(new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                        creditorIban,
                        camt053,
                        internalCorrelationId,
                        debtorName,
                        debtorIban,
                        transactionGroupId,
                        transactionId,
                        endToEndId,
                        transactionCategoryPurposeCode,
                        paymentScheme,
                        unstructured,
                        null,
                        disposalAccountAmsId,
                        null,
                        debtorContactDetails,
                        null,
                        valueDated,
                        direction
                )), "dt_current_transaction_details"))));
                batchItemBuilder.add(tenantIdentifier, items, withdrawRelativeUrl, camt053Body, false);
            }

            moneyInOutWorker.doBatch(items, tenantIdentifier, transactionGroupId, disposalAccountAmsId, conversionAccountAmsId, internalCorrelationId, "depositTheAmountOnDisposalInAms");
        } catch (JAXBException | JsonProcessingException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
        return null;
    }
}

