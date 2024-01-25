package org.mifos.connector.ams.zeebe.workers.bookamount;

import com.baasflow.commons.events.Event;
import com.baasflow.commons.events.EventService;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import hu.dpc.rt.utils.converter.AccountSchemeName1Choice;
import hu.dpc.rt.utils.converter.GenericAccountIdentification1;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import iso.std.iso._20022.tech.json.camt_053_001.*;
import iso.std.iso._20022.tech.json.camt_053_001.ActiveOrHistoricCurrencyAndAmountRange2.CreditDebitCode;
import iso.std.iso._20022.tech.json.pain_001_001.CustomerCreditTransferInitiationV10;
import iso.std.iso._20022.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;
import lombok.extern.slf4j.Slf4j;
import org.mifos.connector.ams.fineract.Config;
import org.mifos.connector.ams.fineract.ConfigFactory;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.log.LogInternalCorrelationId;
import org.mifos.connector.ams.log.TraceZeebeArguments;
import org.mifos.connector.ams.mapstruct.Pain001Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Component
@Slf4j
public class OnUsTransferWorker extends AbstractMoneyInOutWorker {

    @Autowired
    private Pain001Camt053Mapper camt053Mapper;

    @Value("${fineract.incoming-money-api}")
    protected String incomingMoneyApi;

    @Autowired
    private ConfigFactory paymentTypeConfigFactory;

    @Autowired
    private BatchItemBuilder batchItemBuilder;

    @Autowired
    private ContactDetailsUtil contactDetailsUtil;

    @Autowired
    private AuthTokenHelper authTokenHelper;

    @Autowired
    private NotificationHelper notificationHelper;

    @Autowired
    private EventService eventService;

    private ObjectMapper objectMapper = new ObjectMapper() {
        private static final long serialVersionUID = 1L;

        {
            registerModule(new AfterburnerModule());
            registerModule(new JavaTimeModule());
            configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
            setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
                    .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }
    };

    private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);

    @JobWorker
    @LogInternalCorrelationId
    @TraceZeebeArguments
    public Map<String, Object> transferTheAmountBetweenDisposalAccounts(JobClient jobClient,
                                                                        ActivatedJob activatedJob,
                                                                        @Variable String internalCorrelationId,
                                                                        @Variable String paymentScheme,
                                                                        @Variable String originalPain001,
                                                                        @Variable BigDecimal amount,
                                                                        @Variable Integer creditorDisposalAccountAmsId,
                                                                        @Variable Integer debtorDisposalAccountAmsId,
                                                                        @Variable Integer debtorConversionAccountAmsId,
                                                                        @Variable BigDecimal transactionFeeAmount,
                                                                        @Variable String tenantIdentifier,
                                                                        @Variable String transactionGroupId,
                                                                        @Variable String transactionCategoryPurposeCode,
                                                                        @Variable String transactionFeeCategoryPurposeCode,
                                                                        @Variable String transactionFeeInternalCorrelationId,
                                                                        @Variable String creditorIban,
                                                                        @Variable String debtorIban,
                                                                        @Variable String debtorInternalAccountId,
                                                                        @Variable String creditorInternalAccountId) {
        log.info("transferTheAmountBetweenDisposalAccounts");
        return eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "transferTheAmountBetweenDisposalAccounts", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> transferTheAmountBetweenDisposalAccounts(internalCorrelationId,
                        paymentScheme,
                        originalPain001,
                        amount,
                        creditorDisposalAccountAmsId,
                        debtorDisposalAccountAmsId,
                        debtorConversionAccountAmsId,
                        transactionFeeAmount,
                        tenantIdentifier,
                        transactionGroupId,
                        transactionCategoryPurposeCode,
                        transactionFeeCategoryPurposeCode,
                        transactionFeeInternalCorrelationId,
                        creditorIban,
                        debtorIban,
                        debtorInternalAccountId,
                        creditorInternalAccountId,
                        eventBuilder));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> transferTheAmountBetweenDisposalAccounts(String internalCorrelationId,
                                                                         String paymentScheme,
                                                                         String originalPain001,
                                                                         BigDecimal amount,
                                                                         Integer creditorDisposalAccountAmsId,
                                                                         Integer debtorDisposalAccountAmsId,
                                                                         Integer debtorConversionAccountAmsId,
                                                                         BigDecimal transactionFeeAmount,
                                                                         String tenantIdentifier,
                                                                         String transactionGroupId,
                                                                         String transactionCategoryPurposeCode,
                                                                         String transactionFeeCategoryPurposeCode,
                                                                         String transactionFeeInternalCorrelationId,
                                                                         String creditorIban,
                                                                         String debtorIban,
                                                                         String debtorInternalAccountId,
                                                                         String creditorInternalAccountId,
                                                                         Event.Builder eventBuilder) {
        try {

            log.debug("Incoming pain.001: {}", originalPain001);

            Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
            objectMapper.setSerializationInclusion(Include.NON_NULL);
            Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = objectMapper.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);

            BankToCustomerStatementV08 convertedStatement = camt053Mapper.toCamt053Entry(pain001.getDocument());
            ReportEntry10 convertedcamt053Entry = convertedStatement.getStatement().get(0).getEntry().get(0);
            String currency = pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getAmount().getInstructedAmount().getCurrency();
            AmountAndCurrencyExchangeDetails3 withAmountAndCurrency = new AmountAndCurrencyExchangeDetails3()
                    .withAmount(new ActiveOrHistoricCurrencyAndAmount()
                            .withAmount(amount.add(transactionFeeAmount))
                            .withCurrency(currency));
            EntryTransaction10 transactionDetails = convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0);
            transactionDetails.withAmountDetails(
                    new AmountAndCurrencyExchange3()
                            .withTransactionAmount(withAmountAndCurrency));

            transactionDetails.getSupplementaryData().clear();
            refillOtherId(debtorInternalAccountId, creditorInternalAccountId, transactionDetails);

            String interbankSettlementDate = LocalDate.now().format(PATTERN);

            boolean hasFee = !BigDecimal.ZERO.equals(transactionFeeAmount);

            List<TransactionItem> items = new ArrayList<>();

            String holdTransactionUrl = String.format("%s%d/transactions?command=holdAmount", incomingMoneyApi.substring(1), debtorDisposalAccountAmsId);

            Integer outHoldReasonId = paymentTypeConfig.findPaymentTypeIdByOperation(String.format("%s.%s", paymentScheme, "outHoldReasonId"));
            HoldAmountBody body = new HoldAmountBody(
                    interbankSettlementDate,
                    hasFee ? amount.add(transactionFeeAmount) : amount,
                    outHoldReasonId,
                    locale,
                    FORMAT
            );

            objectMapper.setSerializationInclusion(Include.NON_NULL);
            String bodyItem = objectMapper.writeValueAsString(body);

            batchItemBuilder.add(tenantIdentifier, items, holdTransactionUrl, bodyItem, false);

            String camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";

            String partnerName = pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getName();
            String partnerAccountIban = pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditorAccount().getIdentification().getIban();
            String partnerAccountSecondaryIdentifier = contactDetailsUtil.getId(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getContactDetails());
            String unstructured = Optional.ofNullable(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getRemittanceInformation())
                    .map(iso.std.iso._20022.tech.json.pain_001_001.RemittanceInformation16::getUnstructured).map(List::toString).orElse("");

            addDetails(tenantIdentifier, internalCorrelationId, transactionCategoryPurposeCode, internalCorrelationId,
                    objectMapper, batchItemBuilder, items, pain001.getDocument(), convertedcamt053Entry, camt053RelativeUrl, debtorIban,
                    paymentTypeConfig, paymentScheme, null, partnerName, partnerAccountIban,
                    partnerAccountSecondaryIdentifier, unstructured, null, null);

            Long lastHoldTransactionId = holdBatch(items, tenantIdentifier, debtorDisposalAccountAmsId, creditorDisposalAccountAmsId, internalCorrelationId, "transferToConversionAccountInAms");

            HttpHeaders httpHeaders = new HttpHeaders();
            httpHeaders.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
            httpHeaders.set("Authorization", authTokenHelper.generateAuthToken());
            httpHeaders.set("Fineract-Platform-TenantId", tenantIdentifier);
            LinkedHashMap<String, Object> accountDetails = restTemplate.exchange(
                            String.format("%s/%s%d", fineractApiUrl, incomingMoneyApi.substring(1), debtorDisposalAccountAmsId),
                            HttpMethod.GET,
                            new HttpEntity<>(httpHeaders),
                            LinkedHashMap.class)
                    .getBody();
            LinkedHashMap<String, Object> summary = (LinkedHashMap<String, Object>) accountDetails.get("summary");
            BigDecimal availableBalance = new BigDecimal(summary.get("availableBalance").toString());
            if (availableBalance.signum() < 0) {
                restTemplate.exchange(
                        String.format("%s/%ssavingsaccounts/%d/transactions/%d?command=releaseAmount", fineractApiUrl, incomingMoneyApi.substring(1), debtorDisposalAccountAmsId, lastHoldTransactionId),
                        HttpMethod.POST,
                        new HttpEntity<>(httpHeaders),
                        Object.class
                );
                throw new ZeebeBpmnError("Error_InsufficientFunds", "Insufficient funds");
            }

            items.clear();

            String releaseTransactionUrl = String.format("%s%d/transactions/%d?command=releaseAmount", incomingMoneyApi.substring(1), debtorDisposalAccountAmsId, lastHoldTransactionId);
            batchItemBuilder.add(tenantIdentifier, items, releaseTransactionUrl, null, false);
            String releaseAmountOperation = "transferTheAmountBetweenDisposalAccounts.Debtor.DisposalAccount.ReleaseTransactionAmount";
            addDetails(tenantIdentifier, internalCorrelationId, transactionCategoryPurposeCode, internalCorrelationId,
                    objectMapper, batchItemBuilder, items, pain001.getDocument(), convertedcamt053Entry, camt053RelativeUrl, debtorIban,
                    paymentTypeConfig, paymentScheme, releaseAmountOperation, partnerName, partnerAccountIban,
                    partnerAccountSecondaryIdentifier, unstructured, null, null);

            String debtorDisposalWithdrawalRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), debtorDisposalAccountAmsId, "withdrawal");

            String withdrawAmountOperation = "transferTheAmountBetweenDisposalAccounts.Debtor.DisposalAccount.WithdrawTransactionAmount";
            String withdrawAmountConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
            Integer paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawAmountConfigOperationKey);
            String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawAmountConfigOperationKey);

            TransactionBody transactionBody = new TransactionBody(
                    interbankSettlementDate,
                    amount,
                    paymentTypeId,
                    "",
                    FORMAT,
                    locale);

            bodyItem = objectMapper.writeValueAsString(transactionBody);


            batchItemBuilder.add(tenantIdentifier, items, debtorDisposalWithdrawalRelativeUrl, bodyItem, false);

            transactionDetails.getAmountDetails().getTransactionAmount().getAmount().setAmount(amount);
            transactionDetails.setAdditionalTransactionInformation(paymentTypeCode);
            transactionDetails.setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedcamt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedcamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001.getDocument(), convertedcamt053Entry, transactionCategoryPurposeCode);
            transactionDetails.getSupplementaryData().clear();
            refillOtherId(debtorInternalAccountId, creditorInternalAccountId, transactionDetails);
            String camt053Entry = objectMapper.writeValueAsString(convertedcamt053Entry);

            camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";

            DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
                    internalCorrelationId,
                    camt053Entry,
                    pain001.getDocument().getPaymentInformation().get(0).getDebtorAccount().getIdentification().getIban(),
                    paymentTypeCode,
                    transactionGroupId,
                    pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getName(),
                    partnerAccountIban,
                    partnerAccountIban.substring(partnerAccountIban.length() - 8),
                    contactDetailsUtil.getId(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getContactDetails()),
                    Optional.ofNullable(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getRemittanceInformation())
                            .map(iso.std.iso._20022.tech.json.pain_001_001.RemittanceInformation16::getUnstructured).map(List::toString).orElse(""),
                    transactionCategoryPurposeCode,
                    paymentScheme,
                    debtorDisposalAccountAmsId,
                    creditorDisposalAccountAmsId,
                    pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getPaymentIdentification().getEndToEndIdentification());

            String camt053Body = objectMapper.writeValueAsString(td);

            batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);

            if (hasFee) {
                String withdrawFeeDisposalOperation = "transferTheAmountBetweenDisposalAccounts.Debtor.DisposalAccount.WithdrawTransactionFee";
                String withdrawFeeDisposalConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawFeeDisposalOperation);
                paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawFeeDisposalConfigOperationKey);
                paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawFeeDisposalConfigOperationKey);

                transactionBody = new TransactionBody(
                        interbankSettlementDate,
                        transactionFeeAmount,
                        paymentTypeId,
                        "",
                        FORMAT,
                        locale);

                bodyItem = objectMapper.writeValueAsString(transactionBody);

                batchItemBuilder.add(tenantIdentifier, items, debtorDisposalWithdrawalRelativeUrl, bodyItem, false);

                transactionDetails.getSupplementaryData().get(0).getEnvelope().setAdditionalProperty("InternalCorrelationId", transactionFeeInternalCorrelationId);
                transactionDetails.getAmountDetails().getTransactionAmount().getAmount().setAmount(transactionFeeAmount);
                transactionDetails.setAdditionalTransactionInformation(paymentTypeCode);
                transactionDetails.getSupplementaryData().clear();
                camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001.getDocument(), convertedcamt053Entry, transactionFeeCategoryPurposeCode);
                refillOtherId(debtorInternalAccountId, creditorInternalAccountId, transactionDetails);
                camt053Entry = objectMapper.writeValueAsString(convertedcamt053Entry);

                td = new DtSavingsTransactionDetails(
                        transactionFeeInternalCorrelationId,
                        camt053Entry,
                        pain001.getDocument().getPaymentInformation().get(0).getDebtorAccount().getIdentification().getIban(),
                        paymentTypeCode,
                        transactionGroupId,
                        pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getName(),
                        pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditorAccount().getIdentification().getIban(),
                        creditorIban.substring(creditorIban.length() - 8),
                        contactDetailsUtil.getId(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getContactDetails()),
                        Optional.ofNullable(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getRemittanceInformation())
                                .map(iso.std.iso._20022.tech.json.pain_001_001.RemittanceInformation16::getUnstructured).map(List::toString).orElse(""),
                        transactionFeeCategoryPurposeCode,
                        paymentScheme,
                        debtorDisposalAccountAmsId,
                        debtorConversionAccountAmsId,
                        pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getPaymentIdentification().getEndToEndIdentification());

                camt053Body = objectMapper.writeValueAsString(td);
                batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);


                String depositFeeOperation = "transferTheAmountBetweenDisposalAccounts.Debtor.ConversionAccount.DepositTransactionFee";
                String depositFeeConfigOperationKey = String.format("%s.%s", paymentScheme, depositFeeOperation);
                paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(depositFeeConfigOperationKey);
                paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(depositFeeConfigOperationKey);
                transactionDetails.setAdditionalTransactionInformation(paymentTypeCode);
                transactionDetails.getSupplementaryData().clear();
                transactionDetails.setCreditDebitIndicator(CreditDebitCode.CRDT);
                convertedcamt053Entry.setCreditDebitIndicator(CreditDebitCode.CRDT);
                convertedcamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
                camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001.getDocument(), convertedcamt053Entry, transactionFeeCategoryPurposeCode);
                refillOtherId(debtorInternalAccountId, creditorInternalAccountId, transactionDetails);
                camt053Entry = objectMapper.writeValueAsString(convertedcamt053Entry);

                transactionBody = new TransactionBody(
                        interbankSettlementDate,
                        transactionFeeAmount,
                        paymentTypeId,
                        "",
                        FORMAT,
                        locale);

                bodyItem = objectMapper.writeValueAsString(transactionBody);

                String debtorConversionDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), debtorConversionAccountAmsId, "deposit");

                batchItemBuilder.add(tenantIdentifier, items, debtorConversionDepositRelativeUrl, bodyItem, false);

                td = new DtSavingsTransactionDetails(
                        transactionFeeInternalCorrelationId,
                        camt053Entry,
                        pain001.getDocument().getPaymentInformation().get(0).getDebtorAccount().getIdentification().getIban(),
                        paymentTypeCode,
                        transactionGroupId,
                        pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getName(),
                        pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditorAccount().getIdentification().getIban(),
                        partnerAccountIban.substring(partnerAccountIban.length() - 8),
                        contactDetailsUtil.getId(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getContactDetails()),
                        Optional.ofNullable(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getRemittanceInformation())
                                .map(iso.std.iso._20022.tech.json.pain_001_001.RemittanceInformation16::getUnstructured).map(List::toString).orElse(""),
                        transactionFeeCategoryPurposeCode,
                        paymentScheme,
                        debtorDisposalAccountAmsId,
                        debtorConversionAccountAmsId,
                        pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getPaymentIdentification().getEndToEndIdentification());

                camt053Body = objectMapper.writeValueAsString(td);
                batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);
            }

            String depositAmountOperation = "transferTheAmountBetweenDisposalAccounts.Creditor.DisposalAccount.DepositTransactionAmount";
            String depositAmountConfigOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
            paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(depositAmountConfigOperationKey);
            paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(depositAmountConfigOperationKey);
            transactionDetails.setAdditionalTransactionInformation(paymentTypeCode);
            transactionDetails.getSupplementaryData().clear();
            camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001.getDocument(), convertedcamt053Entry, transactionCategoryPurposeCode);
            refillOtherId(debtorInternalAccountId, creditorInternalAccountId, transactionDetails);
            transactionBody = new TransactionBody(
                    interbankSettlementDate,
                    amount,
                    paymentTypeId,
                    "",
                    FORMAT,
                    locale);

            bodyItem = objectMapper.writeValueAsString(transactionBody);

            String creditorDisposalDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), creditorDisposalAccountAmsId, "deposit");

            batchItemBuilder.add(tenantIdentifier, items, creditorDisposalDepositRelativeUrl, bodyItem, false);

            transactionDetails.getSupplementaryData().get(0).getEnvelope().setAdditionalProperty("InternalCorrelationId", internalCorrelationId);
            transactionDetails.getAmountDetails().getTransactionAmount().getAmount().setAmount(amount);
            transactionDetails.setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedcamt053Entry.setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedcamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            camt053Entry = objectMapper.writeValueAsString(convertedcamt053Entry);

            String debtorName = pain001.getDocument().getPaymentInformation().get(0).getDebtor().getName();
            td = new DtSavingsTransactionDetails(
                    internalCorrelationId,
                    camt053Entry,
                    pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditorAccount().getIdentification().getIban(),
                    paymentTypeCode,
                    transactionGroupId,
                    debtorName,
                    pain001.getDocument().getPaymentInformation().get(0).getDebtorAccount().getIdentification().getIban(),
                    debtorIban.substring(debtorIban.length() - 8),
                    contactDetailsUtil.getId(pain001.getDocument().getPaymentInformation().get(0).getDebtor().getContactDetails()),
                    Optional.ofNullable(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getRemittanceInformation())
                            .map(iso.std.iso._20022.tech.json.pain_001_001.RemittanceInformation16::getUnstructured).map(List::toString).orElse(""),
                    transactionCategoryPurposeCode,
                    paymentScheme,
                    debtorDisposalAccountAmsId,
                    creditorDisposalAccountAmsId,
                    pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getPaymentIdentification().getEndToEndIdentification());

            camt053Body = objectMapper.writeValueAsString(td);
            batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);


            if (hasFee) {
                String withdrawFeeConversionOperation = "transferTheAmountBetweenDisposalAccounts.Debtor.ConversionAccount.WithdrawTransactionFee";
                String withdrawFeeConversionConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawFeeConversionOperation);
                paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawFeeConversionConfigOperationKey);
                paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawFeeConversionConfigOperationKey);
                transactionDetails.setAdditionalTransactionInformation(paymentTypeCode);
                transactionDetails.getSupplementaryData().clear();
                transactionDetails.setCreditDebitIndicator(CreditDebitCode.DBIT);
                convertedcamt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
                convertedcamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
                refillOtherId(debtorInternalAccountId, creditorInternalAccountId, transactionDetails);
                transactionBody = new TransactionBody(
                        interbankSettlementDate,
                        transactionFeeAmount,
                        paymentTypeId,
                        "",
                        FORMAT,
                        locale);

                bodyItem = objectMapper.writeValueAsString(transactionBody);

                String debtorConversionWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), debtorConversionAccountAmsId, "withdrawal");

                batchItemBuilder.add(tenantIdentifier, items, debtorConversionWithdrawRelativeUrl, bodyItem, false);

                transactionDetails.getAmountDetails().getTransactionAmount().getAmount().setAmount(transactionFeeAmount);
                transactionDetails.getSupplementaryData().clear();
                camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001.getDocument(), convertedcamt053Entry, transactionFeeCategoryPurposeCode);
                refillOtherId(debtorInternalAccountId, creditorInternalAccountId, transactionDetails);
                transactionDetails.getSupplementaryData().get(0).getEnvelope().setAdditionalProperty("InternalCorrelationId", transactionFeeInternalCorrelationId);
                camt053Entry = objectMapper.writeValueAsString(convertedcamt053Entry);

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
                        debtorConversionAccountAmsId,
                        null,
                        pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getPaymentIdentification().getEndToEndIdentification());

                camt053Body = objectMapper.writeValueAsString(td);

                batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);
            }

            doBatchOnUs(items,
                    tenantIdentifier,
                    debtorDisposalAccountAmsId,
                    debtorConversionAccountAmsId,
                    creditorDisposalAccountAmsId,
                    internalCorrelationId);

            notificationHelper.send("transferTheAmountBetweenDisposalAccounts", amount, currency, debtorName, paymentScheme, creditorIban);

            return Map.of("transactionDate", interbankSettlementDate);
        } catch (JsonProcessingException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("failed to create camt.053", e);
        } catch (ZeebeBpmnError e) {
            throw e;
        } catch (RuntimeException re) {
            throw re;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private void refillOtherId(String debtorInternalAccountId, String creditorInternalAccountId,
                               EntryTransaction10 transactionDetails) {
        boolean isWritten = false;
        for (SupplementaryData1 supplementaryData : transactionDetails.getSupplementaryData()) {
            if ("OrderManagerSupplementaryData".equalsIgnoreCase(supplementaryData.getPlaceAndName())) {
                SupplementaryDataEnvelope1 envelope = supplementaryData.getEnvelope();
                doRefillOtherId(debtorInternalAccountId, creditorInternalAccountId, transactionDetails, envelope);
                isWritten = true;
            }
        }

        if (!isWritten) {
            List<SupplementaryData1> sds = transactionDetails.getSupplementaryData();
            if (sds.isEmpty()) {
                sds.add(new SupplementaryData1());
            }
            SupplementaryData1 supplementaryData = sds.get(0);
            SupplementaryDataEnvelope1 envelope = supplementaryData.getEnvelope();
            if (envelope == null) {
                envelope = new SupplementaryDataEnvelope1();
                supplementaryData.setEnvelope(envelope);
            }

            doRefillOtherId(debtorInternalAccountId, creditorInternalAccountId, transactionDetails, envelope);
        }
    }

    @SuppressWarnings("unchecked")
    private void doRefillOtherId(String debtorInternalAccountId, String creditorInternalAccountId,
                                 EntryTransaction10 transactionDetails, SupplementaryDataEnvelope1 envelope) {
        LinkedHashMap<String, Object> otherIdentification = (LinkedHashMap<String, Object>) envelope.getAdditionalProperties().getOrDefault("OtherIdentification", new LinkedHashMap<>());
        envelope.setAdditionalProperty("OtherIdentification", otherIdentification);

        GenericAccountIdentification1 debtorAccountIdOther = (GenericAccountIdentification1) transactionDetails.getRelatedParties().getDebtorAccount().getIdentification().getAdditionalProperties().getOrDefault("Other", GenericAccountIdentification1.builder().build());
        debtorAccountIdOther.setId(debtorInternalAccountId);
        debtorAccountIdOther.setSchemeName(AccountSchemeName1Choice.builder().code("IAID").build());

        GenericAccountIdentification1 creditorAccountIdOther = (GenericAccountIdentification1) transactionDetails.getRelatedParties().getCreditorAccount().getIdentification().getAdditionalProperties().getOrDefault("Other", GenericAccountIdentification1.builder().build());
        creditorAccountIdOther.setId(creditorInternalAccountId);
        creditorAccountIdOther.setSchemeName(AccountSchemeName1Choice.builder().code("IAID").build());

        LinkedHashMap<String, Object> debtorAccountIdentificationOther = (LinkedHashMap<String, Object>) otherIdentification.getOrDefault("DebtorAccount", new LinkedHashMap<>());
        otherIdentification.put("DebtorAccount", debtorAccountIdentificationOther);
        LinkedHashMap<String, Object> daioAccountIdentification = (LinkedHashMap<String, Object>) debtorAccountIdentificationOther.getOrDefault("Identification", new LinkedHashMap<>());
        debtorAccountIdentificationOther.put("Identification", daioAccountIdentification);
        daioAccountIdentification.put("Other", debtorAccountIdOther);

        LinkedHashMap<String, Object> creditorAccountIdentificationOther = (LinkedHashMap<String, Object>) otherIdentification.getOrDefault("CreditorAccount", new LinkedHashMap<>());
        otherIdentification.put("CreditorAccount", creditorAccountIdentificationOther);
        LinkedHashMap<String, Object> caioAccountIdentification = (LinkedHashMap<String, Object>) creditorAccountIdentificationOther.getOrDefault("Identification", new LinkedHashMap<>());
        creditorAccountIdentificationOther.put("Identification", caioAccountIdentification);
        caioAccountIdentification.put("Other", creditorAccountIdOther);
    }

    private void addDetails(
            String tenantIdentifier,
            String transactionGroupId,
            String transactionFeeCategoryPurposeCode,
            String internalCorrelationId,
            ObjectMapper om,
            BatchItemBuilder batchItemBuilder,
            List<TransactionItem> items,
            CustomerCreditTransferInitiationV10 pain001,
            ReportEntry10 convertedcamt053Entry,
            String camt053RelativeUrl,
            String accountIban,
            Config paymentTypeConfig,
            String paymentScheme,
            String paymentTypeOperation,
            String partnerName,
            String partnerAccountIban,
            String partnerAccountSecondaryIdentifier,
            String unstructured,
            Integer sourceAmsAccountId,
            Integer targetAmsAccountId) throws JsonProcessingException {
        String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(String.format("%s.%s", paymentScheme, paymentTypeOperation));
        if (paymentTypeCode == null) {
            paymentTypeCode = "";
        }
        convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
        String camt053 = objectMapper.writeValueAsString(convertedcamt053Entry);
        DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
                internalCorrelationId,
                camt053,
                accountIban,
                paymentTypeCode,
                transactionGroupId,
                partnerName,
                partnerAccountIban,
                partnerAccountIban.substring(partnerAccountIban.length() - 8),
                partnerAccountSecondaryIdentifier,
                unstructured,
                transactionFeeCategoryPurposeCode,
                paymentScheme,
                sourceAmsAccountId,
                targetAmsAccountId,
                pain001.getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getPaymentIdentification().getEndToEndIdentification());

        String camt053Body = om.writeValueAsString(td);
        batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);
    }
}