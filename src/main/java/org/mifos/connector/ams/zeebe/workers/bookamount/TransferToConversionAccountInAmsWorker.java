package org.mifos.connector.ams.zeebe.workers.bookamount;

import com.baasflow.commons.events.EventService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import hu.dpc.rt.utils.converter.Camt056ToCamt053Converter;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import iso.std.iso._20022.tech.json.camt_053_001.*;
import iso.std.iso._20022.tech.json.camt_053_001.ActiveOrHistoricCurrencyAndAmountRange2.CreditDebitCode;
import iso.std.iso._20022.tech.json.pain_001_001.CreditTransferTransaction40;
import iso.std.iso._20022.tech.json.pain_001_001.CustomerCreditTransferInitiationV10;
import iso.std.iso._20022.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;
import iso.std.iso._20022.tech.json.pain_001_001.PaymentInstruction34;
import iso.std.iso._20022.tech.xsd.camt_056_001.OriginalTransactionReference13;
import iso.std.iso._20022.tech.xsd.camt_056_001.RemittanceInformation5;
import jakarta.xml.bind.JAXBException;
import lombok.extern.slf4j.Slf4j;
import org.mifos.connector.ams.common.SerializationHelper;
import org.mifos.connector.ams.fineract.TenantConfigs;
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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

import static org.mifos.connector.ams.zeebe.workers.bookamount.MoneyInOutWorker.FORMAT;

@Component
@Slf4j
public class TransferToConversionAccountInAmsWorker {

    @Autowired
    private Pain001Camt053Mapper camt053Mapper;

    @Value("${fineract.incoming-money-api}")
    protected String incomingMoneyApi;

    @Value("${fineract.current-account-api}")
    protected String currentAccountApi;

    @Autowired
    private TenantConfigs tenantConfigs;

    @Autowired
    private JAXBUtils jaxbUtils;

    @Autowired
    private AuthTokenHelper authTokenHelper;

    @Autowired
    private BatchItemBuilder batchItemBuilder;

    @Autowired
    private ContactDetailsUtil contactDetailsUtil;

    @Autowired
    private EventService eventService;

    @Autowired
    @Qualifier("painMapper")
    private ObjectMapper painMapper;

    @Autowired
    private SerializationHelper serializationHelper;

    @Autowired
    private MoneyInOutWorker moneyInOutWorker;

    private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);

    @JobWorker
    @LogInternalCorrelationId
    @TraceZeebeArguments
    public void transferToConversionAccountInAms(JobClient jobClient,
                                                 ActivatedJob activatedJob,
                                                 @Variable String transactionGroupId,
                                                 @Variable String transactionId,
                                                 @Variable String transactionCategoryPurposeCode,
                                                 @Variable String transactionFeeCategoryPurposeCode,
                                                 @Variable String originalPain001,
                                                 @Variable String internalCorrelationId,
                                                 @Variable BigDecimal amount,
                                                 @Variable String currency,
                                                 @Variable BigDecimal transactionFeeAmount,
                                                 @Variable String paymentScheme,
                                                 @Variable String disposalAccountAmsId,
                                                 @Variable String conversionAccountAmsId,
                                                 @Variable String tenantIdentifier,
                                                 @Variable String transactionFeeInternalCorrelationId,
                                                 @Variable String accountProductType,
                                                 @Variable String valueDated
    ) {
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "transferToConversionAccountInAms", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> transferToConversionAccountInAms(
                        transactionGroupId,
                        transactionId,
                        transactionCategoryPurposeCode,
                        transactionFeeCategoryPurposeCode,
                        originalPain001,
                        internalCorrelationId,
                        amount,
                        currency,
                        transactionFeeAmount,
                        paymentScheme,
                        disposalAccountAmsId,
                        conversionAccountAmsId,
                        tenantIdentifier,
                        transactionFeeInternalCorrelationId,
                        accountProductType,
                        Boolean.parseBoolean(Optional.ofNullable(valueDated).orElse("false"))
                ));
    }

    private void holdAndReleaseForSavingsAccount(String transactionGroupId, String transactionId, String transactionCategoryPurposeCode, String internalCorrelationId, String paymentScheme, String disposalAccountAmsId, String conversionAccountAmsId, String tenantIdentifier, String iban, String accountProductType, String transactionDate, BigDecimal totalAmountWithFee, List<TransactionItem> items, EntryTransaction10 transactionDetails, CustomerCreditTransferInitiationV10 pain0011, ReportEntry10 convertedCamt053Entry, String partnerName, String partnerAccountIban, String partnerAccountSecondaryIdentifier, String unstructured, String endToEndId, Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001, CreditTransferTransaction40 pain001Transaction) throws JsonProcessingException {
        log.info("Adding hold item to disposal account {}", disposalAccountAmsId);
        String apiPath = accountProductType.equalsIgnoreCase("SAVINGS") ? incomingMoneyApi.substring(1) : currentAccountApi.substring(1);
        String outHoldReasonId = tenantConfigs.findPaymentTypeId(tenantIdentifier, String.format("%s.%s", paymentScheme, "outHoldReasonId"));
        String bodyItem = painMapper.writeValueAsString(new HoldAmountBody(transactionDate, totalAmountWithFee, outHoldReasonId, moneyInOutWorker.getLocale(), FORMAT));
        String holdTransactionUrl = String.format("%s%s/transactions?command=holdAmount", apiPath, disposalAccountAmsId);
        batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, holdTransactionUrl, bodyItem, false);

        String holdAmountOperation = "outHoldReasonId";
        String paymentTypeCode = Optional.ofNullable(tenantConfigs.findResourceCode(tenantIdentifier, String.format("%s.%s", paymentScheme, holdAmountOperation))).orElse("");
        transactionDetails.setAdditionalTransactionInformation(paymentTypeCode);
        if (pain0011 != null) {
            transactionDetails.getSupplementaryData().clear();
            camt053Mapper.fillOtherIdentification(pain0011, transactionDetails);
        }
        String camt053 = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);
        DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(internalCorrelationId, camt053, iban, paymentTypeCode, transactionGroupId, partnerName, partnerAccountIban, null, partnerAccountSecondaryIdentifier, unstructured, transactionCategoryPurposeCode, paymentScheme, null, null, endToEndId);
        String camt053Body = painMapper.writeValueAsString(td);
        batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, "datatables/dt_savings_transaction_details/$.resourceId", camt053Body, true);

        Long lastHoldTransactionId = moneyInOutWorker.holdBatch(items, tenantIdentifier, transactionGroupId, disposalAccountAmsId, conversionAccountAmsId, internalCorrelationId, "transferToConversionAccountInAms");

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
        httpHeaders.set("Authorization", authTokenHelper.generateAuthToken());
        httpHeaders.set("Fineract-Platform-TenantId", tenantIdentifier);
        httpHeaders.set("X-Correlation-ID", transactionGroupId);
        LinkedHashMap<String, Object> accountDetails = moneyInOutWorker.getRestTemplate().exchange(
                String.format("%s/%s%s", moneyInOutWorker.getFineractApiUrl(), apiPath, disposalAccountAmsId),
                HttpMethod.GET,
                new HttpEntity<>(httpHeaders),
                LinkedHashMap.class
        ).getBody();
        LinkedHashMap<String, Object> summary = (LinkedHashMap<String, Object>) accountDetails.get("summary");
        BigDecimal availableBalance = new BigDecimal(summary.get("availableBalance").toString());
        if (availableBalance.signum() < 0) {
            moneyInOutWorker.getRestTemplate().exchange(
                    String.format("%s/%ssavingsaccounts/%s/transactions/%d?command=releaseAmount", moneyInOutWorker.getFineractApiUrl(), apiPath, disposalAccountAmsId, lastHoldTransactionId),
                    HttpMethod.POST,
                    new HttpEntity<>(httpHeaders),
                    Object.class
            );
            throw new ZeebeBpmnError("Error_InsufficientFunds", "Insufficient funds");
        }


        // prepare release transaction
        items.clear();

        String releaseTransactionUrl = String.format("%s%s/transactions/%d?command=releaseAmount", apiPath, disposalAccountAmsId, lastHoldTransactionId);
        batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, releaseTransactionUrl, null, false);
        String releaseAmountOperation = "transferToConversionAccountInAms.DisposalAccount.ReleaseTransactionAmount";
        CustomerCreditTransferInitiationV10 pain0012 = pain001.getDocument();
        String endToEndId1 = pain001Transaction.getPaymentIdentification().getEndToEndIdentification();
        String paymentTypeCode1 = Optional.ofNullable(tenantConfigs.findResourceCode(tenantIdentifier, String.format("%s.%s", paymentScheme, releaseAmountOperation))).orElse("");
        EntryTransaction10 transactionDetails1 = convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0);
        transactionDetails1.setAdditionalTransactionInformation(paymentTypeCode1);
        if (pain0012 != null) {
            transactionDetails1.getSupplementaryData().clear();
            camt053Mapper.fillOtherIdentification(pain0012, transactionDetails1);
        }
        String camt0531 = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);
        DtSavingsTransactionDetails td1 = new DtSavingsTransactionDetails(internalCorrelationId, camt0531, iban, paymentTypeCode1, transactionGroupId, partnerName, partnerAccountIban, null, partnerAccountSecondaryIdentifier, unstructured, transactionCategoryPurposeCode, paymentScheme, null, null, endToEndId1);
        String camt053Body1 = painMapper.writeValueAsString(td1);
        batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, "datatables/dt_savings_transaction_details/$.resourceId", camt053Body1, true);
    }

    @SuppressWarnings("unchecked")
    private Void transferToConversionAccountInAms(String transactionGroupId,
                                                  String transactionId,
                                                  String transactionCategoryPurposeCode,
                                                  String transactionFeeCategoryPurposeCode,
                                                  String originalPain001,
                                                  String internalCorrelationId,
                                                  BigDecimal amount,
                                                  String currency,
                                                  BigDecimal transactionFeeAmount,
                                                  String paymentScheme,
                                                  String disposalAccountAmsId,
                                                  String conversionAccountAmsId,
                                                  String tenantIdentifier,
                                                  String transactionFeeInternalCorrelationId,
                                                  String accountProductType,
                                                  boolean valueDated) {
        try {
            // STEP 0 - collect / extract information
            String transactionDate = LocalDate.now().format(PATTERN);
            String apiPath = accountProductType.equalsIgnoreCase("SAVINGS") ? incomingMoneyApi.substring(1) : currentAccountApi.substring(1);

            MDC.put("internalCorrelationId", internalCorrelationId);
            log.debug("Debtor exchange worker starting, using api path {}", apiPath);

            String disposalAccountWithdrawRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, disposalAccountAmsId, "withdrawal");
            String conversionAccountDepositRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, conversionAccountAmsId, "deposit");

            Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = painMapper.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
            CustomerCreditTransferInitiationV10 pain001Document = pain001.getDocument();
            PaymentInstruction34 pain001PaymentInstruction = pain001Document.getPaymentInformation().get(0);
            CreditTransferTransaction40 creditTransferTransaction = pain001PaymentInstruction.getCreditTransferTransactionInformation().get(0);
            String transactionCreationChannel = batchItemBuilder.findTransactionCreationChannel(creditTransferTransaction.getSupplementaryData());

            log.debug("Withdrawing amount {} from disposal account {} with fee: {}", amount, disposalAccountAmsId, transactionFeeAmount);
            boolean hasFee = !BigDecimal.ZERO.equals(transactionFeeAmount);
            BigDecimal totalAmountWithFee = hasFee ? amount.add(transactionFeeAmount) : amount;

            BankToCustomerStatementV08 convertedStatement = camt053Mapper.toCamt053Entry(pain001Document);
            ReportEntry10 convertedCamt053Entry = convertedStatement.getStatement().get(0).getEntry().get(0);
            EntryTransaction10 entryTransaction10 = convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0);
            batchItemBuilder.setAmount(entryTransaction10, amount, currency);
            entryTransaction10.getAmountDetails().getTransactionAmount().getAmount().setAmount(totalAmountWithFee);
            String partnerName = creditTransferTransaction.getCreditor().getName();
            String partnerAccountIban = creditTransferTransaction.getCreditorAccount().getIdentification().getIban();
            String partnerAccountSecondaryIdentifier = contactDetailsUtil.getId(creditTransferTransaction.getCreditor().getContactDetails());
            String unstructured = Optional.ofNullable(creditTransferTransaction.getRemittanceInformation())
                    .map(iso.std.iso._20022.tech.json.pain_001_001.RemittanceInformation16::getUnstructured).map(it -> String.join(",", it))
                    .orElse("");
            String endToEndId = creditTransferTransaction.getPaymentIdentification().getEndToEndIdentification();
            String debtorIban = pain001PaymentInstruction.getDebtorAccount().getIdentification().getIban();

            // STEP 1 - batch: add hold and release items [only for savings account]
            List<TransactionItem> items = new ArrayList<>();
            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                holdAndReleaseForSavingsAccount(transactionGroupId, transactionId, transactionCategoryPurposeCode, internalCorrelationId, paymentScheme, disposalAccountAmsId, conversionAccountAmsId, tenantIdentifier, debtorIban, accountProductType, transactionDate, totalAmountWithFee, items, entryTransaction10, pain001Document, convertedCamt053Entry, partnerName, partnerAccountIban, partnerAccountSecondaryIdentifier, unstructured, endToEndId, pain001, creditTransferTransaction);
            } else {
                log.info("No hold and release because disposal account {} is not a savings account", disposalAccountAmsId);
            }

            // STEP 2a - batch: add withdraw amount
            entryTransaction10.setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "PENDING"));

            String withdrawAmountOperation = "transferToConversionAccountInAms.DisposalAccount.WithdrawTransactionAmount";
            String configOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
            String withdrawAmountPaymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, configOperationKey);
            String direction = tenantConfigs.findDirection(tenantIdentifier, configOperationKey);

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                String withdrawAmountTransactionBody = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, withdrawAmountPaymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, disposalAccountWithdrawRelativeUrl, withdrawAmountTransactionBody, false);
            } // CURRENT account sends a single call only at the details step

            // STEP 2b - batch: add withdrawal details
            entryTransaction10.getAmountDetails().getTransactionAmount().getAmount().setAmount(amount);
            String paymentTypeCode = Optional.ofNullable(tenantConfigs.findResourceCode(tenantIdentifier, configOperationKey)).orElse("");
            entryTransaction10.setAdditionalTransactionInformation(paymentTypeCode);
            if (pain001Document != null) {
                entryTransaction10.getSupplementaryData().clear();
                camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001Document, entryTransaction10, transactionCategoryPurposeCode);
                camt053Mapper.refillOtherIdentification(pain001Document, entryTransaction10);
            }
            String withdrawAmountCamt053 = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                String withdrawAmountCamt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, withdrawAmountCamt053, debtorIban, paymentTypeCode, transactionGroupId, partnerName, partnerAccountIban, null, partnerAccountSecondaryIdentifier, unstructured, transactionCategoryPurposeCode, paymentScheme, disposalAccountAmsId, conversionAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, "datatables/dt_savings_transaction_details/$.resourceId", withdrawAmountCamt053Body, true);

            } else {  // CURRENT account executes withdrawal and details in one step
                String withdrawAmountTransactionBody = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, moneyInOutWorker.getLocale(), withdrawAmountPaymentTypeId, currency, List.of(
                        new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                                debtorIban,
                                withdrawAmountCamt053,
                                internalCorrelationId,
                                partnerName,
                                partnerAccountIban,
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
                        )), "dt_current_transaction_details"))
                ));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, disposalAccountWithdrawRelativeUrl, withdrawAmountTransactionBody, false);
            }

            // STEP 2c - batch: add withdrawal fee, if there is any
            if (hasFee) {
                log.debug("Withdrawing fee {} from disposal account {}", transactionFeeAmount, disposalAccountAmsId);
                String withdrawFeeOperation = "transferToConversionAccountInAms.DisposalAccount.WithdrawTransactionFee";
                String withdrawFeePaymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, String.format("%s.%s", paymentScheme, withdrawFeeOperation));
                if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                    String withdrawFeeTransactionBody = painMapper.writeValueAsString(new TransactionBody(transactionDate, transactionFeeAmount, withdrawFeePaymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                    batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, disposalAccountWithdrawRelativeUrl, withdrawFeeTransactionBody, false);
                } // CURRENT account sends a single call only at the details step

                if (entryTransaction10.getSupplementaryData() != null) {
                    entryTransaction10.getSupplementaryData().get(0).getEnvelope().setAdditionalProperty("InternalCorrelationId", transactionFeeInternalCorrelationId);
                } else {
                    entryTransaction10.setSupplementaryData(new ArrayList<>(List.of(new SupplementaryData1().withEnvelope(new SupplementaryDataEnvelope1().withAdditionalProperty("InternalCorrelationId", transactionFeeInternalCorrelationId)))));
                }
                if (entryTransaction10.getAmountDetails() != null) {
                    entryTransaction10.getAmountDetails().getTransactionAmount().getAmount().setAmount(transactionFeeAmount);
                } else {
                    entryTransaction10.setAmountDetails(new AmountAndCurrencyExchange3().withTransactionAmount(new AmountAndCurrencyExchangeDetails3().withAmount(new ActiveOrHistoricCurrencyAndAmount().withAmount(transactionFeeAmount))));
                }

                String withdrawFeePaymentTypeCode = Optional.ofNullable(tenantConfigs.findResourceCode(tenantIdentifier, String.format("%s.%s", paymentScheme, withdrawFeeOperation))).orElse("");
                entryTransaction10.setAdditionalTransactionInformation(withdrawFeePaymentTypeCode);
                if (pain001Document != null) {
                    entryTransaction10.getSupplementaryData().clear();
                    camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001Document, entryTransaction10, transactionFeeCategoryPurposeCode);
                    camt053Mapper.refillOtherIdentification(pain001Document, entryTransaction10);
                }
                String withdrawFeeCamt053 = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);
                if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                    String withdrawFeeCamt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(transactionFeeInternalCorrelationId, withdrawFeeCamt053, debtorIban, withdrawFeePaymentTypeCode, transactionGroupId, partnerName, partnerAccountIban, null, partnerAccountSecondaryIdentifier, unstructured, transactionFeeCategoryPurposeCode, paymentScheme, disposalAccountAmsId, conversionAccountAmsId, endToEndId));
                    batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, "datatables/dt_savings_transaction_details/$.resourceId", withdrawFeeCamt053Body, true);

                } else { // CURRENT account executes withdrawal and details in one step
                    String withdrawFeeTransactionBody = painMapper.writeValueAsString(new CurrentAccountTransactionBody(transactionFeeAmount, FORMAT, moneyInOutWorker.getLocale(), withdrawFeePaymentTypeId, currency, List.of(
                            new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                                    debtorIban,
                                    withdrawFeeCamt053,
                                    transactionFeeInternalCorrelationId,
                                    partnerName,
                                    partnerAccountIban,
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
                            )), "dt_current_transaction_details"))
                    ));
                    batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, disposalAccountWithdrawRelativeUrl, withdrawFeeTransactionBody, false);
                }
            }

            // STEP 3a - batch: deposit amount
            log.info("Depositing amount {} to conversion account {}", amount, conversionAccountAmsId);
            String depositAmountOperation = "transferToConversionAccountInAms.ConversionAccount.DepositTransactionAmount";
            String depositAmountPaymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, String.format("%s.%s", paymentScheme, depositAmountOperation));
            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                String depositAmountTransactionBody = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, depositAmountPaymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, conversionAccountDepositRelativeUrl, depositAmountTransactionBody, false);
            }

            // STEP 3b - batch: add deposit details
            if (entryTransaction10.getSupplementaryData() != null) {
                entryTransaction10.getSupplementaryData().get(0).getEnvelope().setAdditionalProperty("InternalCorrelationId", internalCorrelationId);
            } else {
                entryTransaction10.setSupplementaryData(new ArrayList<>(List.of(new SupplementaryData1().withEnvelope(new SupplementaryDataEnvelope1().withAdditionalProperty("InternalCorrelationId", internalCorrelationId)))));
            }
            if (entryTransaction10.getAmountDetails() != null) {
                entryTransaction10.getAmountDetails().getTransactionAmount().getAmount().setAmount(amount);
            } else {
                entryTransaction10.setAmountDetails(new AmountAndCurrencyExchange3().withTransactionAmount(new AmountAndCurrencyExchangeDetails3().withAmount(new ActiveOrHistoricCurrencyAndAmount().withAmount(amount))));
            }
            entryTransaction10.setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.CRDT);
            String depositAmountPaymentTypeCode = Optional.ofNullable(tenantConfigs.findResourceCode(tenantIdentifier, String.format("%s.%s", paymentScheme, depositAmountOperation))).orElse("");
            entryTransaction10.setAdditionalTransactionInformation(depositAmountPaymentTypeCode);
            if (pain001Document != null) {
                entryTransaction10.getSupplementaryData().clear();
                camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001Document, entryTransaction10, transactionCategoryPurposeCode);
                camt053Mapper.refillOtherIdentification(pain001Document, entryTransaction10);
            }
            String depositAmountCamt053 = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);
            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                String depositAmountCamt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, depositAmountCamt053, debtorIban, depositAmountPaymentTypeCode, transactionGroupId, partnerName, partnerAccountIban, null, partnerAccountSecondaryIdentifier, unstructured, transactionCategoryPurposeCode, paymentScheme, disposalAccountAmsId, conversionAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, "datatables/dt_savings_transaction_details/$.resourceId", depositAmountCamt053Body, true);
            } else { // CURRENT account executes deposit amount and details in one step
                String depositAmountTransactionBody = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, moneyInOutWorker.getLocale(), depositAmountPaymentTypeId, currency, List.of(
                        new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                                debtorIban,
                                depositAmountCamt053,
                                internalCorrelationId,
                                partnerName,
                                partnerAccountIban,
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
                        )), "dt_current_transaction_details"))
                ));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, conversionAccountDepositRelativeUrl, depositAmountTransactionBody, false);
            }

            // STEP 3c - batch: add deposit fee, if any
            if (hasFee) {
                log.debug("Depositing fee {} to conversion account {}", transactionFeeAmount, conversionAccountAmsId);
                String depositFeeOperation = "transferToConversionAccountInAms.ConversionAccount.DepositTransactionFee";
                String depositFeePaymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, String.format("%s.%s", paymentScheme, depositFeeOperation));
                if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                    String depositFeeTransactionBody = painMapper.writeValueAsString(new TransactionBody(transactionDate, transactionFeeAmount, depositFeePaymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                    batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, conversionAccountDepositRelativeUrl, depositFeeTransactionBody, false);
                }

                if (entryTransaction10.getSupplementaryData() != null) {
                    entryTransaction10.getSupplementaryData().get(0).getEnvelope().setAdditionalProperty("InternalCorrelationId", transactionFeeInternalCorrelationId);
                } else {
                    entryTransaction10.setSupplementaryData(new ArrayList<>(List.of(new SupplementaryData1().withEnvelope(new SupplementaryDataEnvelope1().withAdditionalProperty("InternalCorrelationId", transactionFeeInternalCorrelationId)))));
                }

                if (entryTransaction10.getAmountDetails() != null) {
                    entryTransaction10.getAmountDetails().getTransactionAmount().getAmount().setAmount(transactionFeeAmount);
                } else {
                    entryTransaction10.setAmountDetails(new AmountAndCurrencyExchange3().withTransactionAmount(new AmountAndCurrencyExchangeDetails3().withAmount(new ActiveOrHistoricCurrencyAndAmount().withAmount(transactionFeeAmount))));
                }

                String depositFeePaymentTypeCode = Optional.ofNullable(tenantConfigs.findResourceCode(tenantIdentifier, String.format("%s.%s", paymentScheme, depositFeeOperation))).orElse("");
                entryTransaction10.setAdditionalTransactionInformation(depositFeePaymentTypeCode);
                if (pain001Document != null) {
                    entryTransaction10.getSupplementaryData().clear();
                    camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001Document, entryTransaction10, transactionFeeCategoryPurposeCode);
                    camt053Mapper.refillOtherIdentification(pain001Document, entryTransaction10);
                }
                String depositFeeCamt053 = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);
                if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                    String depositFeeCamt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(transactionFeeInternalCorrelationId, depositFeeCamt053, debtorIban, depositFeePaymentTypeCode, transactionGroupId, partnerName, partnerAccountIban, null, partnerAccountSecondaryIdentifier, unstructured, transactionFeeCategoryPurposeCode, paymentScheme, disposalAccountAmsId, conversionAccountAmsId, endToEndId));
                    batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, "datatables/dt_savings_transaction_details/$.resourceId", depositFeeCamt053Body, true);
                } else { // CURRENT account executes deposit fee and details in one step
                    String depositFeeTransactionBody = painMapper.writeValueAsString(new CurrentAccountTransactionBody(transactionFeeAmount, FORMAT, moneyInOutWorker.getLocale(), depositFeePaymentTypeId, currency, List.of(
                            new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                                    debtorIban,
                                    depositFeeCamt053,
                                    transactionFeeInternalCorrelationId,
                                    partnerName,
                                    partnerAccountIban,
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
                            )), "dt_current_transaction_details"))
                    ));
                    batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, conversionAccountDepositRelativeUrl, depositFeeTransactionBody, false);
                }
            }

            moneyInOutWorker.doBatch(items, tenantIdentifier, transactionGroupId, disposalAccountAmsId, conversionAccountAmsId, internalCorrelationId, "transferToConversionAccountInAms");

        } catch (ZeebeBpmnError z) {
            throw z;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            MDC.remove("internalCorrelationId");
        }
        return null;
    }

    @JobWorker
    @TraceZeebeArguments
    @LogInternalCorrelationId
    public void withdrawTheAmountFromDisposalAccountInAMS(JobClient client,
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
                                                          @Variable String camt056,
                                                          @Variable String iban,
                                                          @Variable String internalCorrelationId,
                                                          @Variable String accountProductType,
                                                          @Variable String valueDated) {
        log.info("withdrawTheAmountFromDisposalAccountInAMS");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "withdrawTheAmountFromDisposalAccountInAMS",
                        null,
                        null,
                        eventBuilder),
                eventBuilder -> withdrawTheAmountFromDisposalAccountInAMS(amount,
                        currency,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        tenantIdentifier,
                        transactionGroupId,
                        transactionId,
                        paymentScheme,
                        transactionCategoryPurposeCode,
                        camt056,
                        iban,
                        internalCorrelationId,
                        accountProductType,
                        Boolean.parseBoolean(Optional.ofNullable(valueDated).orElse("false"))
                ));
    }

    @SuppressWarnings("unchecked")
    private Void withdrawTheAmountFromDisposalAccountInAMS(BigDecimal amount,
                                                           String currency,
                                                           String conversionAccountAmsId,
                                                           String disposalAccountAmsId,
                                                           String tenantIdentifier,
                                                           String transactionGroupId,
                                                           String transactionId,
                                                           String paymentScheme,
                                                           String transactionCategoryPurposeCode,
                                                           String camt056,
                                                           String iban,
                                                           String internalCorrelationId,
                                                           String accountProductType,
                                                           boolean valueDated) {
        try {
            // STEP 0 - collect / extract information
            String transactionDate = LocalDate.now().format(PATTERN);
            log.debug("Withdrawing amount {} from disposal account {}", amount, disposalAccountAmsId);

            iso.std.iso._20022.tech.xsd.camt_056_001.Document document = jaxbUtils.unmarshalCamt056(camt056);
            Camt056ToCamt053Converter converter = new Camt056ToCamt053Converter();
            BankToCustomerStatementV08 statement = converter.convert(document, new BankToCustomerStatementV08());
            String apiPath = accountProductType.equalsIgnoreCase("SAVINGS") ? incomingMoneyApi.substring(1) : currentAccountApi.substring(1);
            String holdTransactionUrl = String.format("%s%s/transactions?command=holdAmount", apiPath, disposalAccountAmsId);
            String withdrawRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, disposalAccountAmsId, "withdrawal");
            String withdrawAmountOperation = "withdrawTheAmountFromDisposalAccountInAMS.DisposalAccount.WithdrawTransactionAmount";
            String withdrawConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
            String withdrawPaymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, withdrawConfigOperationKey);
            String withdrawPaymentTypeCode = tenantConfigs.findResourceCode(tenantIdentifier, withdrawConfigOperationKey);
            String conversionAccountDepositRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, conversionAccountAmsId, "deposit");
            String depositAmountOperation = "withdrawTheAmountFromDisposalAccountInAMS.ConversionAccount.DepositTransactionAmount";
            String depositPaymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, String.format("%s.%s", paymentScheme, depositAmountOperation));

            ReportEntry10 convertedCamt053Entry = statement.getStatement().get(0).getEntry().get(0);
            convertedCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "PENDING"));
            String hyphenatedDate = transactionDate.substring(0, 4) + "-" + transactionDate.substring(4, 6) + "-" + transactionDate.substring(6);
            convertedCamt053Entry.setValueDate(new DateAndDateTime2Choice().withAdditionalProperty("Date", hyphenatedDate));

            OriginalTransactionReference13 originalTransactionReference = document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef();
            String debtorName = originalTransactionReference.getDbtr().getNm();
            String debtorIban = originalTransactionReference.getDbtrAcct().getId().getIBAN();
            String partnerAccountSecondaryIdentifier = contactDetailsUtil.getId(originalTransactionReference.getCdtr().getCtctDtls());
            String unstructured = Optional.ofNullable(originalTransactionReference.getRmtInf()).map(RemittanceInformation5::getUstrd).map(it -> String.join(",", it)).orElse("");
            String endToEndId = document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlEndToEndId();
            EntryTransaction10 entryTransaction10 = convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0);
            batchItemBuilder.setAmount(entryTransaction10, amount, currency);
            String creditorIban = originalTransactionReference.getCdtrAcct().getId().getIBAN();
            String debtorContactDetails = contactDetailsUtil.getId(originalTransactionReference.getDbtr().getCtctDtls());

            String holdAmountOperation = "withdrawTheAmountFromDisposalAccountInAMS.DisposalAccount.HoldTransactionAmount";
            String configOperationKey = String.format("%s.%s", paymentScheme, holdAmountOperation);
            String paymentTypeCode1 = tenantConfigs.findResourceCode(tenantIdentifier, configOperationKey);
            String direction = tenantConfigs.findDirection(tenantIdentifier, configOperationKey);
            entryTransaction10.setAdditionalTransactionInformation(paymentTypeCode1);
            String camt0531 = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);
            List<TransactionItem> items = new ArrayList<>();

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                // STEP 1 - hold the amount
                String outHoldReasonId = tenantConfigs.findPaymentTypeId(tenantIdentifier, String.format("%s.%s", paymentScheme, "outHoldReasonId"));
                String bodyItem = painMapper.writeValueAsString(new HoldAmountBody(transactionDate, amount, outHoldReasonId, moneyInOutWorker.getLocale(), FORMAT));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, holdTransactionUrl, bodyItem, false);

                String camt053Body1 = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, camt0531, iban, paymentTypeCode1, internalCorrelationId, debtorName, debtorIban, null, partnerAccountSecondaryIdentifier, unstructured, transactionCategoryPurposeCode, paymentScheme, disposalAccountAmsId, conversionAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, "datatables/dt_savings_transaction_details/$.resourceId", camt053Body1, true);
                Long lastHoldTransactionId = moneyInOutWorker.holdBatch(items, tenantIdentifier, transactionGroupId, disposalAccountAmsId, conversionAccountAmsId, internalCorrelationId, "transferToConversionAccountInAms");

                // STEP 2 - query balance
                HttpHeaders httpHeaders = new HttpHeaders();
                httpHeaders.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
                httpHeaders.set("Authorization", authTokenHelper.generateAuthToken());
                httpHeaders.set("Fineract-Platform-TenantId", tenantIdentifier);
                httpHeaders.set("X-Correlation-ID", transactionGroupId);
                LinkedHashMap<String, Object> accountDetails = moneyInOutWorker.getRestTemplate().exchange(String.format("%s/%s%s", moneyInOutWorker.getFineractApiUrl(), apiPath, disposalAccountAmsId), HttpMethod.GET, new HttpEntity<>(httpHeaders), LinkedHashMap.class).getBody();
                LinkedHashMap<String, Object> summary = (LinkedHashMap<String, Object>) accountDetails.get("summary");
                BigDecimal availableBalance = new BigDecimal(summary.get("availableBalance").toString());
                if (availableBalance.signum() < 0) {
                    moneyInOutWorker.getRestTemplate().exchange(String.format("%s/%ssavingsaccounts/%s/transactions/%d?command=releaseAmount", moneyInOutWorker.getFineractApiUrl(), apiPath, disposalAccountAmsId, lastHoldTransactionId), HttpMethod.POST, new HttpEntity<>(httpHeaders), Object.class);
                    throw new ZeebeBpmnError("Error_InsufficientFunds", "Insufficient funds");
                }
                items.clear();

                // STEP 3 - release the amount
                String releaseTransactionUrl = String.format("%s%s/transactions/%d?command=releaseAmount", apiPath, disposalAccountAmsId, lastHoldTransactionId);
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, releaseTransactionUrl, null, false);
                String releaseAmountOperation = "withdrawTheAmountFromDisposalAccountInAMS.DisposalAccount.ReleaseTransactionAmount";
                String releasePaymentTypeCode = Optional.ofNullable(tenantConfigs.findResourceCode(tenantIdentifier, String.format("%s.%s", paymentScheme, releaseAmountOperation))).orElse("");
                entryTransaction10.setAdditionalTransactionInformation(releasePaymentTypeCode);
                String camt053 = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);
                String camt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, camt053, iban, releasePaymentTypeCode, internalCorrelationId, debtorName, debtorIban, null, partnerAccountSecondaryIdentifier, unstructured, transactionCategoryPurposeCode, paymentScheme, disposalAccountAmsId, conversionAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, "datatables/dt_savings_transaction_details/$.resourceId", camt053Body, true);
            }


            // STEP 4 - withdraw the amount
            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                var bodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, withdrawPaymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, withdrawRelativeUrl, bodyItem, false);
            }

            entryTransaction10.setAdditionalTransactionInformation(withdrawPaymentTypeCode);
            entryTransaction10.setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "PENDING"));
            String camt053 = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);

            // STEP 5 - withdraw details
            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                String camt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, camt053, creditorIban, withdrawPaymentTypeCode, internalCorrelationId, debtorName, debtorIban, null, debtorContactDetails, unstructured, transactionCategoryPurposeCode, paymentScheme, disposalAccountAmsId, conversionAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, "datatables/dt_savings_transaction_details/$.resourceId", camt053Body, true);
            } else {
                String camt053Body = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, moneyInOutWorker.getLocale(), withdrawPaymentTypeId, currency, List.of(new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
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
                        disposalAccountAmsId,
                        conversionAccountAmsId,
                        null,
                        debtorContactDetails,
                        null,
                        valueDated,
                        direction
                )), "dt_current_transaction_details"))));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, withdrawRelativeUrl, camt053Body, false);
            }

            // STEP 6 - deposit amount
            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                String bodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, depositPaymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, conversionAccountDepositRelativeUrl, bodyItem, false);
            }

            var depositConfigOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
            var depositPaymentTypeCode = tenantConfigs.findResourceCode(tenantIdentifier, depositConfigOperationKey);
            entryTransaction10.setAdditionalTransactionInformation(depositPaymentTypeCode);
            entryTransaction10.setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "PENDING"));
            camt053 = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);

            // STEP 7 - deposit details
            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                var camt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, camt053, creditorIban, depositPaymentTypeCode, internalCorrelationId, debtorName, debtorIban, null, debtorContactDetails, unstructured, transactionCategoryPurposeCode, paymentScheme, disposalAccountAmsId, conversionAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, "datatables/dt_savings_transaction_details/$.resourceId", camt053Body, true);
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
                        disposalAccountAmsId,
                        conversionAccountAmsId,
                        null,
                        debtorContactDetails,
                        null,
                        valueDated,
                        direction
                )), "dt_current_transaction_details"))));
                batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, conversionAccountDepositRelativeUrl, camt053Body, false);
            }

            moneyInOutWorker.doBatch(items, tenantIdentifier, transactionGroupId, disposalAccountAmsId, conversionAccountAmsId, internalCorrelationId, "withdrawTheAmountFromDisposalAccountInAMS");
        } catch (JAXBException | JsonProcessingException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
        return null;
    }
}