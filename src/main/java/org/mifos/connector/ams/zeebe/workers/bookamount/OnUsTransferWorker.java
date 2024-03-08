package org.mifos.connector.ams.zeebe.workers.bookamount;

import com.baasflow.commons.events.EventService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import hu.dpc.rt.utils.converter.AccountSchemeName1Choice;
import hu.dpc.rt.utils.converter.GenericAccountIdentification1;
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
import lombok.extern.slf4j.Slf4j;
import org.mifos.connector.ams.common.SerializationHelper;
import org.mifos.connector.ams.fineract.TenantConfigs;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.log.LogInternalCorrelationId;
import org.mifos.connector.ams.log.TraceZeebeArguments;
import org.mifos.connector.ams.mapstruct.Pain001Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.*;
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
import java.util.*;

import static org.mifos.connector.ams.zeebe.workers.bookamount.MoneyInOutWorker.FORMAT;

@Component
@Slf4j
public class OnUsTransferWorker {

    @Autowired
    private Pain001Camt053Mapper camt053Mapper;

    @Value("${fineract.incoming-money-api}")
    protected String incomingMoneyApi;

    @Value("${fineract.current-account-api}")
    protected String currentAccountApi;

    @Autowired
    private TenantConfigs tenantConfigs;

    @Autowired
    private BatchItemBuilder batchItemBuilder;

    @Autowired
    private ContactDetailsUtil contactDetailsUtil;

    @Autowired
    private SerializationHelper serializationHelper;

    @Autowired
    private AuthTokenHelper authTokenHelper;

    @Autowired
    private NotificationHelper notificationHelper;

    @Autowired
    private EventService eventService;

    @Autowired
    @Qualifier("painMapper")
    private ObjectMapper painMapper;

    @Autowired
    private MoneyInOutWorker moneyInOutWorker;

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
                                                                        @Variable String currency,
                                                                        @Variable String creditorDisposalAccountAmsId,
                                                                        @Variable String debtorDisposalAccountAmsId,
                                                                        @Variable String debtorConversionAccountAmsId,
                                                                        @Variable BigDecimal transactionFeeAmount,
                                                                        @Variable String tenantIdentifier,
                                                                        @Variable String transactionGroupId,
                                                                        @Variable String transactionId,
                                                                        @Variable String transactionCategoryPurposeCode,
                                                                        @Variable String transactionFeeCategoryPurposeCode,
                                                                        @Variable String transactionFeeInternalCorrelationId,
                                                                        @Variable String creditorIban,
                                                                        @Variable String debtorIban,
                                                                        @Variable String debtorInternalAccountId,
                                                                        @Variable String creditorInternalAccountId,
                                                                        @Variable String accountProductType,
                                                                        @Variable String valueDated
    ) {
        log.info("transferTheAmountBetweenDisposalAccounts");
        return eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "transferTheAmountBetweenDisposalAccounts", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> transferTheAmountBetweenDisposalAccounts(internalCorrelationId,
                        paymentScheme,
                        originalPain001,
                        amount,
                        currency,
                        creditorDisposalAccountAmsId,
                        debtorDisposalAccountAmsId,
                        debtorConversionAccountAmsId,
                        transactionFeeAmount,
                        tenantIdentifier,
                        transactionGroupId,
                        transactionId,
                        transactionCategoryPurposeCode,
                        transactionFeeCategoryPurposeCode,
                        transactionFeeInternalCorrelationId,
                        creditorIban,
                        debtorIban,
                        debtorInternalAccountId,
                        creditorInternalAccountId,
                        accountProductType,
                        Boolean.parseBoolean(Optional.ofNullable(valueDated).orElse("false"))
                ));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> transferTheAmountBetweenDisposalAccounts(String internalCorrelationId,
                                                                         String paymentScheme,
                                                                         String originalPain001,
                                                                         BigDecimal amount,
                                                                         String currency,
                                                                         String creditorDisposalAccountAmsId,
                                                                         String debtorDisposalAccountAmsId,
                                                                         String debtorConversionAccountAmsId,
                                                                         BigDecimal transactionFeeAmount,
                                                                         String tenantIdentifier,
                                                                         String transactionGroupId,
                                                                         String transactionId,
                                                                         String transactionCategoryPurposeCode,
                                                                         String transactionFeeCategoryPurposeCode,
                                                                         String transactionFeeInternalCorrelationId,
                                                                         String creditorIban,
                                                                         String debtorIban,
                                                                         String debtorInternalAccountId,
                                                                         String creditorInternalAccountId,
                                                                         String accountProductType,
                                                                         boolean valueDated
    ) {
        try {
            // STEP 0 - collect / extract information
            log.debug("transferTheAmountBetweenDisposalAccounts - Incoming pain.001: {}", originalPain001);
            String outHoldReasonId = tenantConfigs.findPaymentTypeId(tenantIdentifier, String.format("%s.%s", paymentScheme, "outHoldReasonId"));
            String savingsCamt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";
            String apiPath = accountProductType.equalsIgnoreCase("SAVINGS") ? incomingMoneyApi.substring(1) : currentAccountApi.substring(1);
            Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = painMapper.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
            BankToCustomerStatementV08 convertedStatement = camt053Mapper.toCamt053Entry(pain001.getDocument());
            ReportEntry10 convertedcamt053Entry = convertedStatement.getStatement().get(0).getEntry().get(0);
            AmountAndCurrencyExchangeDetails3 withAmountAndCurrency = new AmountAndCurrencyExchangeDetails3().withAmount(new ActiveOrHistoricCurrencyAndAmount().withAmount(amount.add(transactionFeeAmount)).withCurrency(currency));
            EntryTransaction10 entryTransaction10 = convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0);
            batchItemBuilder.setAmount(entryTransaction10, amount, currency);
            CreditTransferTransaction40 creditTransfer = pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0);
            String creditorName = creditTransfer.getCreditor().getName();
            String transactionCreationChannel = batchItemBuilder.findTransactionCreationChannel(creditTransfer.getSupplementaryData());
            String creditorContactDetails = contactDetailsUtil.getId(creditTransfer.getCreditor().getContactDetails());
            String debtorContactDetails = contactDetailsUtil.getId(pain001.getDocument().getPaymentInformation().get(0).getDebtor().getContactDetails());
            String unstructured = Optional.ofNullable(creditTransfer.getRemittanceInformation())
                    .map(iso.std.iso._20022.tech.json.pain_001_001.RemittanceInformation16::getUnstructured).map(List::toString).orElse("");
            CustomerCreditTransferInitiationV10 pain0011 = pain001.getDocument();
            String endToEndId = pain0011.getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getPaymentIdentification().getEndToEndIdentification();
            String depositAmountOperation = "transferTheAmountBetweenDisposalAccounts.Creditor.DisposalAccount.DepositTransactionAmount";
            String depositAmountConfigOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
            String debtorDisposalWithdrawalRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, debtorDisposalAccountAmsId, "withdrawal");
            String withdrawAmountOperation = "transferTheAmountBetweenDisposalAccounts.Debtor.DisposalAccount.WithdrawTransactionAmount";
            String withdrawAmountConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
            String withdrawPaymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, withdrawAmountConfigOperationKey);
            String withdrawPaymentTypeCode = tenantConfigs.findResourceCode(tenantIdentifier, withdrawAmountConfigOperationKey);

            entryTransaction10.withAmountDetails(new AmountAndCurrencyExchange3().withTransactionAmount(withAmountAndCurrency));
            entryTransaction10.getSupplementaryData().clear();
            refillOtherId(debtorInternalAccountId, creditorInternalAccountId, entryTransaction10);

            String interbankSettlementDate = LocalDate.now().format(PATTERN);
            boolean hasFee = !BigDecimal.ZERO.equals(transactionFeeAmount);
            List<TransactionItem> items = new ArrayList<>();

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                // STEP 1a - add hold amount
                log.debug("transferTheAmountBetweenDisposalAccounts - Add hold amount");
                String holdTransactionUrl = String.format("%s%s/transactions?command=holdAmount", apiPath, debtorDisposalAccountAmsId);
                String bodyItem = painMapper.writeValueAsString(new HoldAmountBody(interbankSettlementDate, hasFee ? amount.add(transactionFeeAmount) : amount, outHoldReasonId, moneyInOutWorker.getLocale(), FORMAT));
                batchItemBuilder.add(tenantIdentifier, items, holdTransactionUrl, bodyItem, false);

                // STEP 1b - transaction details
                String paymentTypeCode1 = Optional.ofNullable(tenantConfigs.findResourceCode(tenantIdentifier, String.format("%s.%s", paymentScheme, null))).orElse("");
                entryTransaction10.setAdditionalTransactionInformation(paymentTypeCode1);
                String camt053 = serializationHelper.writeCamt053AsString(accountProductType, convertedcamt053Entry);

                String holdCamt053Body1 = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, camt053, debtorIban, paymentTypeCode1, internalCorrelationId, creditorName, creditorIban, creditorInternalAccountId, creditorContactDetails, unstructured, transactionCategoryPurposeCode, paymentScheme, null, null, endToEndId));
                batchItemBuilder.add(tenantIdentifier, items, savingsCamt053RelativeUrl, holdCamt053Body1, true);
                Long lastHoldTransactionId = moneyInOutWorker.holdBatch(items, tenantIdentifier, transactionGroupId, debtorDisposalAccountAmsId, creditorDisposalAccountAmsId, internalCorrelationId, "transferToConversionAccountInAms");

                // STEP 2 - query balance
                log.debug("transferTheAmountBetweenDisposalAccounts - Query balance");
                HttpHeaders httpHeaders = new HttpHeaders();
                httpHeaders.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
                httpHeaders.set("Authorization", authTokenHelper.generateAuthToken());
                httpHeaders.set("Fineract-Platform-TenantId", tenantIdentifier);
                httpHeaders.set("X-Correlation-ID", transactionGroupId);
                LinkedHashMap<String, Object> accountDetails = moneyInOutWorker.getRestTemplate().exchange(String.format("%s/%s%s", moneyInOutWorker.getFineractApiUrl(), apiPath, debtorDisposalAccountAmsId), HttpMethod.GET, new HttpEntity<>(httpHeaders), LinkedHashMap.class).getBody();
                LinkedHashMap<String, Object> summary = (LinkedHashMap<String, Object>) accountDetails.get("summary");
                BigDecimal availableBalance = new BigDecimal(summary.get("availableBalance").toString());
                if (availableBalance.signum() < 0) {
                    moneyInOutWorker.getRestTemplate().exchange(String.format("%s/%ssavingsaccounts/%s/transactions/%d?command=releaseAmount", moneyInOutWorker.getFineractApiUrl(), apiPath, debtorDisposalAccountAmsId, lastHoldTransactionId), HttpMethod.POST, new HttpEntity<>(httpHeaders), Object.class);
                    throw new ZeebeBpmnError("Error_InsufficientFunds", "Insufficient funds");
                }
                items.clear();

                // STEP 3 - release hold
                log.debug("transferTheAmountBetweenDisposalAccounts - Release hold");
                String releaseTransactionUrl = String.format("%s%s/transactions/%d?command=releaseAmount", apiPath, debtorDisposalAccountAmsId, lastHoldTransactionId);
                batchItemBuilder.add(tenantIdentifier, items, releaseTransactionUrl, null, false);
                String releaseAmountOperation = "transferTheAmountBetweenDisposalAccounts.Debtor.DisposalAccount.ReleaseTransactionAmount";
                String releaseOperationKey = String.format("%s.%s", paymentScheme, releaseAmountOperation);
                String paymentTypeCode2 = Optional.ofNullable(tenantConfigs.findResourceCode(tenantIdentifier, releaseOperationKey)).orElse("");
                entryTransaction10.setAdditionalTransactionInformation(paymentTypeCode2);
                String camt0531 = serializationHelper.writeCamt053AsString(accountProductType, convertedcamt053Entry);

                String camt053Body1 = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, camt0531, debtorIban, paymentTypeCode2, internalCorrelationId, creditorName, creditorIban, creditorInternalAccountId, creditorContactDetails, unstructured, transactionCategoryPurposeCode, paymentScheme, null, null, endToEndId));
                batchItemBuilder.add(tenantIdentifier, items, savingsCamt053RelativeUrl, camt053Body1, true);
            }

            // STEP 4 - withdraw amount
            log.debug("transferTheAmountBetweenDisposalAccounts - Withdraw amount");
            String direction = tenantConfigs.findDirection(tenantIdentifier, withdrawAmountConfigOperationKey);

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                String bodyItem = painMapper.writeValueAsString(new TransactionBody(interbankSettlementDate, amount, withdrawPaymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                batchItemBuilder.add(tenantIdentifier, items, debtorDisposalWithdrawalRelativeUrl, bodyItem, false);
            } // current account sends amount with details

            entryTransaction10.setAdditionalTransactionInformation(withdrawPaymentTypeCode);
            entryTransaction10.setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedcamt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedcamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001.getDocument(), entryTransaction10, transactionCategoryPurposeCode);
            entryTransaction10.getSupplementaryData().clear();
            refillOtherId(debtorInternalAccountId, creditorInternalAccountId, entryTransaction10);

            String camt053Entry = serializationHelper.writeCamt053AsString(accountProductType, convertedcamt053Entry);
            iso.std.iso._20022.tech.json.pain_001_001.CashAccount38 debtorAccount = pain001.getDocument().getPaymentInformation().get(0).getDebtorAccount();
            String debtorName = pain001.getDocument().getPaymentInformation().get(0).getDebtor().getName();

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                String camt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, camt053Entry, debtorIban, withdrawPaymentTypeCode, transactionGroupId, creditorName, creditorIban, creditorInternalAccountId, creditorContactDetails, unstructured, transactionCategoryPurposeCode, paymentScheme, debtorDisposalAccountAmsId, creditorDisposalAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, items, "datatables/dt_savings_transaction_details/$.resourceId", camt053Body, true);
            } else {
                String bodyItem = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, moneyInOutWorker.getLocale(), withdrawPaymentTypeId, currency, List.of(new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                        debtorIban,
                        camt053Entry,
                        internalCorrelationId,
                        creditorName,
                        creditorIban,
                        transactionGroupId,
                        transactionId,
                        endToEndId,
                        transactionCategoryPurposeCode,
                        paymentScheme,
                        unstructured,
                        debtorDisposalAccountAmsId,
                        creditorDisposalAccountAmsId,
                        transactionCreationChannel,
                        creditorContactDetails,
                        creditorInternalAccountId,
                        valueDated,
                        direction
                )), "dt_current_transaction_details"))));
                batchItemBuilder.add(tenantIdentifier, items, debtorDisposalWithdrawalRelativeUrl, bodyItem, false);
            }

            if (hasFee) {
                // STEP 5 - withdraw fee
                String withdrawFeeDisposalOperation = "transferTheAmountBetweenDisposalAccounts.Debtor.DisposalAccount.WithdrawTransactionFee";
                String withdrawFeeDisposalConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawFeeDisposalOperation);
                withdrawPaymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, withdrawFeeDisposalConfigOperationKey);
                withdrawPaymentTypeCode = tenantConfigs.findResourceCode(tenantIdentifier, withdrawFeeDisposalConfigOperationKey);

                if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                    var bodyItem = painMapper.writeValueAsString(new TransactionBody(interbankSettlementDate, transactionFeeAmount, withdrawPaymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                    batchItemBuilder.add(tenantIdentifier, items, debtorDisposalWithdrawalRelativeUrl, bodyItem, false);
                }

                entryTransaction10.getSupplementaryData().get(0).getEnvelope().setAdditionalProperty("InternalCorrelationId", transactionFeeInternalCorrelationId);
                entryTransaction10.getAmountDetails().getTransactionAmount().getAmount().setAmount(transactionFeeAmount);
                entryTransaction10.setAdditionalTransactionInformation(withdrawPaymentTypeCode);
                entryTransaction10.getSupplementaryData().clear();
                camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001.getDocument(), entryTransaction10, transactionFeeCategoryPurposeCode);
                refillOtherId(debtorInternalAccountId, creditorInternalAccountId, entryTransaction10);
                camt053Entry = serializationHelper.writeCamt053AsString(accountProductType, convertedcamt053Entry);

                if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                    var camt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(transactionFeeInternalCorrelationId, camt053Entry, debtorIban, withdrawPaymentTypeCode, transactionGroupId, creditorName, creditorIban, creditorIban.substring(creditorIban.length() - 8), creditorContactDetails, unstructured, transactionFeeCategoryPurposeCode, paymentScheme, debtorDisposalAccountAmsId, debtorConversionAccountAmsId, creditTransfer.getPaymentIdentification().getEndToEndIdentification()));
                    batchItemBuilder.add(tenantIdentifier, items, savingsCamt053RelativeUrl, camt053Body, true);
                } else {
                    var camt053Body = painMapper.writeValueAsString(new CurrentAccountTransactionBody(transactionFeeAmount, FORMAT, moneyInOutWorker.getLocale(), withdrawPaymentTypeId, currency, List.of(new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                            debtorIban,
                            camt053Entry,
                            internalCorrelationId,
                            creditorName,
                            creditorIban,
                            transactionGroupId,
                            transactionId,
                            endToEndId,
                            transactionFeeCategoryPurposeCode,
                            paymentScheme,
                            unstructured,
                            debtorDisposalAccountAmsId,
                            debtorConversionAccountAmsId,
                            transactionCreationChannel,
                            creditorContactDetails,
                            creditorInternalAccountId,
                            valueDated,
                            direction
                    )), "dt_current_transaction_details"))));
                    batchItemBuilder.add(tenantIdentifier, items, debtorDisposalWithdrawalRelativeUrl, camt053Body, false);
                }

                // STEP 6 - deposit fee
                String depositFeeOperation = "transferTheAmountBetweenDisposalAccounts.Debtor.ConversionAccount.DepositTransactionFee";
                String depositFeeConfigOperationKey = String.format("%s.%s", paymentScheme, depositFeeOperation);
                direction = tenantConfigs.findDirection(tenantIdentifier, depositFeeConfigOperationKey);
                var depositPaymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, depositFeeConfigOperationKey);
                var depositPaymentTypeCode = tenantConfigs.findResourceCode(tenantIdentifier, depositFeeConfigOperationKey);
                entryTransaction10.setAdditionalTransactionInformation(depositPaymentTypeCode);
                entryTransaction10.getSupplementaryData().clear();
                entryTransaction10.setCreditDebitIndicator(CreditDebitCode.CRDT);
                convertedcamt053Entry.setCreditDebitIndicator(CreditDebitCode.CRDT);
                convertedcamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
                camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001.getDocument(), entryTransaction10, transactionFeeCategoryPurposeCode);
                refillOtherId(debtorInternalAccountId, creditorInternalAccountId, entryTransaction10);
                camt053Entry = serializationHelper.writeCamt053AsString(accountProductType, convertedcamt053Entry);
                String debtorConversionDepositRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, debtorConversionAccountAmsId, "deposit");

                if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                    var bodyItem = painMapper.writeValueAsString(new TransactionBody(interbankSettlementDate, transactionFeeAmount, depositPaymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                    batchItemBuilder.add(tenantIdentifier, items, debtorConversionDepositRelativeUrl, bodyItem, false);

                    var camt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(transactionFeeInternalCorrelationId, camt053Entry, debtorIban, depositPaymentTypeCode, transactionGroupId, creditorName, creditorIban, creditorInternalAccountId, creditorContactDetails, unstructured, transactionFeeCategoryPurposeCode, paymentScheme, debtorDisposalAccountAmsId, debtorConversionAccountAmsId, endToEndId));
                    batchItemBuilder.add(tenantIdentifier, items, savingsCamt053RelativeUrl, camt053Body, true);
                } else {
                    var camt053Body = painMapper.writeValueAsString(new CurrentAccountTransactionBody(transactionFeeAmount, FORMAT, moneyInOutWorker.getLocale(), depositPaymentTypeId, currency, List.of(new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                            debtorIban,
                            camt053Entry,
                            internalCorrelationId,
                            creditorName,
                            creditorIban,
                            transactionGroupId,
                            transactionId,
                            endToEndId,
                            transactionFeeCategoryPurposeCode,
                            paymentScheme,
                            unstructured,
                            debtorDisposalAccountAmsId,
                            debtorConversionAccountAmsId,
                            transactionCreationChannel,
                            creditorContactDetails,
                            creditorInternalAccountId,
                            valueDated,
                            direction
                    )), "dt_current_transaction_details"))));
                    batchItemBuilder.add(tenantIdentifier, items, debtorConversionDepositRelativeUrl, camt053Body, false);
                }
            }

            direction = tenantConfigs.findDirection(tenantIdentifier, depositAmountConfigOperationKey);
            var depositPaymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, depositAmountConfigOperationKey);
            var depositPaymentTypeCode = tenantConfigs.findResourceCode(tenantIdentifier, depositAmountConfigOperationKey);
            entryTransaction10.setAdditionalTransactionInformation(depositPaymentTypeCode);
            entryTransaction10.getSupplementaryData().clear();
            camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001.getDocument(), entryTransaction10, transactionCategoryPurposeCode);
            refillOtherId(debtorInternalAccountId, creditorInternalAccountId, entryTransaction10);
            String creditorDisposalDepositRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, creditorDisposalAccountAmsId, "deposit");

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                var bodyItem = painMapper.writeValueAsString(new TransactionBody(interbankSettlementDate, amount, depositPaymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                batchItemBuilder.add(tenantIdentifier, items, creditorDisposalDepositRelativeUrl, bodyItem, false);
            }

            entryTransaction10.getSupplementaryData().get(0).getEnvelope().setAdditionalProperty("InternalCorrelationId", internalCorrelationId);
            entryTransaction10.getAmountDetails().getTransactionAmount().getAmount().setAmount(amount);
            entryTransaction10.setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedcamt053Entry.setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedcamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            camt053Entry = serializationHelper.writeCamt053AsString(accountProductType, convertedcamt053Entry);

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                var camt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, camt053Entry, creditorIban, depositPaymentTypeCode, transactionGroupId, debtorName, debtorIban, debtorIban.substring(debtorIban.length() - 8), debtorContactDetails, unstructured, transactionCategoryPurposeCode, paymentScheme, debtorDisposalAccountAmsId, creditorDisposalAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, items, savingsCamt053RelativeUrl, camt053Body, true);
            } else {
                var camt053Body = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, moneyInOutWorker.getLocale(), depositPaymentTypeId, currency, List.of(new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                        creditorIban,
                        camt053Entry,
                        internalCorrelationId,
                        debtorName,
                        debtorIban,
                        transactionGroupId,
                        transactionId,
                        endToEndId,
                        transactionCategoryPurposeCode,
                        paymentScheme,
                        unstructured,
                        debtorDisposalAccountAmsId,
                        creditorDisposalAccountAmsId,
                        transactionCreationChannel,
                        creditorContactDetails,
                        debtorInternalAccountId,
                        valueDated,
                        direction
                )), "dt_current_transaction_details"))));
                batchItemBuilder.add(tenantIdentifier, items, creditorDisposalDepositRelativeUrl, camt053Body, false);
            }

            // STEP 7: withdraw fee
            if (hasFee) {
                String withdrawFeeConversionOperation = "transferTheAmountBetweenDisposalAccounts.Debtor.ConversionAccount.WithdrawTransactionFee";
                String withdrawFeeConversionConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawFeeConversionOperation);
                depositPaymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, withdrawFeeConversionConfigOperationKey);
                depositPaymentTypeCode = tenantConfigs.findResourceCode(tenantIdentifier, withdrawFeeConversionConfigOperationKey);
                entryTransaction10.setAdditionalTransactionInformation(depositPaymentTypeCode);
                entryTransaction10.getSupplementaryData().clear();
                entryTransaction10.setCreditDebitIndicator(CreditDebitCode.DBIT);
                convertedcamt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
                convertedcamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
                refillOtherId(debtorInternalAccountId, creditorInternalAccountId, entryTransaction10);
                String debtorConversionWithdrawRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, debtorConversionAccountAmsId, "withdrawal");

                if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                    var bodyItem = painMapper.writeValueAsString(new TransactionBody(interbankSettlementDate, transactionFeeAmount, depositPaymentTypeId, "", FORMAT, moneyInOutWorker.getLocale()));
                    batchItemBuilder.add(tenantIdentifier, items, debtorConversionWithdrawRelativeUrl, bodyItem, false);
                }

                entryTransaction10.getAmountDetails().getTransactionAmount().getAmount().setAmount(transactionFeeAmount);
                entryTransaction10.getSupplementaryData().clear();
                camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001.getDocument(), entryTransaction10, transactionFeeCategoryPurposeCode);
                refillOtherId(debtorInternalAccountId, creditorInternalAccountId, entryTransaction10);
                entryTransaction10.getSupplementaryData().get(0).getEnvelope().setAdditionalProperty("InternalCorrelationId", transactionFeeInternalCorrelationId);
                camt053Entry = serializationHelper.writeCamt053AsString(accountProductType, convertedcamt053Entry);

                if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                    var camt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(transactionFeeInternalCorrelationId, camt053Entry, debtorIban, depositPaymentTypeCode, transactionGroupId, creditorName, creditorIban, null, creditorContactDetails, unstructured, transactionFeeCategoryPurposeCode, paymentScheme, debtorConversionAccountAmsId, null, endToEndId));
                    batchItemBuilder.add(tenantIdentifier, items, savingsCamt053RelativeUrl, camt053Body, true);
                } else {
                    var camt053Body = painMapper.writeValueAsString(new CurrentAccountTransactionBody(transactionFeeAmount, FORMAT, moneyInOutWorker.getLocale(), depositPaymentTypeId, currency, List.of(new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                            debtorIban,
                            camt053Entry,
                            transactionFeeInternalCorrelationId,
                            creditorName,
                            creditorIban,
                            transactionGroupId,
                            transactionId,
                            endToEndId,
                            transactionFeeCategoryPurposeCode,
                            paymentScheme,
                            unstructured,
                            debtorConversionAccountAmsId,
                            null,
                            transactionCreationChannel,
                            creditorContactDetails,
                            creditorInternalAccountId,
                            valueDated,
                            direction
                    )), "dt_current_transaction_details"))));
                    batchItemBuilder.add(tenantIdentifier, items, debtorConversionWithdrawRelativeUrl, camt053Body, false);
                }
            }

            moneyInOutWorker.doBatchOnUs(items, tenantIdentifier, transactionGroupId, debtorDisposalAccountAmsId, debtorConversionAccountAmsId, creditorDisposalAccountAmsId, internalCorrelationId);
            notificationHelper.send("transferTheAmountBetweenDisposalAccounts", amount, currency, debtorName, paymentScheme, creditorIban);

            return Map.of("transactionDate", interbankSettlementDate);
        } catch (JsonProcessingException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("failed to create camt.053", e);
        }
    }

    private void refillOtherId(String debtorInternalAccountId, String creditorInternalAccountId,
                               EntryTransaction10 entryTransaction10) {
        boolean isWritten = false;
        for (SupplementaryData1 supplementaryData : entryTransaction10.getSupplementaryData()) {
            if ("OrderManagerSupplementaryData".equalsIgnoreCase(supplementaryData.getPlaceAndName())) {
                SupplementaryDataEnvelope1 envelope = supplementaryData.getEnvelope();
                doRefillOtherId(debtorInternalAccountId, creditorInternalAccountId, entryTransaction10, envelope);
                isWritten = true;
            }
        }

        if (!isWritten) {
            List<SupplementaryData1> sds = entryTransaction10.getSupplementaryData();
            if (sds.isEmpty()) {
                sds.add(new SupplementaryData1());
            }
            SupplementaryData1 supplementaryData = sds.get(0);
            SupplementaryDataEnvelope1 envelope = supplementaryData.getEnvelope();
            if (envelope == null) {
                envelope = new SupplementaryDataEnvelope1();
                supplementaryData.setEnvelope(envelope);
            }

            doRefillOtherId(debtorInternalAccountId, creditorInternalAccountId, entryTransaction10, envelope);
        }
    }

    @SuppressWarnings("unchecked")
    private void doRefillOtherId(String debtorInternalAccountId, String creditorInternalAccountId,
                                 EntryTransaction10 entryTransaction10, SupplementaryDataEnvelope1 envelope) {
        LinkedHashMap<String, Object> otherIdentification = (LinkedHashMap<String, Object>) envelope.getAdditionalProperties().getOrDefault("OtherIdentification", new LinkedHashMap<>());
        envelope.setAdditionalProperty("OtherIdentification", otherIdentification);

        GenericAccountIdentification1 debtorAccountIdOther = (GenericAccountIdentification1) entryTransaction10.getRelatedParties().getDebtorAccount().getIdentification().getAdditionalProperties().getOrDefault("Other", GenericAccountIdentification1.builder().build());
        debtorAccountIdOther.setId(debtorInternalAccountId);
        debtorAccountIdOther.setSchemeName(AccountSchemeName1Choice.builder().code("IAID").build());

        GenericAccountIdentification1 creditorAccountIdOther = (GenericAccountIdentification1) entryTransaction10.getRelatedParties().getCreditorAccount().getIdentification().getAdditionalProperties().getOrDefault("Other", GenericAccountIdentification1.builder().build());
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

}