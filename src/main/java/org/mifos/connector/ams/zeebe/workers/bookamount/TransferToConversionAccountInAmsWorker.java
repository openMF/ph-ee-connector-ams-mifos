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
import iso.std.iso._20022.tech.json.camt_053_001.ActiveOrHistoricCurrencyAndAmountRange2.CreditDebitCode;
import iso.std.iso._20022.tech.json.camt_053_001.*;
import iso.std.iso._20022.tech.json.pain_001_001.CreditTransferTransaction40;
import iso.std.iso._20022.tech.json.pain_001_001.CustomerCreditTransferInitiationV10;
import iso.std.iso._20022.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;
import iso.std.iso._20022.tech.json.pain_001_001.PaymentInstruction34;
import jakarta.xml.bind.JAXBException;
import lombok.extern.slf4j.Slf4j;
import org.mifos.connector.ams.common.SerializationHelper;
import org.mifos.connector.ams.fineract.Config;
import org.mifos.connector.ams.fineract.ConfigFactory;
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

@Component
@Slf4j
public class TransferToConversionAccountInAmsWorker extends AbstractMoneyInOutWorker {

    @Autowired
    private Pain001Camt053Mapper camt053Mapper;

    @Value("${fineract.incoming-money-api}")
    protected String incomingMoneyApi;

    @Value("${fineract.current-account-api}")
    protected String currentAccountApi;

    @Autowired
    private ConfigFactory paymentTypeConfigFactory;

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

    private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);

    @JobWorker
    @LogInternalCorrelationId
    @TraceZeebeArguments
    public void transferToConversionAccountInAms(JobClient jobClient,
                                                 ActivatedJob activatedJob,
                                                 @Variable String transactionGroupId,
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
                                                 @Variable String accountProductType) {
        log.info("transferToConversionAccountInAms");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "transferToConversionAccountInAms", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> transferToConversionAccountInAms(transactionGroupId,
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
                        accountProductType));
    }

    @SuppressWarnings("unchecked")
    private Void transferToConversionAccountInAms(String transactionGroupId,
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
                                                  String accountProductType) {
        try {
            // STEP 0 - collect / extract information
            String transactionDate = LocalDate.now().format(PATTERN);
            String apiPath = accountProductType.equalsIgnoreCase("SAVINGS") ? incomingMoneyApi.substring(1) : currentAccountApi.substring(1);

            MDC.put("internalCorrelationId", internalCorrelationId);
            log.debug("Debtor exchange worker starting, using api path {}", apiPath);

            String disposalAccountWithdrawRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, disposalAccountAmsId, "withdrawal");
            String conversionAccountDepositRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, conversionAccountAmsId, "deposit");
            Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);

            Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = painMapper.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);

            log.debug("Withdrawing amount {} from disposal account {} with fee: {}", amount, disposalAccountAmsId, transactionFeeAmount);
            boolean hasFee = !BigDecimal.ZERO.equals(transactionFeeAmount);
            BigDecimal totalAmountWithFee = hasFee ? amount.add(transactionFeeAmount) : amount;

            BankToCustomerStatementV08 convertedStatement = camt053Mapper.toCamt053Entry(pain001.getDocument());
            ReportEntry10 convertedCamt053Entry = convertedStatement.getStatement().get(0).getEntry().get(0);
            EntryTransaction10 transactionDetails = convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0);
            transactionDetails.getAmountDetails().getTransactionAmount().getAmount().setAmount(totalAmountWithFee);
            PaymentInstruction34 pain001PaymentInstruction = pain001.getDocument().getPaymentInformation().get(0);
            CreditTransferTransaction40 pain001Transaction = pain001PaymentInstruction.getCreditTransferTransactionInformation().get(0);
            String partnerName = pain001Transaction.getCreditor().getName();
            String partnerAccountIban = pain001Transaction.getCreditorAccount().getIdentification().getIban();
            String partnerAccountSecondaryIdentifier = contactDetailsUtil.getId(pain001Transaction.getCreditor().getContactDetails());
            String unstructured = Optional.ofNullable(pain001Transaction.getRemittanceInformation())
                    .map(iso.std.iso._20022.tech.json.pain_001_001.RemittanceInformation16::getUnstructured).map(List::toString)
                    .orElse("");
            CustomerCreditTransferInitiationV10 pain001Document = pain001.getDocument();
            String endToEndId = pain001Transaction.getPaymentIdentification().getEndToEndIdentification();
            String debtorIban = pain001PaymentInstruction.getDebtorAccount().getIdentification().getIban();


            // STEP 1 - batch: add hold and release items [only for savings account]
            List<TransactionItem> items = new ArrayList<>();
            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                holdAndReleaseForSavingsAccount(transactionGroupId, transactionCategoryPurposeCode, internalCorrelationId, paymentScheme, disposalAccountAmsId, conversionAccountAmsId, tenantIdentifier, debtorIban, accountProductType, paymentTypeConfig, transactionDate, totalAmountWithFee, items, transactionDetails, pain001Document, convertedCamt053Entry, partnerName, partnerAccountIban, partnerAccountSecondaryIdentifier, unstructured, endToEndId, pain001, pain001Transaction);
            } else {
                log.info("No hold and release because disposal account {} is not a savings account", disposalAccountAmsId);
            }

            // STEP 2a - batch: add withdraw amount
            transactionDetails.setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "PENDING"));

            String withdrawAmountOperation = "transferToConversionAccountInAms.DisposalAccount.WithdrawTransactionAmount";
            Integer withdrawAmountPaymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(String.format("%s.%s", paymentScheme, withdrawAmountOperation));

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                String withdrawAmountTransactionBody = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, withdrawAmountPaymentTypeId, "", FORMAT, locale));
                batchItemBuilder.add(tenantIdentifier, items, disposalAccountWithdrawRelativeUrl, withdrawAmountTransactionBody, false);
            } // CURRENT account sends a single call only at the details step

            // STEP 2b - batch: add withdrawal details
            transactionDetails.getAmountDetails().getTransactionAmount().getAmount().setAmount(amount);
            String paymentTypeCode = Optional.ofNullable(paymentTypeConfig.findPaymentTypeCodeByOperation(String.format("%s.%s", paymentScheme, withdrawAmountOperation))).orElse("");
            transactionDetails.setAdditionalTransactionInformation(paymentTypeCode);
            if (pain001Document != null) {
                transactionDetails.getSupplementaryData().clear();
                camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001Document, transactionDetails, transactionCategoryPurposeCode);
                camt053Mapper.refillOtherIdentification(pain001Document, transactionDetails);
            }
            String withdrawAmountCamt053 = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                String withdrawAmountCamt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, withdrawAmountCamt053, debtorIban, paymentTypeCode, transactionGroupId, partnerName, partnerAccountIban, null, partnerAccountSecondaryIdentifier, unstructured, transactionCategoryPurposeCode, paymentScheme, disposalAccountAmsId, conversionAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, items, "datatables/dt_savings_transaction_details/$.resourceId", withdrawAmountCamt053Body, true);

            } else {  // CURRENT account executes withdrawal and details in one step
                String withdrawAmountTransactionBody = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, locale, withdrawAmountPaymentTypeId, currency, List.of(
                        new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(debtorIban, withdrawAmountCamt053, internalCorrelationId, partnerName, partnerAccountIban)), "dt_current_transaction_details"))
                ));
                batchItemBuilder.add(tenantIdentifier, items, disposalAccountWithdrawRelativeUrl, withdrawAmountTransactionBody, false);
            }

            // STEP 2c - batch: add withdrawal fee, if there is any
            if (hasFee) {
                log.debug("Withdrawing fee {} from disposal account {}", transactionFeeAmount, disposalAccountAmsId);
                String withdrawFeeOperation = "transferToConversionAccountInAms.DisposalAccount.WithdrawTransactionFee";
                Integer withdrawFeePaymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(String.format("%s.%s", paymentScheme, withdrawFeeOperation));
                if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                    String withdrawFeeTransactionBody = painMapper.writeValueAsString(new TransactionBody(transactionDate, transactionFeeAmount, withdrawFeePaymentTypeId, "", FORMAT, locale));
                    batchItemBuilder.add(tenantIdentifier, items, disposalAccountWithdrawRelativeUrl, withdrawFeeTransactionBody, false);
                } // CURRENT account sends a single call only at the details step

                if (transactionDetails.getSupplementaryData() != null) {
                    transactionDetails.getSupplementaryData().get(0).getEnvelope().setAdditionalProperty("InternalCorrelationId", transactionFeeInternalCorrelationId);
                } else {
                    transactionDetails.getSupplementaryData().add(new SupplementaryData1().withEnvelope(new SupplementaryDataEnvelope1().withAdditionalProperty("InternalCorrelationId", transactionFeeInternalCorrelationId)));
                }
                transactionDetails.getAmountDetails().getTransactionAmount().getAmount().setAmount(transactionFeeAmount);

                String withdrawFeePaymentTypeCode = Optional.ofNullable(paymentTypeConfig.findPaymentTypeCodeByOperation(String.format("%s.%s", paymentScheme, withdrawFeeOperation))).orElse("");
                transactionDetails.setAdditionalTransactionInformation(withdrawFeePaymentTypeCode);
                if (pain001Document != null) {
                    transactionDetails.getSupplementaryData().clear();
                    camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001Document, transactionDetails, transactionFeeCategoryPurposeCode);
                    camt053Mapper.refillOtherIdentification(pain001Document, transactionDetails);
                }
                String withdrawFeeCamt053 = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);
                if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                    String withdrawFeeCamt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(transactionFeeInternalCorrelationId, withdrawFeeCamt053, debtorIban, withdrawFeePaymentTypeCode, transactionGroupId, partnerName, partnerAccountIban, null, partnerAccountSecondaryIdentifier, unstructured, transactionFeeCategoryPurposeCode, paymentScheme, disposalAccountAmsId, conversionAccountAmsId, endToEndId));
                    batchItemBuilder.add(tenantIdentifier, items, "datatables/dt_savings_transaction_details/$.resourceId", withdrawFeeCamt053Body, true);

                } else { // CURRENT account executes withdrawal and details in one step
                    String withdrawFeeTransactionBody = painMapper.writeValueAsString(new CurrentAccountTransactionBody(transactionFeeAmount, FORMAT, locale, withdrawAmountPaymentTypeId, currency, List.of(
                            new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(debtorIban, withdrawFeeCamt053, transactionFeeInternalCorrelationId, partnerName, partnerAccountIban)), "dt_current_transaction_details"))
                    ));
                    batchItemBuilder.add(tenantIdentifier, items, disposalAccountWithdrawRelativeUrl, withdrawFeeTransactionBody, false);
                }
            }

            // STEP 3a - batch: deposit amount
            log.info("Depositing amount {} to conversion account {}", amount, conversionAccountAmsId);
            String depositAmountOperation = "transferToConversionAccountInAms.ConversionAccount.DepositTransactionAmount";
            Integer depositAmountPaymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(String.format("%s.%s", paymentScheme, depositAmountOperation));
            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                String depositAmountTransactionBody = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, depositAmountPaymentTypeId, "", FORMAT, locale));
                batchItemBuilder.add(tenantIdentifier, items, conversionAccountDepositRelativeUrl, depositAmountTransactionBody, false);
            }

            // STEP 3b - batch: add deposit details
            transactionDetails.getSupplementaryData().get(0).getEnvelope().setAdditionalProperty("InternalCorrelationId", internalCorrelationId);
            transactionDetails.getAmountDetails().getTransactionAmount().getAmount().setAmount(amount);
            transactionDetails.setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.CRDT);
            String depositAmountPaymentTypeCode = Optional.ofNullable(paymentTypeConfig.findPaymentTypeCodeByOperation(String.format("%s.%s", paymentScheme, depositAmountOperation))).orElse("");
            transactionDetails.setAdditionalTransactionInformation(depositAmountPaymentTypeCode);
            if (pain001Document != null) {
                transactionDetails.getSupplementaryData().clear();
                camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001Document, transactionDetails, transactionCategoryPurposeCode);
                camt053Mapper.refillOtherIdentification(pain001Document, transactionDetails);
            }
            String depositAmountCamt053 = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);
            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                String depositAmountCamt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, depositAmountCamt053, debtorIban, depositAmountPaymentTypeCode, transactionGroupId, partnerName, partnerAccountIban, null, partnerAccountSecondaryIdentifier, unstructured, transactionCategoryPurposeCode, paymentScheme, disposalAccountAmsId, conversionAccountAmsId, endToEndId));
                batchItemBuilder.add(tenantIdentifier, items, "datatables/dt_savings_transaction_details/$.resourceId", depositAmountCamt053Body, true);
            } else { // CURRENT account executes deposit amount and details in one step
                String depositAmountTransactionBody = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, locale, depositAmountPaymentTypeId, currency, List.of(
                        new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(debtorIban, depositAmountCamt053, internalCorrelationId, partnerName, partnerAccountIban)), "dt_current_transaction_details"))
                ));
                batchItemBuilder.add(tenantIdentifier, items, conversionAccountDepositRelativeUrl, depositAmountTransactionBody, false);
            }

            // STEP 3c - batch: add deposit fee, if any
            if (hasFee) {
                log.debug("Depositing fee {} to conversion account {}", transactionFeeAmount, conversionAccountAmsId);
                String depositFeeOperation = "transferToConversionAccountInAms.ConversionAccount.DepositTransactionFee";
                Integer depositFeePaymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(String.format("%s.%s", paymentScheme, depositFeeOperation));
                if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                    String depositFeeTransactionBody = painMapper.writeValueAsString(new TransactionBody(transactionDate, transactionFeeAmount, depositFeePaymentTypeId, "", FORMAT, locale));
                    batchItemBuilder.add(tenantIdentifier, items, conversionAccountDepositRelativeUrl, depositFeeTransactionBody, false);
                }

                transactionDetails.getSupplementaryData().get(0).getEnvelope().setAdditionalProperty("InternalCorrelationId", transactionFeeInternalCorrelationId);
                transactionDetails.getAmountDetails().getTransactionAmount().getAmount().setAmount(transactionFeeAmount);

                String depositFeePaymentTypeCode = Optional.ofNullable(paymentTypeConfig.findPaymentTypeCodeByOperation(String.format("%s.%s", paymentScheme, depositFeeOperation))).orElse("");
                transactionDetails.setAdditionalTransactionInformation(depositFeePaymentTypeCode);
                if (pain001Document != null) {
                    transactionDetails.getSupplementaryData().clear();
                    camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001Document, transactionDetails, transactionFeeCategoryPurposeCode);
                    camt053Mapper.refillOtherIdentification(pain001Document, transactionDetails);
                }
                String depositFeeCamt053 = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);
                if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                    String depositFeeCamt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(transactionFeeInternalCorrelationId, depositFeeCamt053, debtorIban, depositFeePaymentTypeCode, transactionGroupId, partnerName, partnerAccountIban, null, partnerAccountSecondaryIdentifier, unstructured, transactionFeeCategoryPurposeCode, paymentScheme, disposalAccountAmsId, conversionAccountAmsId, endToEndId));
                    batchItemBuilder.add(tenantIdentifier, items, "datatables/dt_savings_transaction_details/$.resourceId", depositFeeCamt053Body, true);
                } else { // CURRENT account executes deposit fee and details in one step
                    String depositFeeTransactionBody = painMapper.writeValueAsString(new CurrentAccountTransactionBody(transactionFeeAmount, FORMAT, locale, depositFeePaymentTypeId, currency, List.of(
                            new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(debtorIban, depositFeeCamt053, transactionFeeInternalCorrelationId, partnerName, partnerAccountIban)), "dt_current_transaction_details"))
                    ));
                    batchItemBuilder.add(tenantIdentifier, items, conversionAccountDepositRelativeUrl, depositFeeTransactionBody, false);
                }
            }

            doBatch(items, tenantIdentifier, disposalAccountAmsId, conversionAccountAmsId, internalCorrelationId, "transferToConversionAccountInAms");

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

    private void holdAndReleaseForSavingsAccount(String transactionGroupId, String transactionCategoryPurposeCode, String internalCorrelationId, String paymentScheme, String disposalAccountAmsId, String conversionAccountAmsId, String tenantIdentifier, String iban, String accountProductType, Config paymentTypeConfig, String transactionDate, BigDecimal totalAmountWithFee, List<TransactionItem> items, EntryTransaction10 transactionDetails, CustomerCreditTransferInitiationV10 pain0011, ReportEntry10 convertedCamt053Entry, String partnerName, String partnerAccountIban, String partnerAccountSecondaryIdentifier, String unstructured, String endToEndId, Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001, CreditTransferTransaction40 pain001Transaction) throws JsonProcessingException {
        log.info("Adding hold item to disposal account {}", disposalAccountAmsId);
        Integer outHoldReasonId = paymentTypeConfig.findPaymentTypeIdByOperation(String.format("%s.%s", paymentScheme, "outHoldReasonId"));
        String bodyItem = painMapper.writeValueAsString(new HoldAmountBody(transactionDate, totalAmountWithFee, outHoldReasonId, locale, FORMAT));
        String holdTransactionUrl = String.format("%s%s/transactions?command=holdAmount", incomingMoneyApi.substring(1), disposalAccountAmsId);
        batchItemBuilder.add(tenantIdentifier, items, holdTransactionUrl, bodyItem, false);

        String holdAmountOperation = "transferToConversionAccountInAms.DisposalAccount.HoldTransactionAmount";
        String paymentTypeCode = Optional.ofNullable(paymentTypeConfig.findPaymentTypeCodeByOperation(String.format("%s.%s", paymentScheme, holdAmountOperation))).orElse("");
        transactionDetails.setAdditionalTransactionInformation(paymentTypeCode);
        if (pain0011 != null) {
            transactionDetails.getSupplementaryData().clear();
            camt053Mapper.fillOtherIdentification(pain0011, transactionDetails);
        }
        String camt053 = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);
        DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(internalCorrelationId, camt053, iban, paymentTypeCode, transactionGroupId, partnerName, partnerAccountIban, null, partnerAccountSecondaryIdentifier, unstructured, transactionCategoryPurposeCode, paymentScheme, null, null, endToEndId);
        String camt053Body = painMapper.writeValueAsString(td);
        batchItemBuilder.add(tenantIdentifier, items, "datatables/dt_savings_transaction_details/$.resourceId", camt053Body, true);

        Long lastHoldTransactionId = holdBatch(items, tenantIdentifier, disposalAccountAmsId, conversionAccountAmsId, internalCorrelationId, "transferToConversionAccountInAms");

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
        httpHeaders.set("Authorization", authTokenHelper.generateAuthToken());
        httpHeaders.set("Fineract-Platform-TenantId", tenantIdentifier);
        LinkedHashMap<String, Object> accountDetails = restTemplate.exchange(
                String.format("%s/%s%s", fineractApiUrl, incomingMoneyApi.substring(1), disposalAccountAmsId),
                HttpMethod.GET,
                new HttpEntity<>(httpHeaders),
                LinkedHashMap.class
        ).getBody();
        LinkedHashMap<String, Object> summary = (LinkedHashMap<String, Object>) accountDetails.get("summary");
        BigDecimal availableBalance = new BigDecimal(summary.get("availableBalance").toString());
        if (availableBalance.signum() < 0) {
            restTemplate.exchange(
                    String.format("%s/%ssavingsaccounts/%s/transactions/%d?command=releaseAmount", fineractApiUrl, incomingMoneyApi.substring(1), disposalAccountAmsId, lastHoldTransactionId),
                    HttpMethod.POST,
                    new HttpEntity<>(httpHeaders),
                    Object.class
            );
            throw new ZeebeBpmnError("Error_InsufficientFunds", "Insufficient funds");
        }


        // prepare release transaction
        items.clear();

        String releaseTransactionUrl = String.format("%s%s/transactions/%d?command=releaseAmount", incomingMoneyApi.substring(1), disposalAccountAmsId, lastHoldTransactionId);
        batchItemBuilder.add(tenantIdentifier, items, releaseTransactionUrl, null, false);
        String releaseAmountOperation = "transferToConversionAccountInAms.DisposalAccount.ReleaseTransactionAmount";
        addDetails(tenantIdentifier, pain001.getDocument(), transactionGroupId, transactionCategoryPurposeCode, internalCorrelationId,
                accountProductType, batchItemBuilder, items, convertedCamt053Entry, "datatables/dt_savings_transaction_details/$.resourceId", iban,
                paymentTypeConfig, paymentScheme, releaseAmountOperation, partnerName, partnerAccountIban,
                partnerAccountSecondaryIdentifier, unstructured, null, null, false, pain001Transaction.getPaymentIdentification().getEndToEndIdentification());
    }

    private void addExchange(
            String tenantIdentifier,
            BigDecimal amount,
            String paymentScheme,
            String transactionDate,
            ObjectMapper om,
            Config paymentTypeConfig,
            BatchItemBuilder batchItemBuilder,
            List<TransactionItem> items,
            String relativeUrl,
            String paymentTypeOperation) throws JsonProcessingException {
        Integer paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(String.format("%s.%s", paymentScheme, paymentTypeOperation));

        TransactionBody transactionBody = new TransactionBody(transactionDate, amount, paymentTypeId, "", FORMAT, locale);

        String bodyItem = om.writeValueAsString(transactionBody);
        batchItemBuilder.add(tenantIdentifier, items, relativeUrl, bodyItem, false);
    }

    private void addDetails(
            String tenantIdentifier,
            CustomerCreditTransferInitiationV10 pain001,
            String transactionGroupId,
            String transactionFeeCategoryPurposeCode,
            String internalCorrelationId,
            String accountProductType,
            BatchItemBuilder batchItemBuilder,
            List<TransactionItem> items,
            ReportEntry10 convertedCamt053Entry,
            String camt053RelativeUrl,
            String accountIban,
            Config paymentTypeConfig,
            String paymentScheme,
            String paymentTypeOperation,
            String partnerName,
            String partnerAccountIban,
            String partnerAccountSecondaryIdentifier,
            String unstructured,
            String sourceAmsAccountId,
            String targetAmsAccountId,
            boolean includeSupplementary,
            String endToEndId) throws JsonProcessingException {
        String paymentTypeCode = Optional.ofNullable(paymentTypeConfig.findPaymentTypeCodeByOperation(String.format("%s.%s", paymentScheme, paymentTypeOperation))).orElse("");
        EntryTransaction10 transactionDetails = convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0);
        transactionDetails.setAdditionalTransactionInformation(paymentTypeCode);
        if (pain001 != null) {
            transactionDetails.getSupplementaryData().clear();
            if (includeSupplementary) {
                camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001, transactionDetails, transactionFeeCategoryPurposeCode);
                camt053Mapper.refillOtherIdentification(pain001, transactionDetails);
            } else {
                camt053Mapper.fillOtherIdentification(pain001, transactionDetails);
            }
        }
        String camt053 = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);
        DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
                internalCorrelationId,
                camt053,
                accountIban,
                paymentTypeCode,
                transactionGroupId,
                partnerName,
                partnerAccountIban,
                null,
                partnerAccountSecondaryIdentifier,
                unstructured,
                transactionFeeCategoryPurposeCode,
                paymentScheme,
                sourceAmsAccountId,
                targetAmsAccountId,
                endToEndId);

        String camt053Body = painMapper.writeValueAsString(td);
        batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);
    }

    @JobWorker
    @TraceZeebeArguments
    @LogInternalCorrelationId
    public void withdrawTheAmountFromDisposalAccountInAMS(JobClient client,
                                                          ActivatedJob activatedJob,
                                                          @Variable BigDecimal amount,
                                                          @Variable String conversionAccountAmsId,
                                                          @Variable String disposalAccountAmsId,
                                                          @Variable String tenantIdentifier,
                                                          @Variable String paymentScheme,
                                                          @Variable String transactionCategoryPurposeCode,
                                                          @Variable String camt056,
                                                          @Variable String iban,
                                                          @Variable String internalCorrelationId,
                                                          @Variable String accountProductType) {
        log.info("withdrawTheAmountFromDisposalAccountInAMS");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "withdrawTheAmountFromDisposalAccountInAMS",
                        null,
                        null,
                        eventBuilder),
                eventBuilder -> withdrawTheAmountFromDisposalAccountInAMS(amount,
                        conversionAccountAmsId,
                        disposalAccountAmsId,
                        tenantIdentifier,
                        paymentScheme,
                        transactionCategoryPurposeCode,
                        camt056,
                        iban,
                        internalCorrelationId,
                        accountProductType));
    }

    @SuppressWarnings("unchecked")
    private Void withdrawTheAmountFromDisposalAccountInAMS(BigDecimal amount,
                                                           String conversionAccountAmsId,
                                                           String disposalAccountAmsId,
                                                           String tenantIdentifier,
                                                           String paymentScheme,
                                                           String transactionCategoryPurposeCode,
                                                           String camt056,
                                                           String iban,
                                                           String internalCorrelationId,
                                                           String accountProductType) {
        try {
            String transactionDate = LocalDate.now().format(PATTERN);

            log.debug("Withdrawing amount {} from disposal account {}", amount, disposalAccountAmsId);

            iso.std.iso._20022.tech.xsd.camt_056_001.Document document = jaxbUtils.unmarshalCamt056(camt056);
            Camt056ToCamt053Converter converter = new Camt056ToCamt053Converter();
            BankToCustomerStatementV08 statement = converter.convert(document, new BankToCustomerStatementV08());

            List<TransactionItem> items = new ArrayList<>();
            Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);

            String holdTransactionUrl = String.format("%s%s/transactions?command=holdAmount", incomingMoneyApi.substring(1), disposalAccountAmsId);

            Integer outHoldReasonId = paymentTypeConfig.findPaymentTypeIdByOperation(String.format("%s.%s", paymentScheme, "outHoldReasonId"));
            HoldAmountBody holdAmountBody = new HoldAmountBody(
                    transactionDate,
                    amount,
                    outHoldReasonId,
                    locale,
                    FORMAT
            );

            String bodyItem = painMapper.writeValueAsString(holdAmountBody);

            batchItemBuilder.add(tenantIdentifier, items, holdTransactionUrl, bodyItem, false);

            ReportEntry10 convertedCamt053Entry = statement.getStatement().get(0).getEntry().get(0);
            convertedCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "PENDING"));
            String hyphenatedDate = transactionDate.substring(0, 4) + "-" + transactionDate.substring(4, 6) + "-" + transactionDate.substring(6);
            convertedCamt053Entry.setValueDate(new DateAndDateTime2Choice().withAdditionalProperty("Date", hyphenatedDate));
            String camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";


            String partnerName = document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getDbtr().getNm();
            String partnerAccountIban = document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getDbtrAcct().getId().getIBAN();
            String partnerAccountSecondaryIdentifier = contactDetailsUtil.getId(document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getCdtr().getCtctDtls());
            String unstructured = Optional.ofNullable(document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getRmtInf())
                    .map(iso.std.iso._20022.tech.xsd.camt_056_001.RemittanceInformation5::getUstrd).map(List::toString).orElse("");

            String holdAmountOperation = "withdrawTheAmountFromDisposalAccountInAMS.DisposalAccount.HoldTransactionAmount";

            addDetails(tenantIdentifier, null, internalCorrelationId, transactionCategoryPurposeCode, internalCorrelationId,
                    accountProductType, batchItemBuilder, items, convertedCamt053Entry, camt053RelativeUrl, iban,
                    paymentTypeConfig, paymentScheme, holdAmountOperation, partnerName, partnerAccountIban,
                    partnerAccountSecondaryIdentifier, unstructured, disposalAccountAmsId, conversionAccountAmsId, false,
                    document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlEndToEndId());

            Long lastHoldTransactionId = holdBatch(items, tenantIdentifier, disposalAccountAmsId, conversionAccountAmsId, internalCorrelationId, "transferToConversionAccountInAms");

            HttpHeaders httpHeaders = new HttpHeaders();
            httpHeaders.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
            httpHeaders.set("Authorization", authTokenHelper.generateAuthToken());
            httpHeaders.set("Fineract-Platform-TenantId", tenantIdentifier);
            LinkedHashMap<String, Object> accountDetails = restTemplate.exchange(
                            String.format("%s/%s%s", fineractApiUrl, incomingMoneyApi.substring(1), disposalAccountAmsId),
                            HttpMethod.GET,
                            new HttpEntity<>(httpHeaders),
                            LinkedHashMap.class)
                    .getBody();
            LinkedHashMap<String, Object> summary = (LinkedHashMap<String, Object>) accountDetails.get("summary");
            BigDecimal availableBalance = new BigDecimal(summary.get("availableBalance").toString());
            if (availableBalance.signum() < 0) {
                restTemplate.exchange(
                        String.format("%s/%ssavingsaccounts/%s/transactions/%d?command=releaseAmount", fineractApiUrl, incomingMoneyApi.substring(1), disposalAccountAmsId, lastHoldTransactionId),
                        HttpMethod.POST,
                        new HttpEntity<>(httpHeaders),
                        Object.class
                );
                throw new ZeebeBpmnError("Error_InsufficientFunds", "Insufficient funds");
            }

            items.clear();

            String releaseTransactionUrl = String.format("%s%s/transactions/%d?command=releaseAmount", incomingMoneyApi.substring(1), disposalAccountAmsId, lastHoldTransactionId);
            batchItemBuilder.add(tenantIdentifier, items, releaseTransactionUrl, null, false);
            String releaseAmountOperation = "withdrawTheAmountFromDisposalAccountInAMS.DisposalAccount.ReleaseTransactionAmount";
            addDetails(tenantIdentifier, null, internalCorrelationId, transactionCategoryPurposeCode, internalCorrelationId,
                    accountProductType, batchItemBuilder, items, convertedCamt053Entry, camt053RelativeUrl, iban,
                    paymentTypeConfig, paymentScheme, releaseAmountOperation, partnerName, partnerAccountIban,
                    partnerAccountSecondaryIdentifier, unstructured, disposalAccountAmsId, conversionAccountAmsId, false,
                    document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlEndToEndId());

            String disposalAccountWithdrawRelativeUrl = String.format("%s%s/transactions?command=%s", incomingMoneyApi.substring(1), disposalAccountAmsId, "withdrawal");

            String withdrawAmountOperation = "withdrawTheAmountFromDisposalAccountInAMS.DisposalAccount.WithdrawTransactionAmount";
            String configOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
            Integer paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(configOperationKey);
            String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(configOperationKey);

            TransactionBody transactionBody = new TransactionBody(
                    transactionDate,
                    amount,
                    paymentTypeId,
                    "",
                    FORMAT,
                    locale);

            bodyItem = painMapper.writeValueAsString(transactionBody);

            batchItemBuilder.add(tenantIdentifier, items, disposalAccountWithdrawRelativeUrl, bodyItem, false);

            convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
            convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "PENDING"));

            String camt053 = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);

            camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";

            DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
                    internalCorrelationId,
                    camt053,
                    document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getCdtrAcct().getId().getIBAN(),
                    paymentTypeCode,
                    internalCorrelationId,
                    document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getDbtr().getNm(),
                    document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getDbtrAcct().getId().getIBAN(),
                    null,
                    contactDetailsUtil.getId(document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getDbtr().getCtctDtls()),
                    Optional.ofNullable(document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getRmtInf())
                            .map(iso.std.iso._20022.tech.xsd.camt_056_001.RemittanceInformation5::getUstrd).map(List::toString).orElse(""),
                    transactionCategoryPurposeCode,
                    paymentScheme,
                    disposalAccountAmsId,
                    conversionAccountAmsId,
                    document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlEndToEndId());

            String camt053Body = painMapper.writeValueAsString(td);

            batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);

            String conversionAccountDepositRelativeUrl = String.format("%s%s/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "deposit");

            String depositAmountOperation = "withdrawTheAmountFromDisposalAccountInAMS.ConversionAccount.DepositTransactionAmount";
            addExchange(tenantIdentifier, amount, paymentScheme, transactionDate, painMapper, paymentTypeConfig, batchItemBuilder, items, conversionAccountDepositRelativeUrl, depositAmountOperation);
            configOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
            paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(configOperationKey);
            convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
            convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.CRDT);
            convertedCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "PENDING"));
            camt053 = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);

            td = new DtSavingsTransactionDetails(
                    internalCorrelationId,
                    camt053,
                    document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getCdtrAcct().getId().getIBAN(),
                    paymentTypeCode,
                    internalCorrelationId,
                    document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getDbtr().getNm(),
                    document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getDbtrAcct().getId().getIBAN(),
                    null,
                    contactDetailsUtil.getId(document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getDbtr().getCtctDtls()),
                    Optional.ofNullable(document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlTxRef().getRmtInf())
                            .map(iso.std.iso._20022.tech.xsd.camt_056_001.RemittanceInformation5::getUstrd).map(List::toString).orElse(""),
                    transactionCategoryPurposeCode,
                    paymentScheme,
                    disposalAccountAmsId,
                    conversionAccountAmsId,
                    document.getFIToFIPmtCxlReq().getUndrlyg().get(0).getTxInf().get(0).getOrgnlEndToEndId());

            camt053Body = painMapper.writeValueAsString(td);
            batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);

            doBatch(items,
                    tenantIdentifier,
                    disposalAccountAmsId,
                    conversionAccountAmsId,
                    internalCorrelationId,
                    "withdrawTheAmountFromDisposalAccountInAMS");
        } catch (JAXBException | JsonProcessingException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }

        return null;
    }
}