package org.mifos.connector.ams.zeebe.workers;

import com.baasflow.commons.events.EventService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.fineract.client.models.BatchResponse;
import org.jetbrains.annotations.NotNull;
import org.mifos.connector.ams.fineract.TenantConfigs;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.log.LogInternalCorrelationId;
import org.mifos.connector.ams.log.TraceZeebeArguments;
import org.mifos.connector.ams.zeebe.workers.bookamount.MoneyInOutWorker;
import org.mifos.connector.ams.zeebe.workers.utils.BatchItem;
import org.mifos.connector.ams.zeebe.workers.utils.BatchItemBuilder;
import org.mifos.connector.ams.zeebe.workers.utils.CurrentAccountTransactionBody;
import org.mifos.connector.ams.zeebe.workers.utils.ExternalHoldItem;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.mifos.connector.ams.zeebe.workers.bookamount.MoneyInOutWorker.DATETIME_FORMAT;

@Component
public class CardWorkers {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${fineract.current-account-api}")
    String currentAccountApi;

    @Value("${fineract.locale}")
    String locale;

    @Autowired
    EventService eventService;

    @Autowired
    BatchItemBuilder batchItemBuilder;

    @Autowired
    MoneyInOutWorker moneyInOutWorker;

    @Autowired
    TenantConfigs tenantConfigs;

    @Autowired
    @Qualifier("painMapper")
    ObjectMapper painMapper;

    @Data
    @Accessors(chain = true)
    static class WithdrawWithHoldResponse {
        BigDecimal availableBalance;
        BigDecimal holdAmount;
        String holdIdentifier;

        public boolean isWithdraw() {
            return holdIdentifier == null;
        }
    }


    @JobWorker
    @LogInternalCorrelationId
    @TraceZeebeArguments
    public Map<String, Object> bookCardTransactionOnConversionAccountInAms(
            JobClient jobClient,
            ActivatedJob activatedJob,
            @Variable BigDecimal amount,
            @Variable BigDecimal transactionFeeAmount,
            @Variable String cardAccountId,
            @Variable String cardTransactionType,
            @Variable String cardFeeTransactionType,
            @Variable String cardHolderName,
            @Variable String cardToken,
            @Variable String conversionAccountAmsId,
            @Variable String currency,
            @Variable String direction,
            @Variable String disposalAccountAmsId,
            @Variable String holdFeeIdentifier,
            @Variable String holdIdentifier,
            @Variable String instructedAmount,
            @Variable String instructedCurrency,
            @Variable String internalCorrelationId,
            @Variable String isEcommerce,
            @Variable String maskedPan,
            @Variable String merchName,
            @Variable String merchantCategoryCode,
            @Variable String messageId,
            @Variable String partnerCity,
            @Variable String partnerCountry,
            @Variable String paymentScheme,
            @Variable String paymentTokenWallet,
            @Variable String processCode,
            @Variable String requestId,
            @Variable String sequenceDateTime,
            @Variable String sequenceDateTimeFormat,
            @Variable String tenantIdentifier,
            @Variable String transactionCategoryPurpose,
            @Variable String transactionFeeCategoryPurpose,
            @Variable String transactionFeeInternalCorrelationId,
            @Variable String transactionGroupId,
            @Variable String transactionReference

    ) {
        return eventService.auditedEvent(
                event -> EventLogUtil.initZeebeJob(activatedJob, "bookCardTransactionOnConversionAccountInAms", internalCorrelationId, transactionGroupId, event),
                event -> {
                    MDC.put("internalCorrelationId", internalCorrelationId);
                    String apiPath = currentAccountApi.substring(1);
                    String withdrawalUrl = String.format("%s%s/transactions?command=withdrawal&force-type=hold", apiPath, disposalAccountAmsId);
                    String paymentTypeConversionWithdrawFee = tenantConfigs.findPaymentTypeId(tenantIdentifier, "%s:%s.conversion.withdraw".formatted(paymentScheme, cardFeeTransactionType));
                    String paymentTypeConversionWithdrawAmount = tenantConfigs.findPaymentTypeId(tenantIdentifier, "%s:%s.conversion.withdraw".formatted(paymentScheme, cardTransactionType));

                    String dateTimeFormat = sequenceDateTimeFormat != null ? sequenceDateTimeFormat : detectDateTimeFormat(sequenceDateTime);
                    CurrentAccountTransactionBody cardTransactionBody = new CurrentAccountTransactionBody()
                            .setTransactionAmount(transactionFeeAmount)
                            .setDateTimeFormat(dateTimeFormat)
                            .setLocale(locale)
                            .setCurrencyCode(currency)
                            .setDatatables(List.of(
                                            new CurrentAccountTransactionBody.DataTable(List.of(
                                                    new CurrentAccountTransactionBody.Entry()
                                                            .setAccount_iban("TODO")
                                                            .setStructured_transaction_details("{}")
                                                            .setInternal_correlation_id(internalCorrelationId)
                                                            .setEnd_to_end_id("TODO")
                                                            .setTransaction_id(transactionReference)
                                                            .setTransaction_group_id(transactionGroupId)
                                                            .setPayment_scheme(paymentScheme)
                                                            .setDirection(direction)
                                                            .setPartner_name(merchName)
                                                            .setPartner_account_iban("")
                                            ), "dt_current_transaction_details"),
                                            new CurrentAccountTransactionBody.DataTable(List.of(
                                                    new CurrentAccountTransactionBody.CardEntry()
                                                            .setInstruction_identification(requestId)
                                                            .setMessage_id(messageId)
                                                            .setCard_token(cardToken)
                                                            .setMasked_pan(maskedPan)
                                                            .setCard_holder_name(cardHolderName)
                                                            .setPartner_city(partnerCity)
                                                            .setPartner_country(partnerCountry)
                                                            .setInstructed_amount(instructedAmount)
                                                            .setInstructed_currency(instructedCurrency)
                                                            .setProcess_code(processCode)
                                                            .setMerchant_category_code(merchantCategoryCode)
                                                            .setIs_ecommerce(isEcommerce)
                                                            .setPayment_token_wallet(paymentTokenWallet)
                                            ), "dt_current_card_transaction_details")
                                    )
                            );

                    try {
                        if (holdFeeIdentifier != null) {
                            logger.info("Hold fee detected with id {}, skipping fee withdrawal", holdFeeIdentifier);
                        } else {
                            // STEP 1 - withdraw fee from conversion, execute
                            logger.info("Withdrawing fee {} from conversion account {}", transactionFeeAmount, conversionAccountAmsId);
                            cardTransactionBody.setPaymentTypeId(paymentTypeConversionWithdrawFee);
                            executeWithdrawNoHold("FEE-K", withdrawalUrl, cardTransactionBody, conversionAccountAmsId, disposalAccountAmsId, tenantIdentifier, requestId, transactionGroupId, internalCorrelationId);
                        }

                        if (holdIdentifier != null) {
                            logger.info("Hold detected with id {}, skipping card amount withdrawal", holdIdentifier);
                        } else {
                            // STEP 2 - withdraw card amount from disposal account
                            logger.info("Withdrawing amount {} from disposal account {}", amount, disposalAccountAmsId);
                            cardTransactionBody.setPaymentTypeId(paymentTypeConversionWithdrawAmount);
                            executeWithdrawNoHold("TRX-K", withdrawalUrl, cardTransactionBody, conversionAccountAmsId, disposalAccountAmsId, tenantIdentifier, requestId, transactionGroupId, internalCorrelationId);
                        }
                        return Map.of();
                    } catch (Exception e) {
                        logger.error("## Exception in bookCardTransactionOnConversionAccountInAms ##", e);
                        throw e;
                    } finally {
                        MDC.remove("internalCorrelationId");
                    }
                }
        );
    }


    @JobWorker
    @LogInternalCorrelationId
    @TraceZeebeArguments
    public Map<String, Object> transferToConversionAccountAndUpdateEHoldInAms(
            JobClient jobClient,
            ActivatedJob activatedJob,
            @Variable BigDecimal amount,
            @Variable BigDecimal externalHoldAmount,
            @Variable BigDecimal transactionFeeAmount,
            @Variable String cardAccountId,
            @Variable String cardHolderName,
            @Variable String cardToken,
            @Variable String cardTransactionType,
            @Variable String cardFeeTransactionType,
            @Variable String conversionAccountAmsId,
            @Variable String currency,
            @Variable String direction,
            @Variable String disposalAccountAmsId,
            @Variable String instructedAmount,
            @Variable String instructedCurrency,
            @Variable String internalCorrelationId,
            @Variable String isEcommerce,
            @Variable String maskedPan,
            @Variable String merchName,
            @Variable String merchantCategoryCode,
            @Variable String messageId,
            @Variable String partnerCity,
            @Variable String partnerCountry,
            @Variable String paymentScheme, // "CARD_CLEARING"
            @Variable String paymentTokenWallet,
            @Variable String processCode,
            @Variable String requestId,
            @Variable String sequenceDateTime,
            @Variable String sequenceDateTimeFormat,
            @Variable String tenantIdentifier,
            @Variable String transactionGroupId,
            @Variable String transactionReference
    ) {
        return eventService.auditedEvent(
                event -> EventLogUtil.initZeebeJob(activatedJob, "transferToConversionAccountAndUpdateEHoldInAmsWorker", internalCorrelationId, transactionGroupId, event),
                event -> {
                    MDC.put("internalCorrelationId", internalCorrelationId);
                    try {
                        // STEP 0 - prepare data
                        String apiPath = currentAccountApi.substring(1);
                        String holdUrl = String.format("%s%s/transactions?command=external-hold", apiPath, disposalAccountAmsId);
                        String withdrawalUrl = String.format("%s%s/transactions?command=withdrawal&force-type=hold", apiPath, disposalAccountAmsId);
                        String depositUrl = String.format("%s%s/transactions?command=deposit", apiPath, conversionAccountAmsId);

                        String paymentTypeDisposalWithdrawFee = tenantConfigs.findPaymentTypeId(tenantIdentifier, "%s:%s.disposal.withdraw".formatted(paymentScheme, cardFeeTransactionType));
                        String paymentTypeDisposalWithdrawAmount = tenantConfigs.findPaymentTypeId(tenantIdentifier, "%s:%s.disposal.withdraw".formatted(paymentScheme, cardTransactionType));
                        String paymentTypeConversionDepositFee = tenantConfigs.findPaymentTypeId(tenantIdentifier, "%s:%s.conversion.deposit".formatted(paymentScheme, cardFeeTransactionType));
                        String paymentTypeConversionDepositAmount = tenantConfigs.findPaymentTypeId(tenantIdentifier, "%s:%s.conversion.deposit".formatted(paymentScheme, cardTransactionType));
                        String dateTimeFormat = sequenceDateTimeFormat != null ? sequenceDateTimeFormat : detectDateTimeFormat(sequenceDateTime);

                        BigDecimal originalAmount = externalHoldAmount.subtract(transactionFeeAmount).max(BigDecimal.ZERO);
                        CurrentAccountTransactionBody holdBody = new CurrentAccountTransactionBody()
                                .setTransactionAmount(BigDecimal.ZERO)
                                .setSequenceDateTime(sequenceDateTime)
                                .setOriginalAmount(originalAmount)
                                .setLocale(locale)
                                .setDateTimeFormat(DATETIME_FORMAT)
                                .setDatatables(List.of(
                                                new CurrentAccountTransactionBody.DataTable(List.of(
                                                        new CurrentAccountTransactionBody.HoldEntry()
                                                                .setEnd_to_end_id("TODO")
                                                                .setTransaction_id(transactionReference)
                                                                .setInternal_correlation_id(internalCorrelationId)
                                                                .setPartner_name(merchName)
                                                                .setPayment_scheme(paymentScheme)
                                                                .setPartner_account_iban("TODO")
                                                                .setDirection(direction)
                                                                .setAccount_iban("TODO")
                                                ), "dt_current_transaction_details")
                                        )
                                );

                        CurrentAccountTransactionBody cardTransactionBody = new CurrentAccountTransactionBody()
                                .setTransactionAmount(transactionFeeAmount)
                                .setDateTimeFormat(dateTimeFormat)
                                .setLocale(locale)
                                .setCurrencyCode(currency)
                                .setDatatables(List.of(
                                                new CurrentAccountTransactionBody.DataTable(List.of(
                                                        new CurrentAccountTransactionBody.Entry()
                                                                .setAccount_iban("TODO")
                                                                .setStructured_transaction_details("{}")
                                                                .setInternal_correlation_id(internalCorrelationId)
                                                                .setEnd_to_end_id("TODO")
                                                                .setTransaction_id(transactionReference)
                                                                .setTransaction_group_id(transactionGroupId)
                                                                .setPayment_scheme(paymentScheme)
                                                                .setDirection(direction)
                                                                .setPartner_name(merchName)
                                                                .setPartner_account_iban("")
                                                ), "dt_current_transaction_details"),
                                                new CurrentAccountTransactionBody.DataTable(List.of(
                                                        new CurrentAccountTransactionBody.CardEntry()
                                                                .setInstruction_identification(requestId)
                                                                .setMessage_id(messageId)
                                                                .setCard_token(cardToken)
                                                                .setMasked_pan(maskedPan)
                                                                .setCard_holder_name(cardHolderName)
                                                                .setPartner_city(partnerCity)
                                                                .setPartner_country(partnerCountry)
                                                                .setInstructed_amount(instructedAmount)
                                                                .setInstructed_currency(instructedCurrency)
                                                                .setProcess_code(processCode)
                                                                .setMerchant_category_code(merchantCategoryCode)
                                                                .setIs_ecommerce(isEcommerce)
                                                                .setPayment_token_wallet(paymentTokenWallet)
                                                ), "dt_current_card_transaction_details")
                                        )
                                );

                        String holdFeeIdentifier = null;
                        String holdIdentifier = null;
                        if (transactionFeeAmount.equals(BigDecimal.ZERO)) {
                            logger.info("Transaction fee is zero, skipping fee handling");
                        } else {
                            // STEP 1 - withdraw fee from disposal, execute
                            logger.info("Withdraw fee {} from disposal account {}", transactionFeeAmount, disposalAccountAmsId);
                            cardTransactionBody.setPaymentTypeId(paymentTypeDisposalWithdrawFee);
                            WithdrawWithHoldResponse holdResponse = executeWithdrawWithHold("FEE-R", holdUrl, holdBody, withdrawalUrl, cardTransactionBody, conversionAccountAmsId, disposalAccountAmsId, tenantIdentifier, requestId, transactionGroupId, internalCorrelationId);

                            if (holdResponse.isWithdraw()) {
                                // STEP 2 - deposit fee to conversion, execute
                                logger.info("Deposit fee {} to conversion account {}", transactionFeeAmount, conversionAccountAmsId);
                                cardTransactionBody.setPaymentTypeId(paymentTypeConversionDepositFee);
                                executeDeposit(cardTransactionBody, conversionAccountAmsId, disposalAccountAmsId, internalCorrelationId, tenantIdentifier, transactionGroupId, depositUrl);
                            } else {
                                holdFeeIdentifier = holdResponse.getHoldIdentifier();
                                logger.info("Insufficient balance at disposal {} for fee {}, no deposit made to conversion", disposalAccountAmsId, transactionFeeAmount);
                            }
                        }

                        // STEP 3 - withdraw amount from disposal, execute
                        originalAmount = originalAmount.subtract(amount).max(BigDecimal.ZERO);
                        holdBody.setTransactionAmount(BigDecimal.ZERO);
                        holdBody.setOriginalAmount(originalAmount);
                        holdBody.setSequenceDateTime(increase(holdBody.getSequenceDateTime(), dateTimeFormat));
                        cardTransactionBody.setTransactionAmount(amount);
                        cardTransactionBody.setOriginalAmount(null);

                        logger.info("Withdraw amount {} from disposal account {}", amount, disposalAccountAmsId);
                        cardTransactionBody.setPaymentTypeId(paymentTypeDisposalWithdrawAmount);
                        WithdrawWithHoldResponse balanceAndWithdraw = executeWithdrawWithHold("TRX-R", holdUrl, holdBody, withdrawalUrl, cardTransactionBody, conversionAccountAmsId, disposalAccountAmsId, tenantIdentifier, requestId, transactionGroupId, internalCorrelationId);

                        if (balanceAndWithdraw.isWithdraw()) {
                            // STEP 4 - deposit amount to conversion, execute
                            logger.info("Deposit amount {} to conversion account {}", amount, conversionAccountAmsId);
                            cardTransactionBody.setPaymentTypeId(paymentTypeConversionDepositAmount);
                            executeDeposit(cardTransactionBody, conversionAccountAmsId, disposalAccountAmsId, internalCorrelationId, tenantIdentifier, transactionGroupId, depositUrl);
                        } else {
                            holdIdentifier = balanceAndWithdraw.getHoldIdentifier();
                            logger.info("Insufficient balance at disposal {} for amount {}, no deposit made to conversion", disposalAccountAmsId, amount);
                        }

                        Map<String, Object> results = new HashMap<>();
                        results.put("availableBalance", balanceAndWithdraw.getAvailableBalance());
                        if (holdFeeIdentifier != null) {
                            results.put("holdFeeIdentifier", holdFeeIdentifier);
                        }
                        if (holdIdentifier != null) {
                            results.put("holdIdentifier", holdIdentifier);
                        }
                        return results;

                    } catch (Exception e) {
                        logger.error("## Exception in transferToConversionAccountAndUpdateEHoldInAms ##", e);
                        throw e;
                    } finally {
                        MDC.remove("internalCorrelationId");
                    }
                }
        );
    }

    private @NotNull Pair<BigDecimal, Boolean> executeWithdrawNoHold(String idempotencyPostfix, String withdrawalUrl, CurrentAccountTransactionBody cardTransactionBody, String conversionAccountAmsId, String disposalAccountAmsId, String tenantIdentifier, String requestId, String transactionGroupId, String internalCorrelationId) {
        try {
            String cardTransactionBodyString = painMapper.writeValueAsString(cardTransactionBody);
            String withdrawIdempotencyKey = requestId + "_" + idempotencyPostfix;
            logger.debug("{} card transaction body: {}", idempotencyPostfix, cardTransactionBodyString);
            List<BatchItem> items = List.of(
                    new TransactionItem(1, withdrawalUrl, "POST", null, batchItemBuilder.createHeaders(tenantIdentifier, withdrawIdempotencyKey), cardTransactionBodyString)
            );

            Pair<String, List<BatchResponse>> out = moneyInOutWorker.doBatch(items, tenantIdentifier, transactionGroupId, disposalAccountAmsId, conversionAccountAmsId, internalCorrelationId, "transferToConversionAccountAndUpdateEHoldInAmsWorker");

            BatchResponse response = out.getRight().get(0);
            DocumentContext json = JsonPath.parse(response.getBody());
            BigDecimal availableBalance = json.read("$.changes.availableBalance", BigDecimal.class);
            logger.info("availableBalance: {} from json response: {}", availableBalance, response.getBody());

            return Pair.of(availableBalance, true); // withdraw did happen
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private @NotNull WithdrawWithHoldResponse executeWithdrawWithHold(String idempotencyPostfix, String holdUrl, CurrentAccountTransactionBody holdBody, String withdrawalUrl, CurrentAccountTransactionBody cardTransactionBody, String conversionAccountAmsId, String disposalAccountAmsId, String tenantIdentifier, String requestId, String transactionGroupId, String internalCorrelationId) {
        try {
            String holdBodyString = painMapper.writeValueAsString(holdBody);
            String holdIdempotencyKey = requestId + "_EH-" + idempotencyPostfix;
            logger.debug("{} hold body: {}", idempotencyPostfix, holdBodyString);

            String cardTransactionBodyString = painMapper.writeValueAsString(cardTransactionBody);
            String withdrawIdempotencyKey = requestId + "_" + idempotencyPostfix;
            logger.debug("{} card transaction body: {}", idempotencyPostfix, cardTransactionBodyString);

            List<BatchItem> items = List.of(
                    new ExternalHoldItem()
                            .setRelativeUrl(holdUrl)
                            .setRequestId(1)
                            .setMethod("POST")
                            .setHeaders(batchItemBuilder.createHeaders(tenantIdentifier, holdIdempotencyKey))
                            .setBody(holdBodyString),
                    new TransactionItem(2, withdrawalUrl, "POST", null, batchItemBuilder.createHeaders(tenantIdentifier, withdrawIdempotencyKey), cardTransactionBodyString)
            );
            Pair<String, List<BatchResponse>> out = moneyInOutWorker.doBatch(items, tenantIdentifier, transactionGroupId, disposalAccountAmsId, conversionAccountAmsId, internalCorrelationId, "transferToConversionAccountAndUpdateEHoldInAmsWorker");

            BatchResponse response = out.getRight().get(0);
            DocumentContext json = JsonPath.parse(response.getBody());
            BigDecimal availableBalance = json.read("$.changes.availableBalance", BigDecimal.class);
            logger.info("availableBalance: {} from json response: {}", availableBalance, response.getBody());

            try {
                BigDecimal holdAmount = json.read("$.changes.appliedAmounts.holdAmount", BigDecimal.class);
                String holdIdentifier = json.read("$.resourceIdentifier", String.class);
                return new WithdrawWithHoldResponse()
                        .setAvailableBalance(availableBalance)
                        .setHoldAmount(holdAmount)
                        .setHoldIdentifier(holdIdentifier);
            } catch (PathNotFoundException e) {
                return new WithdrawWithHoldResponse()
                        .setAvailableBalance(availableBalance);  // withdraw did happen
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void executeDeposit(CurrentAccountTransactionBody cardTransactionBody, String conversionAccountAmsId, String disposalAccountAmsId, String internalCorrelationId, String tenantIdentifier, String transactionGroupId, String depositUrl) {
        try {
            String idempotencyKey = UUID.randomUUID().toString();
            String cardTransactionBodyString = painMapper.writeValueAsString(cardTransactionBody);
            logger.debug("amount card transaction body: {}", cardTransactionBodyString);

            List<BatchItem> items = List.of(new TransactionItem(1, depositUrl, "POST", null, batchItemBuilder.createHeaders(tenantIdentifier, idempotencyKey), cardTransactionBodyString));
            Pair<String, List<BatchResponse>> response = moneyInOutWorker.doBatch(items, tenantIdentifier, transactionGroupId, disposalAccountAmsId, conversionAccountAmsId, internalCorrelationId, "transferToConversionAccountAndUpdateEHoldInAmsWorker");


        } catch (ZeebeBpmnError z) {
            throw z;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    String increase(String dateTime, String dateTimeFormat) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dateTimeFormat);
        OffsetDateTime localDateTime = LocalDateTime.parse(dateTime, formatter).atOffset(ZoneOffset.UTC);
        return localDateTime.plus(1, ChronoUnit.MILLIS).format(formatter);
    }

    final List<String> possibleFormats = List.of(
            "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            "yyyy-MM-dd'T'HH:mm:ss.SSS"
    );

    String detectDateTimeFormat(String dateTime) {
        for (String format : possibleFormats) {
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
                LocalDate.parse(dateTime, formatter);
                logger.debug("Detected date time format: {}", format);
                return format;
            } catch (DateTimeParseException e) {
                // Ignore and try the next format
            }
        }
        throw new RuntimeException("Could not detect date time format for: " + dateTime);
    }
}