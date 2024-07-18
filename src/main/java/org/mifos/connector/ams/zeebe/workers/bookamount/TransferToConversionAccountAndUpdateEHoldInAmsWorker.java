package org.mifos.connector.ams.zeebe.workers.bookamount;

import com.baasflow.commons.events.EventService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.fineract.client.models.BatchResponse;
import org.jetbrains.annotations.NotNull;
import org.mifos.connector.ams.fineract.TenantConfigs;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.log.LogInternalCorrelationId;
import org.mifos.connector.ams.log.TraceZeebeArguments;
import org.mifos.connector.ams.zeebe.workers.utils.BatchItem;
import org.mifos.connector.ams.zeebe.workers.utils.BatchItemBuilder;
import org.mifos.connector.ams.zeebe.workers.utils.CurrentAccountTransactionBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;

import static org.mifos.connector.ams.zeebe.workers.bookamount.MoneyInOutWorker.DATETIME_FORMAT;

@Component
public class TransferToConversionAccountAndUpdateEHoldInAmsWorker {
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

    final String caller = "transferToConversionAccountAndUpdateEHoldInAms";


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
            @Variable String paymentScheme,
            @Variable String paymentTokenWallet,
            @Variable String processCode,
            @Variable String requestId,
            @Variable String sequenceDateTime,
            @Variable String tenantIdentifier,
            @Variable String transactionGroupId,
            @Variable String transactionReference
    ) {
        return eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "transferToConversionAccountAndUpdateEHoldInAmsWorker", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> {
                    MDC.put("internalCorrelationId", internalCorrelationId);
                    try {
                        // STEP 0 - prepare data
                        String apiPath = currentAccountApi.substring(1);
                        String holdUrl = String.format("%s%s/transactions?command=external-hold", apiPath, disposalAccountAmsId);
                        String withdrawalUrl = String.format("%s%s/transactions?command=withdrawal", apiPath, disposalAccountAmsId);  // &force-type=hold
                        String depositUrl = String.format("%s%s/transactions?command=deposit", apiPath, conversionAccountAmsId);
                        String depositFeeOperation = "transferToConversionAccountInAms.ConversionAccount.DepositTransactionFee"; // TODO use card types
                        String depositFeePaymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, String.format("%s.%s", paymentScheme, depositFeeOperation));
                        String withdrawFeeOperation = "transferToConversionAccountInAms.ConversionAccount.DepositTransactionFee"; // TODO use card types
                        String withdrawFeePaymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, String.format("%s.%s", paymentScheme, withdrawFeeOperation));

                        CurrentAccountTransactionBody holdBody = new CurrentAccountTransactionBody()
                                .setTransactionAmount(transactionFeeAmount)
                                .setSequenceDateTime(sequenceDateTime)
                                .setOriginalAmount(externalHoldAmount)
                                .setLocale(locale)
                                .setDateTimeFormat(DATETIME_FORMAT)
                                .setPaymentTypeId(depositFeePaymentTypeId)
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
                                .setDateTimeFormat(detectDateTimeFormat(sequenceDateTime))
                                .setLocale(locale)
                                .setPaymentTypeId(depositFeePaymentTypeId)
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

                        logger.debug("fee card transaction body: {}", cardTransactionBody);
                        logger.debug("fee hold body: {}", holdBody);

                        if (transactionFeeAmount.equals(BigDecimal.ZERO)) {
                            logger.info("Transaction fee is zero, skipping fee handling");
                        } else {
                            // STEP 1 - withdraw fee from disposal, execute
                            logger.info("Withdraw fee {} from disposal account {}", transactionFeeAmount, disposalAccountAmsId);
                            Pair<BigDecimal, Boolean> balanceAndWithdraw = executeWithdraw(holdBody, cardTransactionBody, conversionAccountAmsId, disposalAccountAmsId, internalCorrelationId, tenantIdentifier, transactionGroupId, holdUrl, withdrawalUrl);

                            if (balanceAndWithdraw.getRight()) {
                                // STEP 2 - deposit fee to conversion, execute
                                logger.info("Deposit fee {} to conversion account {}", transactionFeeAmount, conversionAccountAmsId);
                                executeDeposit(cardTransactionBody, conversionAccountAmsId, disposalAccountAmsId, internalCorrelationId, tenantIdentifier, transactionGroupId, depositUrl);
                            } else {
                                logger.info("Insufficient balance at disposal {} for fee {}, no deposit made to conversion", disposalAccountAmsId, transactionFeeAmount);
                            }
                        }

                        // STEP 3 - withdraw amount from disposal, execute
                        BigDecimal originalAmount = externalHoldAmount.subtract(transactionFeeAmount); // TODO review this
                        holdBody.setTransactionAmount(amount);
                        holdBody.setOriginalAmount(originalAmount);
                        cardTransactionBody.setTransactionAmount(amount);
                        cardTransactionBody.setOriginalAmount(originalAmount);
                        logger.debug("amount card transaction body: {}", cardTransactionBody);
                        logger.debug("amount hold body: {}", holdBody);

                        logger.info("Withdraw amount {} from disposal account {}", amount, disposalAccountAmsId);
                        Pair<BigDecimal, Boolean> balanceAndWithdraw = executeWithdraw(holdBody, cardTransactionBody, conversionAccountAmsId, disposalAccountAmsId, internalCorrelationId, tenantIdentifier, transactionGroupId, holdUrl, withdrawalUrl);

                        if (balanceAndWithdraw.getRight()) {
                            // STEP 4 - deposit amount to conversion, execute
                            logger.info("Deposit amount {} to conversion account {}", amount, conversionAccountAmsId);
                            executeDeposit(cardTransactionBody, conversionAccountAmsId, disposalAccountAmsId, internalCorrelationId, tenantIdentifier, transactionGroupId, depositUrl);
                        } else {
                            logger.info("Insufficient balance at disposal {} for amount {}, no deposit made to conversion", disposalAccountAmsId, amount);
                        }

                        return Map.of("availableBalance", balanceAndWithdraw.getLeft());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        MDC.remove("internalCorrelationId");
                    }
                }
        );
    }

    private @NotNull Pair<BigDecimal, Boolean> executeWithdraw(CurrentAccountTransactionBody holdBody, CurrentAccountTransactionBody cardTransactionBody, String conversionAccountAmsId, String disposalAccountAmsId, String internalCorrelationId, String tenantIdentifier, String transactionGroupId, String holdUrl, String withdrawalUrl) {
        try {
            String holdBodyString = painMapper.writeValueAsString(holdBody);
            String cardTransactionBodyString = painMapper.writeValueAsString(cardTransactionBody);

            List<BatchItem> items = List.of(
                    batchItemBuilder.createExternalHoldItem(1, caller, internalCorrelationId, holdUrl, tenantIdentifier, holdBodyString),
                    batchItemBuilder.createTransactionItem(2, caller, internalCorrelationId, withdrawalUrl, tenantIdentifier, cardTransactionBodyString, null)
            );

            // execute batch
            Pair<String, List<BatchResponse>> out = moneyInOutWorker.doBatch(items, tenantIdentifier, transactionGroupId, disposalAccountAmsId, conversionAccountAmsId, internalCorrelationId, "transferToConversionAccountAndUpdateEHoldInAmsWorker");
            BatchResponse response = out.getRight().get(0);
            DocumentContext json = JsonPath.parse(response.getBody());
            logger.debug("## json response: {}", json);
            BigDecimal holdAmount = json.read("$.changes.appliedAmounts.holdAmount", BigDecimal.class);
            BigDecimal availableBalance = json.read("$.changes.availableBalance", BigDecimal.class);
            logger.info("availableBalance: {} and holdAmount: {} from json response: {}", availableBalance, holdAmount, response.getBody());
            return Pair.of(availableBalance, holdAmount.equals(BigDecimal.ZERO));

        } catch (ZeebeBpmnError z) {
            throw z;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private @NotNull BigDecimal executeDeposit(CurrentAccountTransactionBody cardTransactionBody, String conversionAccountAmsId, String disposalAccountAmsId, String internalCorrelationId, String tenantIdentifier, String transactionGroupId, String depositUrl) {
        try {
            String cardTransactionBodyString = painMapper.writeValueAsString(cardTransactionBody);
            List<BatchItem> items = List.of(batchItemBuilder.createTransactionItem(1, caller, internalCorrelationId, depositUrl, tenantIdentifier, cardTransactionBodyString, null));

            // execute batch
            Pair<String, List<BatchResponse>> out = moneyInOutWorker.doBatch(items, tenantIdentifier, transactionGroupId, disposalAccountAmsId, conversionAccountAmsId, internalCorrelationId, "transferToConversionAccountAndUpdateEHoldInAmsWorker");
            BatchResponse response = out.getRight().get(0);
            DocumentContext json = JsonPath.parse(response.getBody());
            BigDecimal availableBalance = json.read("$.changes.availableBalance", BigDecimal.class);
            logger.info("returning availableBalance: {} from json response: {}", availableBalance, response.getBody());
            return availableBalance;

        } catch (ZeebeBpmnError z) {
            throw z;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private final List<String> possibleFormats = List.of(
            "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
            "yyyy-MM-dd'T'HH:mm:ss.SSSX",
            "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            "yyyy-MM-dd'T'HH:mm:ss.SSS"
    );

    private String detectDateTimeFormat(String dateTime) {
        for (String format : possibleFormats) {
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
                LocalDateTime.parse(dateTime, formatter);
                logger.debug("Detected date time format: {}", format);
                return format;
            } catch (DateTimeParseException e) {
                // Ignore and try the next format
            }
        }
        throw new RuntimeException("Could not detect date time format for: " + dateTime);
    }
}
