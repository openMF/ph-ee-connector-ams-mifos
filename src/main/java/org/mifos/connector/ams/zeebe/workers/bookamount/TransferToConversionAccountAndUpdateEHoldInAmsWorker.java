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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mifos.connector.ams.zeebe.workers.bookamount.MoneyInOutWorker.FORMAT;

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
    private TenantConfigs tenantConfigs;

    @Autowired
    @Qualifier("painMapper")
    private ObjectMapper painMapper;


    @JobWorker
    @LogInternalCorrelationId
    @TraceZeebeArguments
    public Map<String, Object> transferToConversionAccountAndUpdateEHoldInAms(
            JobClient jobClient,
            ActivatedJob activatedJob,
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
                    List<BatchItem> items = new ArrayList<>();

                    String apiPath = currentAccountApi.substring(1);
                    String holdUrl = String.format("%s%s/transactions?command=external-hold", apiPath, disposalAccountAmsId);
                    String withdrawalUrl = String.format("%s%s/transactions?command=withdrawal", apiPath, disposalAccountAmsId);  // &force-type=hold
                    String depositUrl = String.format("%s%s/transactions?command=deposit", apiPath, conversionAccountAmsId);

                    try {
                        // STEP 1 - add fee
                        if (transactionFeeAmount.equals(BigDecimal.ZERO)) {
                            logger.info("Transaction fee is zero, skipping fee handling");
                        } else {
                            logger.debug("Depositing fee {} to conversion account {}", transactionFeeAmount, conversionAccountAmsId);
                            String depositFeeOperation = "transferToConversionAccountInAms.ConversionAccount.DepositTransactionFee"; // TODO use card types
                            String depositFeePaymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, String.format("%s.%s", paymentScheme, depositFeeOperation));

                            String holdBody = painMapper.writeValueAsString(new CurrentAccountTransactionBody()
                                    .setTransactionAmount(transactionFeeAmount)
                                    .setSequenceDateTime(sequenceDateTime)
                                    .setOriginalAmount(externalHoldAmount)
                                    .setLocale(locale)
                                    .setDateFormat(FORMAT)
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
                                    ))
                            );

                            String cardTransactionBody = painMapper.writeValueAsString(new CurrentAccountTransactionBody()
                                    .setSequenceDateTime(sequenceDateTime)
                                    .setTransactionAmount(transactionFeeAmount)
                                    .setDateFormat(FORMAT)
                                    .setLocale(locale)
                                    .setPaymentTypeId(depositFeePaymentTypeId)
                                    .setCurrencyCode(currency)
                                    .setDatatables(List.of(
                                            new CurrentAccountTransactionBody.DataTable(List.of(
                                                    new CurrentAccountTransactionBody.Entry()
                                                            .setAccount_iban("TODO")
                                                            .setStructured_transaction_details("TODO")
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
                                    ))
                            );

                            String caller = "transferToConversionAccountAndUpdateEHoldInAms";
                            items.add(batchItemBuilder.createExternalHoldItem(1, caller, internalCorrelationId, holdUrl, tenantIdentifier, holdBody));
                            items.add(batchItemBuilder.createTransactionItem(2, caller, internalCorrelationId, withdrawalUrl, tenantIdentifier, cardTransactionBody, null));
                            items.add(batchItemBuilder.createTransactionItem(3, caller, internalCorrelationId, depositUrl, tenantIdentifier, cardTransactionBody, null));
                        }

                        // execute batch
                        Pair<String, List<BatchResponse>> out = moneyInOutWorker.doBatch(items, tenantIdentifier, transactionGroupId, disposalAccountAmsId, conversionAccountAmsId, internalCorrelationId, "transferToConversionAccountAndUpdateEHoldInAmsWorker");
                        BatchResponse response = out.getRight().get(0);
                        DocumentContext json = JsonPath.parse(response.getBody());
                        BigDecimal availableBalance = json.read("$.changes.availableBalance", BigDecimal.class);
                        logger.info("returning availableBalance: {} from json response: {}", availableBalance, response.getBody());
                        return Map.of("availableBalance", availableBalance);

                    } catch (ZeebeBpmnError z) {
                        throw z;
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                        throw new RuntimeException(e);
                    } finally {
                        MDC.remove("internalCorrelationId");
                    }
                }
        );
    }
}
