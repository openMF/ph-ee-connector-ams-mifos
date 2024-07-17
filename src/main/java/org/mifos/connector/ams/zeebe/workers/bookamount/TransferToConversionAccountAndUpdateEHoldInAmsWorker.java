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
import org.mifos.connector.ams.zeebe.workers.utils.BatchItemBuilder;
import org.mifos.connector.ams.zeebe.workers.utils.CurrentAccountTransactionBody;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;
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
import java.util.Optional;

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
    public Map<String, Object> transferToConversionAccountAndUpdateEHoldInAmsWorker(
            JobClient jobClient,
            ActivatedJob activatedJob,
            @Variable String internalCorrelationId,
            @Variable String transactionGroupId,
            @Variable String tenantIdentifier,
            @Variable String paymentScheme,
            @Variable String disposalAccountAmsId,
            @Variable String conversionAccountAmsId,
            @Variable String merchName,
            @Variable BigDecimal transactionFeeAmount,
            @Variable String currency,
            @Variable String requestId,
            @Variable String messageId,
            @Variable String cardToken,
            @Variable String maskedPan,
            @Variable String cardHolderName,
            @Variable String partnerCity,
            @Variable String partnerCountry,
            @Variable String instructedAmount,
            @Variable String instructedCurrency,
            @Variable String processCode,
            @Variable String merchantCategoryCode,
            @Variable String isEcommerce,
            @Variable String paymentTokenWallet
    ) {
        return eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "transferToConversionAccountAndUpdateEHoldInAmsWorker", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> {
                    MDC.put("internalCorrelationId", internalCorrelationId);
                    List<TransactionItem> items = new ArrayList<>();

                    String apiPath = currentAccountApi.substring(1);
                    String disposalAccountWithdrawRelativeUrl = String.format("%s%s/transactions?command=%s&force-type=hold", apiPath, disposalAccountAmsId, "withdrawal");
                    String conversionAccountDepositRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, conversionAccountAmsId, "deposit");

                    try {
                        // STEP 1 - add fee
                        logger.debug("Depositing fee {} to conversion account {}", transactionFeeAmount, conversionAccountAmsId);
                        String depositFeeOperation = "transferToConversionAccountInAms.ConversionAccount.DepositTransactionFee";
                        String depositFeePaymentTypeId = tenantConfigs.findPaymentTypeId(tenantIdentifier, String.format("%s.%s", paymentScheme, depositFeeOperation));
                        String depositFeePaymentTypeCode = Optional.ofNullable(tenantConfigs.findResourceCode(tenantIdentifier, String.format("%s.%s", paymentScheme, depositFeeOperation))).orElse("");
                        String transactionBody = painMapper.writeValueAsString(new CurrentAccountTransactionBody(transactionFeeAmount, FORMAT, locale, depositFeePaymentTypeId, currency, List.of(
                                new CurrentAccountTransactionBody.DataTable(List.of(
                                        new CurrentAccountTransactionBody.Entry()
                                                .setAccount_iban(conversionAccountAmsId)
                                                .setStructured_transaction_details("TODO")
                                                .setInternal_correlation_id(internalCorrelationId)
                                                .setTransaction_group_id(transactionGroupId)
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
                                ), "dt_current_transaction_details")
                        )));
                        batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, disposalAccountWithdrawRelativeUrl, transactionBody, false);
                        batchItemBuilder.add(tenantIdentifier, internalCorrelationId, items, conversionAccountDepositRelativeUrl, transactionBody, false);


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
