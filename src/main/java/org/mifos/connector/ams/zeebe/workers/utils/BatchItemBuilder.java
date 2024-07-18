package org.mifos.connector.ams.zeebe.workers.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import iso.std.iso._20022.tech.json.camt_053_001.ActiveOrHistoricCurrencyAndAmount;
import iso.std.iso._20022.tech.json.camt_053_001.AmountAndCurrencyExchange3;
import iso.std.iso._20022.tech.json.camt_053_001.AmountAndCurrencyExchangeDetails3;
import iso.std.iso._20022.tech.json.camt_053_001.EntryTransaction10;
import iso.std.iso._20022.tech.json.pain_001_001.SupplementaryData1;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Component
public class BatchItemBuilder {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    public AuthTokenHelper authTokenHelper;

    @Value("${fineract.idempotency.key-max-length}")
    public int maxLength = 50;


    public void add(String tenantId, String internalCorrelationId, List items, String url, String body, boolean isDetails) throws JsonProcessingException {
        String caller = Thread.currentThread().getStackTrace()[2].getMethodName();
        int stepNumber = items.size() + 1;

        items.add(createTransactionItem(stepNumber, caller, internalCorrelationId, url, tenantId, body, isDetails ? items.size() : null));
    }

    public ExternalHoldItem createExternalHoldItem(Integer stepNumber, String caller, String internalCorrelationId, String relativeUrl, String tenantId, String bodyItem) {
        String idempotencyKey = createIdempotencyKey(caller, internalCorrelationId, stepNumber);
        logger.debug("creating external hold item with idempotencyKey: {}", idempotencyKey);
        return new ExternalHoldItem()
                .setRelativeUrl(relativeUrl)
                .setRequestId(stepNumber)
                .setMethod("POST")
                .setHeaders(createHeaders(tenantId, idempotencyKey))
                .setBody(bodyItem);
    }

    public TransactionItem createTransactionItem(Integer stepNumber, String caller, String internalCorrelationId, String relativeUrl, String tenantId, String bodyItem, Integer reference) throws JsonProcessingException {
        String idempotencyKey = createIdempotencyKey(caller, internalCorrelationId, stepNumber);
        logger.debug("creating transaction item with idempotencyKey: {}", idempotencyKey);
        return new TransactionItem(stepNumber, relativeUrl, "POST", reference, createHeaders(tenantId, idempotencyKey), bodyItem);
    }

    private @NotNull List<Header> createHeaders(String tenantId, String idempotencyKey) {
        return List.of(new Header("Idempotency-Key", idempotencyKey),
                new Header("Content-Type", "application/json"),
                new Header("Fineract-Platform-TenantId", tenantId),
                new Header("Authorization", authTokenHelper.generateAuthToken())
        );
    }

    public String createIdempotencyKey(String caller, String internalCorrelationId, Integer stepNumber) {
        String key = String.format("%s-%s-%d", internalCorrelationId, caller, stepNumber);
        if (key.length() > maxLength) {
            key = key.substring(key.length() - maxLength);
        }
        return key;
    }

    public String findTransactionCreationChannel(List<SupplementaryData1> supplementaryData) {
        logger.debug("finding transactionCreationChannel in supplementaryData: {}", supplementaryData);
        for (SupplementaryData1 data : supplementaryData) {
            Map<String, Object> additionalProperties = data.getEnvelope().getAdditionalProperties();
            if (additionalProperties == null) continue;

            Object orderManagerSupplementaryData = additionalProperties.get("OrderManagerSupplementaryData");
            if (orderManagerSupplementaryData == null) continue;

            Map<String, Object> orderManagerSupplementaryDataMap = (Map<String, Object>) orderManagerSupplementaryData;
            Object transactionCreationChannel = orderManagerSupplementaryDataMap.get("transactionCreationChannel");

            if (transactionCreationChannel != null) return (String) transactionCreationChannel;
        }
        logger.debug("transactionCreationChannel not found in supplementaryData: {}", supplementaryData);
        return null;
    }

    public void setAmount(EntryTransaction10 entryTransaction10, BigDecimal amount, String currency) {
        AmountAndCurrencyExchange3 amountDetails = entryTransaction10.getAmountDetails();
        if (amountDetails == null) {
            amountDetails = new AmountAndCurrencyExchange3();
            entryTransaction10.setAmountDetails(amountDetails);
        }

        AmountAndCurrencyExchangeDetails3 transactionAmount = amountDetails.getTransactionAmount();
        if (transactionAmount == null) {
            transactionAmount = new AmountAndCurrencyExchangeDetails3();
            amountDetails.setTransactionAmount(transactionAmount);
        }

        transactionAmount.setAmount(new ActiveOrHistoricCurrencyAndAmount(amount, currency));
    }
}
