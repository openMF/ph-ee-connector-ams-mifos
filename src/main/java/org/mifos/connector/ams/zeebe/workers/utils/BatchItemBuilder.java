package org.mifos.connector.ams.zeebe.workers.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import iso.std.iso._20022.tech.json.pain_001_001.SupplementaryData1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
public class BatchItemBuilder {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    public AuthTokenHelper authTokenHelper;

    public void add(String tenantId, List<TransactionItem> items, String url, String body, boolean isDetails) throws JsonProcessingException {
        items.add(createTransactionItem(items.size() + 1, url, tenantId, body, isDetails ? items.size() : null));
    }

    private TransactionItem createTransactionItem(Integer requestId, String relativeUrl, String tenantId, String bodyItem, Integer reference) throws JsonProcessingException {
        List<Header> headers = headers(tenantId);
        return new TransactionItem(requestId, relativeUrl, "POST", reference, headers, bodyItem);
    }

    private List<Header> headers(String tenantId) {
        List<Header> headers = new ArrayList<>();
        headers.add(new Header("Idempotency-Key", UUID.randomUUID().toString()));
        headers.add(new Header("Content-Type", "application/json"));
        headers.add(new Header("Fineract-Platform-TenantId", tenantId));
        headers.add(new Header("Authorization", authTokenHelper.generateAuthToken()));
        return headers;
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
}
