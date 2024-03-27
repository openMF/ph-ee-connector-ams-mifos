package org.mifos.connector.ams.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import iso.std.iso._20022.tech.json.camt_053_001.EntryTransaction10;
import iso.std.iso._20022.tech.json.camt_053_001.ReportEntry10;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class SerializationHelper {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    @Qualifier("painMapper")
    public ObjectMapper painMapper;


    public String writeCamt053AsString(String accountProductType, ReportEntry10 camt053) {
        try {
            if ("CURRENT".equalsIgnoreCase(accountProductType)) {
                logger.trace("serializeCamt053 for Current account");
                EntryTransaction10 transactionDetail = camt053.getEntryDetails().get(0).getTransactionDetails().get(0);
                return painMapper.writeValueAsString(removeFieldsFromCurrentAccount(transactionDetail));

            } else {
                logger.trace("serializeCamt053 for Savings account");
                return painMapper.writeValueAsString(camt053);
            }

        } catch (Exception e) {
            throw new RuntimeException("failed to serialize camt053", e);
        }
    }

    EntryTransaction10 removeFieldsFromCurrentAccount(EntryTransaction10 transactionDetail) {
        transactionDetail.setAdditionalTransactionInformation(null);
        transactionDetail.getSupplementaryData().forEach(data -> {
            try {
                Object omSupplementaryData = data.getEnvelope().getAdditionalProperties().get("OrderManagerSupplementaryData");
                if (omSupplementaryData == null) return;
                ((Map) omSupplementaryData).remove("transactionCreationChannel");
            } catch (Exception e) {
                logger.warn("failed to remove supplementaryData from transactionDetail: " + transactionDetail.getSupplementaryData(), e);
            }
        });
        return transactionDetail;
    }
}