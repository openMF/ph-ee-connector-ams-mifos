package org.mifos.connector.ams.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import iso.std.iso._20022.tech.json.camt_053_001.EntryTransaction10;
import iso.std.iso._20022.tech.json.camt_053_001.ReportEntry10;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SerializationHelper {
    private static Logger logger = LoggerFactory.getLogger(SerializationHelper.class);

    private static ObjectMapper objectMapper = new ObjectMapper() {
        private static final long serialVersionUID = 1L;

        {
            registerModule(new AfterburnerModule());
            registerModule(new JavaTimeModule());
            configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
            setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
                    .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }
    };

    public static String writeCamt053AsString(String accountProductType, ReportEntry10 camt053) {
        try {
            if ("CURRENT".equalsIgnoreCase(accountProductType)) {
                logger.debug("serializeCamt053orFragment: Current account");
                EntryTransaction10 transactionDetail = camt053.getEntryDetails().get(0).getTransactionDetails().get(0);
                return objectMapper.writeValueAsString(transactionDetail);
            } else {
                logger.debug("serializeCamt053orFragment: Savings account");
                return objectMapper.writeValueAsString(camt053);
            }
        } catch (Exception e) {
            throw new RuntimeException("failed to serialize camt053", e);
        }
    }
}
