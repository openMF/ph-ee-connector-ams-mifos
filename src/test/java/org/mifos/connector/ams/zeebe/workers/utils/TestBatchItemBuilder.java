package org.mifos.connector.ams.zeebe.workers.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import iso.std.iso._20022.tech.json.camt_053_001.EntryDetails9;
import iso.std.iso._20022.tech.json.camt_053_001.EntryTransaction10;
import iso.std.iso._20022.tech.json.camt_053_001.ReportEntry10;
import org.junit.jupiter.api.Test;
import org.mifos.connector.ams.common.SerializationHelper;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TestBatchItemBuilder {

    @Test
    public void test() throws Exception {
        EntryTransaction10 tx = new EntryTransaction10();
        BatchItemBuilder batchItemBuilder = new BatchItemBuilder();
        batchItemBuilder.setAmount(tx, BigDecimal.TEN, "USD");


        ReportEntry10 reportEntry10 = new ReportEntry10();
        reportEntry10.setEntryDetails(List.of(new EntryDetails9(null, List.of(tx))));
        SerializationHelper serializationHelper = new SerializationHelper();
        serializationHelper.painMapper = new ObjectMapper();
        serializationHelper.writeCamt053AsString("CURRENT", reportEntry10);
        System.out.println(tx.toString());
    }

    @Test
    public void testIdempotencyKeyLength() {
        String idempotencyKey = BatchItemBuilder.createIdempotencyKey("12345678901234567890", "123456789012345678901234567890", 3);
        assertTrue(idempotencyKey.length() <= 50);
        System.out.println(idempotencyKey);
        System.out.println(idempotencyKey.length());
    }
}