package org.mifos.connector.ams.common;

import iso.std.iso._20022.tech.json.camt_053_001.EntryTransaction10;
import iso.std.iso._20022.tech.json.camt_053_001.SupplementaryData1;
import iso.std.iso._20022.tech.json.camt_053_001.SupplementaryDataEnvelope1;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TestSerializationHelper {

    @Test
    public void testRemoveSupplementaryData() {
        EntryTransaction10 entryTransaction10 = new EntryTransaction10();
        SupplementaryData1 supplementaryData1 = new SupplementaryData1();
        entryTransaction10.getSupplementaryData().add(supplementaryData1);

        SupplementaryDataEnvelope1 envelope = new SupplementaryDataEnvelope1();
        supplementaryData1.setEnvelope(envelope);

        HashMap<Object, Object> map = new HashMap<>();
        envelope.setAdditionalProperty("OrderManagerSupplementaryData", map);
        map.put("internalCorrelationId", "123");
        map.put("transactionCreationChannel", "simulator");

        new SerializationHelper().removeFieldsFromCurrentAccount(entryTransaction10);
    }
    // {internalCorrelationId=BBIXHUH0_88646a16e4fc4a88b03deff34e59f05a,
    // transactionGroupId=BBIXHUH0_88646a16e4fc4a88b03deff34e59f05a,
    // acceptanceDatetime=2024-02-14T16:51:10.865613467Z,
    // categoryPurpose={Code=MAIN},
    // transactionCreationChannel=simulator}

}