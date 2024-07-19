package org.mifos.connector.ams.zeebe.workers.bookamount;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TestTransferToConversionAccountAndUpdateEHoldInAmsWorker {

    @Test
    public void testDetectDates() {
        String sample = "2024-07-19T10:42:43.636+0200";
        String format = new TransferToConversionAccountAndUpdateEHoldInAmsWorker().detectDateTimeFormat(sample);
        System.out.println(format);
    }
}