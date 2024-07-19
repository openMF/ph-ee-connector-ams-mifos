package org.mifos.connector.ams.zeebe.workers;

import org.junit.jupiter.api.Test;

class TestCardWorkers {

    @Test
    public void testDetectDates() {
        String sample = "2024-07-19T10:42:43.636+0200";
        String format = new CardWorkers().detectDateTimeFormat(sample);
        System.out.println(format);
    }
}