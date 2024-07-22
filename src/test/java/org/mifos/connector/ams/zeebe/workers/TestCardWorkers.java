package org.mifos.connector.ams.zeebe.workers;

import org.junit.jupiter.api.Test;

class TestCardWorkers {

    @Test
    public void testDetectDates() {
        String sample = "2024-07-19T10:42:43.636+0200";
        String format = new CardWorkers().detectDateTimeFormat(sample);
        System.out.println(format);
    }

    @Test
    public void testIncrease() {
        String sample = "2024-07-19T10:42:43.636+0200";
        CardWorkers cardWorkers = new CardWorkers();
        String format = cardWorkers.detectDateTimeFormat(sample);
        System.out.println(cardWorkers.increase(sample, format));
    }
}