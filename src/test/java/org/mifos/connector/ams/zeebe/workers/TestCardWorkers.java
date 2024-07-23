package org.mifos.connector.ams.zeebe.workers;

import org.junit.jupiter.api.Test;

class TestCardWorkers {

    @Test
    public void testDetectDates() {
        String sample = "2024-07-19T10:42:43.636+0200";
        String format = new CardWorkers().detectDateTimeFormat(sample);
        System.out.println(format);

        String sample2 = "2024-07-22T15:04:33.090";
        String format2 = new CardWorkers().detectDateTimeFormat(sample2);
        System.out.println(format2);
    }

    @Test
    public void testIncrease() {
        String sample = "2024-07-19T10:42:43.636+0200";
        CardWorkers cardWorkers = new CardWorkers();
        String format = cardWorkers.detectDateTimeFormat(sample);
        System.out.println(cardWorkers.increase(sample, format));

        String sample2 = "2024-07-22T15:04:33.090";
        String format2 = cardWorkers.detectDateTimeFormat(sample2);
        System.out.println(cardWorkers.increase(sample2, format2));

    }
}