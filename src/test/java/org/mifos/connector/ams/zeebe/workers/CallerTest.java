package org.mifos.connector.ams.zeebe.workers;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CallerTest {

    @Test
    public void testCaller() {
        assertEquals("testCaller", hello());
    }

    private String hello() {
        String caller = Thread.currentThread().getStackTrace()[2].getMethodName();
        System.out.println(caller);
        return caller;
    }
}
