package org.mifos.connector.ams.fineract.currentaccount.response;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestCAGetResponse {

    @Test
    public void test() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        CAGetResponse response = objectMapper.readValue("{" +
                "\"accountBalance\": 2775495665.000000," +
                "\"minimumRequiredBalance\": 123.45" +
                "}", CAGetResponse.class);

        assertEquals(2775495665L, response.getAccountBalance().longValue());
        assertEquals(new BigDecimal("123.45"), response.getMinimumRequiredBalance());
        System.out.println(objectMapper.writeValueAsString(response));
    }
}