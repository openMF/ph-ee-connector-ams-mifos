package org.mifos.connector.ams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mifos.connector.ams.fineract.TenantConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TenantConfigsTest {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Test
    public void test() throws Exception {
        String json = new String(getClass().getResourceAsStream("sample.json").readAllBytes());
        logger.debug("sample json: \n{}", json);
        ObjectMapper objectMapper = new ObjectMapper();
        TenantConfigs tenantConfigs = objectMapper.readValue(json, TenantConfigs.class);

        logger.info("tenant configs: \n{}", tenantConfigs);
        assertNotNull(tenantConfigs.getTenants().get("key1").getPaymentTypeConfigs().get(0).getResourceCode());
        assertNotNull(tenantConfigs.getTenants().get("key1").findPaymentTypeByOperation("HCT_INST.bookCreditedAmountToConversionAccount.ConversionAccount.DepositTransactionAmount").getResourceCode());
    }

}
