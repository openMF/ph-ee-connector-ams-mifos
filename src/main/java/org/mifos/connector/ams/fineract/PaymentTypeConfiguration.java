package org.mifos.connector.ams.fineract;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PaymentTypeConfiguration {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${TENANT_CONFIGS}")
    private String tenantConfigsJson;

    @Autowired
    private ObjectMapper objectMapper;

    @Bean
    public TenantConfigs tenantConfigs() throws JsonProcessingException {
        TenantConfigs tenantConfigs = objectMapper.readValue(tenantConfigsJson, TenantConfigs.class);
        logger.info("Read tenant configs: " + tenantConfigs);
        return tenantConfigs;
    }
}
