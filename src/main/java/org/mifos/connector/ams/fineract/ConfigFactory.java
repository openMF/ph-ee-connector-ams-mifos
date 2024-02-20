package org.mifos.connector.ams.fineract;

import java.util.Map;
import java.util.Optional;

public class ConfigFactory {

    private final Map<String, Map<String, String>> paymentTypeIdConfigs;

    private final Map<String, Map<String, String>> paymentTypeCodeConfigs;

    public ConfigFactory(Map<String, Map<String, String>> paymentTypeIdConfigs, Map<String, Map<String, String>> paymentTypeCodeConfigs) {
        this.paymentTypeIdConfigs = paymentTypeIdConfigs;
        this.paymentTypeCodeConfigs = paymentTypeCodeConfigs;
    }

    public Config getConfig(String tenant) {
        Map<String, String> paymentTypeIdConfig = paymentTypeIdConfigs.getOrDefault(tenant, null);
        if (paymentTypeIdConfig == null) {
            throw new IllegalArgumentException("No tenant payment type Id configuration found for " + tenant);
        }

        Map<String, String> paymentTypeCodeConfig = Optional.ofNullable(paymentTypeCodeConfigs)
                .map(configs -> configs.getOrDefault(tenant, null)).orElse(null);

        return new Config(tenant, paymentTypeIdConfig, paymentTypeCodeConfig);
    }
}
