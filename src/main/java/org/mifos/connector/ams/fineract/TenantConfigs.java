package org.mifos.connector.ams.fineract;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Data
public class TenantConfigs {
    private Map<String, Tenant> tenants = new HashMap<>();

    @JsonAnySetter
    public void addTenant(String key, Tenant value) {
        tenants.put(key, value);
    }

    public Tenant getTenant(String key) {
        return tenants.get(key);
    }

    public String findPaymentTypeId(String tenant, String operation) {
        return Optional.ofNullable(tenants.get(tenant).findPaymentTypeByOperation(operation))
                .map(PaymentTypeConfig::getFineractId)
                .orElse(null);
    }

    public String findResourceCode(String tenant, String operation) {
        return Optional.ofNullable(tenants.get(tenant).findPaymentTypeByOperation(operation))
                .map(PaymentTypeConfig::getResourceCode)
                .orElse(null);
    }


    @Data
    public static class Tenant {
        @JsonProperty("paymentTypeConfigs")
        private List<PaymentTypeConfig> paymentTypeConfigs;

        public PaymentTypeConfig findPaymentTypeByOperation(String operation) {
            return paymentTypeConfigs
                    .stream()
                    .filter(x -> x.getOperation().equals(operation))
                    .findFirst()
                    .orElse(null);
        }
    }

    @Data
    public static class PaymentTypeConfig {
        @JsonProperty("Operation")
        private String operation;
        @JsonProperty("ResourceCode")
        private String resourceCode;
        @JsonProperty("FineractId")
        private String fineractId;
        @JsonProperty("AccountId")
        private String accountId;
    }
}