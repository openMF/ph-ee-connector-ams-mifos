package org.mifos.connector.ams.fineract;

import java.util.Map;

public class Config {

    private String tenant;
    private Map<String, String> paymentTypeIdConfigs;
    private Map<String, String> paymentTypeCodeConfigs;

    Config(String tenant, Map<String, String> ptiConfigs, Map<String, String> ptcConfigs) {
        super();
        this.tenant = tenant;
        this.paymentTypeIdConfigs = ptiConfigs;
        this.paymentTypeCodeConfigs = ptcConfigs;
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public Map<String, String> getPaymentTypeIdConfigs() {
        return paymentTypeIdConfigs;
    }

    public void setPaymentTypeIdConfigs(Map<String, String> configs) {
        this.paymentTypeIdConfigs = configs;
    }

    public Map<String, String> getPaymentTypeCodeConfigs() {
        return paymentTypeCodeConfigs;
    }

    public void setPaymentTypeCodeConfigs(Map<String, String> configs) {
        this.paymentTypeCodeConfigs = configs;
    }

    public String findPaymentTypeIdByOperation(String operation) {
        return paymentTypeIdConfigs.get(operation);
    }

    public String findPaymentTypeCodeByOperation(String operation) {
        return paymentTypeCodeConfigs.get(operation);
    }
}
