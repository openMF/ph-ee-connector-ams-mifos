package org.mifos.connector.ams.fineract;

import java.util.Map;

public class Config {
	
	private String tenant;
	private Map<String, Integer> paymentTypeIdConfigs;
	private Map<String, String> paymentTypeCodeConfigs;

	Config(String tenant, Map<String, Integer> ptiConfigs, Map<String, String> ptcConfigs) {
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

	public Map<String, Integer> getPaymentTypeIdConfigs() {
		return paymentTypeIdConfigs;
	}

	public void setPaymentTypeIdConfigs(Map<String, Integer> configs) {
		this.paymentTypeIdConfigs = configs;
	}
	
	public Map<String, String> getPaymentTypeCodeConfigs() {
		return paymentTypeCodeConfigs;
	}
	
	public void setPaymentTypeCodeConfigs(Map<String, String> configs) {
		this.paymentTypeCodeConfigs = configs;
	}
	
	public Integer findPaymentTypeIdByOperation(String operation) {
		return paymentTypeIdConfigs.get(operation);
	}
	
	public String findPaymentTypeCodeByOperation(String operation) {
		return paymentTypeCodeConfigs.get(operation);
	}
}
