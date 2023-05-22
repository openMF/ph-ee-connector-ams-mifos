package org.mifos.connector.ams.fineract;

import java.util.Map;

public class PaymentTypeConfig {
	
	private String tenant;
	private Map<String, Integer> paymentTypes;

	PaymentTypeConfig(String tenant, Map<String, Integer> paymentTypes) {
		super();
		this.tenant = tenant;
		this.paymentTypes = paymentTypes;
	}

	public String getTenant() {
		return tenant;
	}

	public void setTenant(String tenant) {
		this.tenant = tenant;
	}

	public Map<String, Integer> getPaymentTypes() {
		return paymentTypes;
	}

	public void setPaymentTypes(Map<String, Integer> paymentTypes) {
		this.paymentTypes = paymentTypes;
	}
	
	public Integer findPaymentTypeByOperation(String operation) {
		return paymentTypes.get(operation);
	}
}
