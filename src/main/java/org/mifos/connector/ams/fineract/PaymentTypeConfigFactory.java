package org.mifos.connector.ams.fineract;

import java.util.Map;

public class PaymentTypeConfigFactory {

	private final Map<String, Map<String, Integer>> paymentTypeConfigs;
	
	public PaymentTypeConfigFactory(Map<String, Map<String, Integer>> paymentTypeConfigs) {
		this.paymentTypeConfigs = paymentTypeConfigs;
	}
	
	public PaymentTypeConfig getPaymentTypeConfig(String tenant) {
		Map<String, Integer> paymentTypes = paymentTypeConfigs.getOrDefault(tenant, null);
		if (paymentTypes == null) {
			throw new IllegalArgumentException("No tenant configuration found for " + tenant);
		}
		return new PaymentTypeConfig(tenant, paymentTypes);
	}
}
