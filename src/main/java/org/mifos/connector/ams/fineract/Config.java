package org.mifos.connector.ams.fineract;

import java.util.Map;

public class Config {
	
	private String tenant;
	private Map<String, Integer> configs;

	Config(String tenant, Map<String, Integer> configs) {
		super();
		this.tenant = tenant;
		this.configs = configs;
	}

	public String getTenant() {
		return tenant;
	}

	public void setTenant(String tenant) {
		this.tenant = tenant;
	}

	public Map<String, Integer> getConfigs() {
		return configs;
	}

	public void setConfigs(Map<String, Integer> configs) {
		this.configs = configs;
	}
	
	public Integer findByOperation(String operation) {
		return configs.get(operation);
	}
}
