package org.mifos.connector.ams.fineract;

import java.util.Map;

public class ConfigFactory {

	private final Map<String, Map<String, Integer>> configs;
	
	public ConfigFactory(Map<String, Map<String, Integer>> configs) {
		this.configs = configs;
	}
	
	public Config getConfig(String tenant) {
		Map<String, Integer> config = configs.getOrDefault(tenant, null);
		if (config == null) {
			throw new IllegalArgumentException("No tenant configuration found for " + tenant);
		}
		return new Config(tenant, config);
	}
}
