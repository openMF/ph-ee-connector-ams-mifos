package org.mifos.connector.ams.fineract;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Configuration
public class PaymentTypeConfiguration {

	@Value("${TENANT_CONFIGS}")
	private String tenantConfigsJson;
	
	@Bean(name = "paymentTypeConfigFactory")
	public ConfigFactory paymentTypeConfigFactory() throws JsonProcessingException {
		Map<String, Map<String, List<LinkedHashMap<String, Object>>>> tenantConfigs = readTenantConfigs();
		
		Map<String, Map<String, Integer>> paymentTypeConfigMap = new HashMap<>();
		for (Entry<String, Map<String, List<LinkedHashMap<String, Object>>>> tenantEntry : tenantConfigs.entrySet()) {
			populateConfigMap(paymentTypeConfigMap, tenantEntry, "paymentTypeConfigs", "Operation", "FineractId");
		}
		
		return new ConfigFactory(paymentTypeConfigMap);
	}

	private void populateConfigMap(Map<String, Map<String, Integer>> paymentTypeConfigMap,
			Entry<String, Map<String, List<LinkedHashMap<String, Object>>>> tenantEntry, 
			String configName,
			String keyName, 
			String valueName) {
		Map<String, Integer> paymentTypeMap = tenantEntry.getValue().get(configName)
				.stream()
				.collect(
						Collectors.toMap(
								config -> (String) config.get(keyName),
								config -> (Integer) config.get(valueName)));
		paymentTypeConfigMap.put(tenantEntry.getKey(), paymentTypeMap);
	}

	private Map<String, Map<String, List<LinkedHashMap<String, Object>>>> readTenantConfigs()
			throws JsonProcessingException {
		ObjectMapper objectMapper = new ObjectMapper();
		JsonNode jsonNode = objectMapper.readTree(tenantConfigsJson);
		return objectMapper.convertValue(jsonNode, new TypeReference<Map<String, Map<String, List<LinkedHashMap<String, Object>>>>>() {});
	}
	
	@Bean(name = "technicalAccountConfigFactory")
	public ConfigFactory technicalAccountConfigFactory() throws JsonProcessingException {
		Map<String, Map<String, List<LinkedHashMap<String, Object>>>> tenantConfigs = readTenantConfigs();
		
		Map<String, Map<String, Integer>> paymentTypeConfigMap = new HashMap<>();
		for (Entry<String, Map<String, List<LinkedHashMap<String, Object>>>> tenantEntry : tenantConfigs.entrySet()) {
			populateConfigMap(paymentTypeConfigMap, tenantEntry, "technicalAccountConfigs", "Operation", "AccountId");
		}
		
		return new ConfigFactory(paymentTypeConfigMap);
	}
}
