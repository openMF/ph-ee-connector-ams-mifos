package org.mifos.connector.ams.fineract;

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
	
	@Bean
	@SuppressWarnings("unchecked")
	public PaymentTypeConfigFactory paymentTypeConfigFactory() throws JsonProcessingException {
		ObjectMapper objectMapper = new ObjectMapper();
		JsonNode jsonNode = objectMapper.readTree(tenantConfigsJson);
		// Map<String, List<PaymentType>> paymentTypeConfig = new ObjectMapper().readValue(tenantConfigsJson, Map.class);
		Map<String, List<PaymentType>> paymentTypeConfig = objectMapper.convertValue(jsonNode, new TypeReference<Map<String, List<PaymentType>>>() {});
		
		Map<String, Map<String, Integer>> paymentTypeConfigMap = paymentTypeConfig
				.entrySet()
				.stream()
				.collect(
						Collectors.toMap(
								Entry::getKey, 
								entry -> entry
										.getValue()
										.stream()
										.collect(
												Collectors.toMap(
														PaymentType::operation, 
														PaymentType::fineractId
												)
										)
						)
				);
		
		return new PaymentTypeConfigFactory(paymentTypeConfigMap);
	}
}
