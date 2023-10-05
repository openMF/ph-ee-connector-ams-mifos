package org.mifos.connector;

import java.util.concurrent.TimeUnit;

import org.apache.camel.Processor;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.camunda.zeebe.spring.client.EnableZeebeClient;

@EnableZeebeClient
@SpringBootApplication
public class AmsConnectorApplication {

    public static void main(String[] args) {
        SpringApplication.run(AmsConnectorApplication.class, args);
    }

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        return objectMapper
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Bean
    public Processor pojoToString(ObjectMapper objectMapper) {
        return exchange -> exchange.getIn().setBody(objectMapper.writeValueAsString(exchange.getIn().getBody()));
    }
    
    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder builder) {
    	PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    	connectionManager.setMaxTotal(500);
    	connectionManager.setDefaultMaxPerRoute(500);
    	
    	RequestConfig requestConfig = RequestConfig
    			.custom()
    			.setConnectionRequestTimeout(1000, TimeUnit.MILLISECONDS)
    			.setConnectTimeout(1000, TimeUnit.MILLISECONDS)
    			.build();
    	
    	HttpClient httpClient = HttpClientBuilder.create()
    			.setConnectionManager(connectionManager)
    			.setDefaultRequestConfig(requestConfig)
    			.build();
    	
    	ClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory(httpClient);
    	
    	return new RestTemplate(requestFactory);
    }
}
