package org.mifos.connector.ams.zeebe.workers.utils;

import java.io.IOException;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersionDetector;
import com.networknt.schema.ValidationMessage;

import jakarta.annotation.PostConstruct;

@Component
public class JsonSchemaValidator {

//    private JsonSchema schema;

//    @Autowired
//    private ObjectMapper objectMapper;

//    @PostConstruct
//    public void init() throws IOException {
//        JsonNode jsonNode = objectMapper.readTree(new ClassPathResource("/iso20022-plus-json/pain_001_001/pain.001.001.10-CustomerCreditTransferInitiationV10.Message.schema.json").getInputStream());
//        JsonSchemaFactory sf = JsonSchemaFactory.getInstance(SpecVersionDetector.detect(jsonNode));
//        schema = sf.getSchema(new ClassPathResource("/iso20022-plus-json/pain_001_001/pain.001.001.10-CustomerCreditTransferInitiationV10.Message.schema.json").getInputStream());
//    }

//    public Set<ValidationMessage> validate(String pain001) throws JsonProcessingException {
//        JsonNode json = objectMapper.readTree(pain001);
//        return schema.validate(json);
//    }
}
