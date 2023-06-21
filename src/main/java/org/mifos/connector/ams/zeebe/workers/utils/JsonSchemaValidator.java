package org.mifos.connector.ams.zeebe.workers.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersionDetector;
import com.networknt.schema.ValidationMessage;
import jakarta.annotation.PostConstruct;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Set;

@Component
public class JsonSchemaValidator {

    private JsonSchema schema;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    public void init() throws IOException {
        JsonNode jsonNode = objectMapper.readTree(new ClassPathResource("/iso20022-plus-json/pain_001_001/pain.001.001.10-CustomerCreditTransferInitiationV10.Message.schema.json").getInputStream());
        JsonSchemaFactory sf = JsonSchemaFactory.getInstance(SpecVersionDetector.detect(jsonNode));
        schema = sf.getSchema(new ClassPathResource("/iso20022-plus-json/pain_001_001/pain.001.001.10-CustomerCreditTransferInitiationV10.Message.schema.json").getInputStream());
    }

    public Set<ValidationMessage> validate(String pain001) throws JsonProcessingException {
        JsonNode json = objectMapper.readTree(pain001);
        return schema.validate(json);
    }
}
