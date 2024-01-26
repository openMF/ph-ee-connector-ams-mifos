package org.mifos.connector.ams.zeebe.workers.utils;

import org.springframework.stereotype.Component;

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
