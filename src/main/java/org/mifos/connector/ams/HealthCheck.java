package org.mifos.connector.ams;

import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.json.JSONObject;
import org.mifos.connector.ams.interop.AmsService;
import org.mifos.connector.common.ams.dto.PartyFspResponseDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static org.mifos.connector.ams.zeebe.ZeebeVariables.*;

@Component
public class HealthCheck extends RouteBuilder {

    @Override
    public void configure() {
        from("rest:GET:/")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(200))
                .setBody(constant(""));
    }
}
