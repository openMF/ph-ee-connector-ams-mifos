package org.mifos.connector.ams.interop;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.mifos.connector.ams.interop.errordto.ErrorResponse;
import org.mifos.connector.ams.interop.errordto.FineractError;
import org.mifos.connector.common.camel.ErrorHandlerRouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import static org.mifos.connector.ams.zeebe.ZeebeVariables.*;

@Component
//@ConditionalOnExpression("${ams.local.enabled}")
public class ErrorParserRouteBuilder extends ErrorHandlerRouteBuilder {

    @Autowired
    private ObjectMapper objectMapper;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void configure() {

        // parses error payload from fineract and sets relevant error related variable if error exist
        from("direct:error-handler")
                .id("error-handler")
                .log(LoggingLevel.INFO, "Error handler for response ${body}")
                .choice()
                // check if http status code is >= 202
                .when(e -> e.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class) >= 400)
                .unmarshal().json(JsonLibrary.Jackson, ErrorResponse.class)
                .process(exchange -> {
                    ErrorResponse errorResponse = exchange.getIn().getBody(ErrorResponse.class);
                    logger.info("Error response parsed: {}", errorResponse);
                    try {
                        FineractError fineractError = errorResponse.getErrors().get(0);
                        exchange.setProperty(ERROR_CODE, fineractError.getUserMessageGlobalisationCode());
                        exchange.setProperty(ERROR_INFORMATION, fineractError.getDefaultUserMessage());
                    } catch (Exception e) {
                        exchange.setProperty(ERROR_CODE, errorResponse.getUserMessageGlobalisationCode());
                        exchange.setProperty(ERROR_INFORMATION, errorResponse.getDefaultUserMessage());
                    }
                    exchange.setProperty(ERROR_PAYLOAD, errorResponse);
                })
                .otherwise()
                .endChoice();
    }
}
