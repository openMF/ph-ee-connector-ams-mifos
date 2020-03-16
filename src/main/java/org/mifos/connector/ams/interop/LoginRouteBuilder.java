package org.mifos.connector.ams.quote;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.camel.LoggingLevel;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.mifos.connector.ams.camel.cxfrs.CxfrsUtil;
import org.mifos.connector.ams.interop.FspLoginResponseProcessor;
import org.mifos.connector.ams.tenant.TenantService;
import org.mifos.phee.common.ams.dto.LoginFineractXResponseDTO;
import org.mifos.phee.common.camel.ErrorHandlerRouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import static org.mifos.connector.ams.camel.config.CamelProperties.LOGIN_PASSWORD;
import static org.mifos.connector.ams.camel.config.CamelProperties.LOGIN_USERNAME;
import static org.mifos.connector.ams.camel.config.CamelProperties.TENANT_ID;
import static org.mifos.connector.ams.camel.cxfrs.HeaderBasedInterceptor.CXF_TRACE_HEADER;


@Component
@ConditionalOnExpression("${ams.local.enabled}")
public class LoginRouteBuilder extends ErrorHandlerRouteBuilder {

    @Value("${ams.local.auth-path}")
    private String fpsLocalAuthPath;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private FspLoginResponseProcessor fspLoginResponseProcessor;

    @Autowired
    private CxfrsUtil cxfrsUtil;

    @Autowired
    private ObjectMapper objectMapper;

    public LoginRouteBuilder() {
        super.configure();
    }

    @Override
    public void configure() {
        // fin1.4
        from("direct:send-auth-request")
                .id("send-local-auth")
                .log(LoggingLevel.INFO, "local fsp auth request")
                .process(e -> {
                    Map<String, Object> headers = new HashMap<>();
                    headers.put(CXF_TRACE_HEADER, true);
                    headers.put("CamelHttpMethod", "POST");
                    headers.put("CamelHttpPath", fpsLocalAuthPath);
                    headers.put("Content-Type", "application/json");
                    headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID, String.class), false));

                    ObjectNode authJson = objectMapper.createObjectNode();
                    authJson.put("username", e.getProperty(LOGIN_USERNAME, String.class));
                    authJson.put("password", e.getProperty(LOGIN_PASSWORD, String.class));
                    cxfrsUtil.sendInOut("cxfrs:bean:ams.local", e, headers, objectMapper.writeValueAsString(authJson));
                })
                .unmarshal().json(JsonLibrary.Jackson, LoginFineractXResponseDTO.class)
                .process(fspLoginResponseProcessor);
    }
}