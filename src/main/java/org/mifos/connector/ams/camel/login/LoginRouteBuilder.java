package org.mifos.connector.ams.camel.login;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.camel.LoggingLevel;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.mifos.common.ams.dto.LoginFineractXResponseDTO;
import org.mifos.common.camel.ErrorHandlerRouteBuilder;
import org.mifos.connector.ams.camel.cxfrs.CxfrsUtil;
import org.mifos.connector.ams.properties.Tenant;
import org.mifos.connector.ams.tenant.TenantService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import static org.mifos.connector.ams.camel.cxfrs.HeaderBasedInterceptor.CXF_TRACE_HEADER;


@Component
public class LoginRouteBuilder extends ErrorHandlerRouteBuilder {

    public static final String TENANT_ID_PROPERTY = "TENANT_ID";
    public static final String LOGIN_USERNAME_PROPERTY = "LOGIN_USERNAME";
    public static final String LOGIN_PASSWORD_PROPERTY = "LOGIN_PASSWORD";

    @Value("${ams.local.base-url}")
    private String fpsLocalBaseUrl;

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
        from("direct:send-auth-request")
                .id("send-local-auth")
                .log(LoggingLevel.INFO, "local fsp auth request")
                .process(e -> {
                    Map<String, Object> headers = new HashMap<>();
                    headers.put(CXF_TRACE_HEADER, true);
                    headers.put("CamelHttpMethod", "POST");
                    headers.put("CamelHttpPath", fpsLocalBaseUrl + fpsLocalAuthPath);
                    headers.put("Content-Type", "application/json");
                    headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID_PROPERTY, String.class), false));

                    ObjectNode authJson = objectMapper.createObjectNode();
                    authJson.put("username", e.getProperty(LOGIN_USERNAME_PROPERTY, String.class));
                    authJson.put("password", e.getProperty(LOGIN_PASSWORD_PROPERTY, String.class));
                    cxfrsUtil.sendInOut("cxfrs:bean:ams.local", e, headers, objectMapper.writeValueAsString(authJson));
                })
                .unmarshal().json(JsonLibrary.Jackson, LoginFineractXResponseDTO.class)
                .process(fspLoginResponseProcessor);
    }
}