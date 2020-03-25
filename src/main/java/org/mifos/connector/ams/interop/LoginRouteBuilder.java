package org.mifos.connector.ams.interop;

import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.component.cxf.common.message.CxfConstants;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.mifos.connector.ams.camel.cxfrs.CxfrsUtil;
import org.mifos.phee.common.ams.dto.LoginFineractCnResponseDTO;
import org.mifos.phee.common.camel.ErrorHandlerRouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.mifos.connector.ams.camel.config.CamelProperties.LOGIN_PASSWORD;
import static org.mifos.connector.ams.camel.config.CamelProperties.LOGIN_USERNAME;
import static org.mifos.connector.ams.camel.config.CamelProperties.TENANT_ID;
import static org.mifos.connector.ams.camel.cxfrs.HeaderBasedInterceptor.CXF_TRACE_HEADER;
import static org.mifos.connector.ams.tenant.TenantService.X_TENANT_IDENTIFIER_HEADER;


@Component
@ConditionalOnExpression("${ams.local.enabled}")
public class LoginRouteBuilder extends ErrorHandlerRouteBuilder {

    @Value("${ams.local.auth.path}")
    private String amsLocalAuthPath;

    @Autowired
    private CxfrsUtil cxfrsUtil;

    public LoginRouteBuilder() {
        super.configure();
    }

    @Override
    public void configure() {
        from("direct:send-fincn-auth-request")
                .id("send-fincn-auth-request")
                .log(LoggingLevel.INFO, "local fincn auth request for tenant: ${exchangeProperty. " + TENANT_ID + "}")
                .process(e -> {
                    Map<String, Object> headers = new HashMap<>();
                    headers.put(CXF_TRACE_HEADER, true);
                    headers.put("CamelHttpMethod", "POST");
                    headers.put("CamelHttpPath", amsLocalAuthPath);
                    headers.put(X_TENANT_IDENTIFIER_HEADER, e.getProperty(TENANT_ID));

//                    Map<String, String> queryMap = new LinkedHashMap<>();
//                    queryMap.put("grant_type", "password");
//                    queryMap.put("username", e.getProperty(LOGIN_USERNAME, String.class));
//                    queryMap.put("password", Base64.getEncoder().encodeToString(e.getProperty(LOGIN_PASSWORD, String.class).getBytes()));
//                    headers.put(CxfConstants.CAMEL_CXF_RS_QUERY_MAP, queryMap);
                    headers.put(Exchange.HTTP_QUERY,
                            constant("grant_type=password&username="+e.getProperty(LOGIN_USERNAME, String.class)+"&password="+Base64.getEncoder().encodeToString(e.getProperty(LOGIN_PASSWORD, String.class).getBytes())));

                    cxfrsUtil.sendInOut("cxfrs:bean:ams.local.auth", e, headers, null);
                })
                .unmarshal().json(JsonLibrary.Jackson, LoginFineractCnResponseDTO.class);
    }
}