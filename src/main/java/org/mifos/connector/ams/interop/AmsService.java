package org.mifos.connector.ams.interop;

import org.apache.camel.Exchange;
import org.apache.camel.component.cxf.common.message.CxfConstants;
import org.mifos.connector.ams.camel.cxfrs.CxfrsUtil;
import org.mifos.connector.ams.tenant.TenantService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.camel.Exchange.HTTP_METHOD;
import static org.apache.camel.Exchange.HTTP_PATH;
import static org.mifos.connector.ams.camel.config.CamelProperties.LOGIN_PASSWORD;
import static org.mifos.connector.ams.camel.config.CamelProperties.LOGIN_USERNAME;
import static org.mifos.connector.ams.camel.config.CamelProperties.PARTY_IDENTIFIER_FOR_EXT_ACC;
import static org.mifos.connector.ams.camel.config.CamelProperties.PARTY_ID_TYPE_FOR_EXT_ACC;
import static org.mifos.connector.ams.camel.config.CamelProperties.TENANT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSFER_ACTION;
import static org.mifos.connector.ams.camel.cxfrs.HeaderBasedInterceptor.CXF_TRACE_HEADER;
import static org.mifos.connector.ams.tenant.TenantService.X_TENANT_IDENTIFIER_HEADER;

@Component
@ConditionalOnExpression("${ams.local.enabled}")
public class AmsService {

    @Value("${ams.local.interop-quotes-path}")
    private String amsInteropQuotesPath;

    @Value("${ams.local.interop-parties-path}")
    private String amsInteropPartiesPath;

    @Value("${ams.local.interop-transfers-path}")
    private String amsInteropTransfersPath;

    @Value("${ams.local.auth.path}")
    private String amsLocalAuthPath;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private CxfrsUtil cxfrsUtil;

    public void getLocalQuote(Exchange e) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(CXF_TRACE_HEADER, true);
        headers.put(HTTP_METHOD, "POST");
        headers.put(HTTP_PATH, amsInteropQuotesPath);
        headers.put("Content-Type", "application/json");
        headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID, String.class)));
        cxfrsUtil.sendInOut("cxfrs:bean:ams.local", e, headers, e.getIn().getBody());
    }

    public void getExternalAccount(Exchange e) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(CXF_TRACE_HEADER, true);
        headers.put(HTTP_METHOD, "GET");
        headers.put(HTTP_PATH, amsInteropPartiesPath.replace("{idType}", e.getProperty(PARTY_ID_TYPE_FOR_EXT_ACC, String.class))
                .replace("{idValue}", e.getProperty(PARTY_IDENTIFIER_FOR_EXT_ACC, String.class)));
        headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID, String.class)));
        cxfrsUtil.sendInOut("cxfrs:bean:ams.local", e, headers, null);
    }

    public void sendTransfer(Exchange e) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(CXF_TRACE_HEADER, true);
        headers.put(HTTP_METHOD, "POST");
        headers.put(HTTP_PATH, amsInteropTransfersPath);

        Map<String, String> queryMap = new LinkedHashMap<>();
        queryMap.put("action", e.getProperty(TRANSFER_ACTION, String.class));
        headers.put(CxfConstants.CAMEL_CXF_RS_QUERY_MAP, queryMap);
        headers.put("Content-Type", "application/json");
        headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID, String.class)));
        cxfrsUtil.sendInOut("cxfrs:bean:ams.local", e, headers, e.getIn().getBody());
    }

    public void login(Exchange e) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(CXF_TRACE_HEADER, true);
        headers.put(HTTP_METHOD, "POST");
        headers.put(HTTP_PATH, amsLocalAuthPath);
        headers.put(X_TENANT_IDENTIFIER_HEADER, e.getProperty(TENANT_ID));
        headers.put("Content-Type", "application/x-www-form-urlencoded");

        Map<String, String> queryMap = new LinkedHashMap<>();
        queryMap.put("grant_type", "password");
        queryMap.put("username", e.getProperty(LOGIN_USERNAME, String.class));
        queryMap.put("password", Base64.getEncoder().encodeToString(e.getProperty(LOGIN_PASSWORD, String.class).getBytes()));
        headers.put(CxfConstants.CAMEL_CXF_RS_QUERY_MAP, queryMap);

        cxfrsUtil.sendInOut("cxfrs:bean:ams.local.auth", e, headers, null);
    }
}