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
import static org.mifos.connector.ams.camel.config.CamelProperties.CLIENT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.EXTERNAL_ACCOUNT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.LOGIN_PASSWORD;
import static org.mifos.connector.ams.camel.config.CamelProperties.LOGIN_USERNAME;
import static org.mifos.connector.ams.camel.config.CamelProperties.TENANT_ID;
import static org.mifos.connector.ams.camel.cxfrs.HeaderBasedInterceptor.CXF_TRACE_HEADER;
import static org.mifos.connector.ams.tenant.TenantService.X_TENANT_IDENTIFIER_HEADER;

@Component
@ConditionalOnExpression("${ams.local.enabled} && '${ams.local.version}'.equals('cn')")
public class AmsFinCNService extends AmsCommonService implements AmsService {

    @Value("${ams.local.account.path}")
    private String amsLocalAccountPath;

    @Value("${ams.local.customer.path}")
    private String amsLocalCustomerPath;

    @Value("${ams.local.auth.path}")
    private String amsLocalAuthPath;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private CxfrsUtil cxfrsUtil;

    public void getSavingsAccount(Exchange e) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(CXF_TRACE_HEADER, true);
        headers.put(HTTP_METHOD, "GET");
        headers.put(HTTP_PATH, amsLocalAccountPath.replace("{accountId}", e.getProperty(EXTERNAL_ACCOUNT_ID, String.class)));
        headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID, String.class)));
        cxfrsUtil.sendInOut("cxfrs:bean:ams.local.account", e, headers, null);
    }

    public void getClient(Exchange e) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(CXF_TRACE_HEADER, true);
        headers.put(HTTP_METHOD, "GET");
        headers.put(HTTP_PATH, amsLocalCustomerPath.replace("{customerIdentifier}", e.getProperty(CLIENT_ID, String.class)));
        headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID, String.class)));
        cxfrsUtil.sendInOut("cxfrs:bean:ams.local.customer", e, headers, null);
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