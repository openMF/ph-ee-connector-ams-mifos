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
import static org.mifos.connector.ams.camel.config.CamelProperties.*;
import static org.mifos.connector.ams.camel.cxfrs.HeaderBasedInterceptor.CXF_TRACE_HEADER;
import static org.mifos.connector.ams.tenant.TenantService.X_TENANT_IDENTIFIER_HEADER;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.ACCOUNT_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TENANT_ID;

@Component
@ConditionalOnExpression("'${ams.local.version}'.equals('cn')")
public class AmsFinCNService extends AmsCommonService implements AmsService {

    @Value("${ams.local.account.instances-path}")
    private String amsAccountInstancesPath;

    @Value("${ams.local.account.definitons-path}")
    private String amsAccountDefinitionsPath;

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
        headers.put(HTTP_PATH, amsAccountInstancesPath.replace("{accountId}", e.getProperty(ACCOUNT_ID, String.class)));
        headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID, String.class)));
        cxfrsUtil.sendInOut("cxfrs:bean:ams.local.account", e, headers, null);
    }

    public void getSavingsAccountDefiniton(Exchange e) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(CXF_TRACE_HEADER, true);
        headers.put(HTTP_METHOD, "GET");
        headers.put(HTTP_PATH, amsAccountDefinitionsPath.replace("{definitionId}", e.getProperty(DEFINITON_ID, String.class)));
        headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID, String.class)));
        cxfrsUtil.sendInOut("cxfrs:bean:ams.local.account", e, headers, null);
    }

    public void getSavingsAccounts(Exchange e) {
        throw new RuntimeException("getSavingsAccounts not implemented for FineractCN");
    }

    public void getClient(Exchange e) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(CXF_TRACE_HEADER, true);
        headers.put(HTTP_METHOD, "GET");
        headers.put(HTTP_PATH, amsLocalCustomerPath.replace("{customerIdentifier}", e.getProperty(CLIENT_ID, String.class)));
        headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID, String.class)));
        cxfrsUtil.sendInOut("cxfrs:bean:ams.local.customer", e, headers, null);
    }

    public void getClientByMobileNo(Exchange e) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(CXF_TRACE_HEADER, true);
        headers.put(HTTP_METHOD, "GET");
        headers.put(HTTP_PATH, amsLocalCustomerPath.replace("{customerIdentifier}", e.getProperty(IDENTIFIER_ID, String.class)));
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

    public void getSavingsAccountsTransactions(Exchange e) {
        //need this to be filled

    }
}
