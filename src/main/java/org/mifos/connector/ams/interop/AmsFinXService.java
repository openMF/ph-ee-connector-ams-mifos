package org.mifos.connector.ams.interop;

import org.apache.camel.Exchange;
import org.mifos.connector.ams.camel.cxfrs.CxfrsUtil;
import org.mifos.connector.ams.tenant.TenantService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import static org.apache.camel.Exchange.HTTP_METHOD;
import static org.apache.camel.Exchange.HTTP_PATH;
import static org.mifos.connector.ams.camel.config.CamelProperties.CLIENT_ID;
import static org.mifos.connector.ams.camel.cxfrs.HeaderBasedInterceptor.CXF_TRACE_HEADER;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.EXTERNAL_ACCOUNT_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TENANT_ID;

@Component
@ConditionalOnExpression("${ams.local.enabled} && '${ams.local.version}'.equals('1.2')")
public class AmsFinXService extends AmsCommonService implements AmsService {

    @Value("${ams.local.interop.accounts-path}")
    private String amsInteropAccountsPath;

    @Value("${ams.local.customer.path}")
    private String amsClientsPath;

    @Value("${ams.local.account.savingsaccounts-path}")
    private String amsSavingsAccountsPath;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private CxfrsUtil cxfrsUtil;

    public void getSavingsAccount(Exchange e) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(CXF_TRACE_HEADER, true);
        headers.put(HTTP_METHOD, "GET");
        headers.put(HTTP_PATH, amsInteropAccountsPath.replace("{externalAccountId}", e.getProperty(EXTERNAL_ACCOUNT_ID, String.class)));
        headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID, String.class)));
        cxfrsUtil.sendInOut("cxfrs:bean:ams.local.interop", e, headers, null);
    }

    public void getSavingsAccountDefiniton(Exchange e) {
        throw new RuntimeException("getSavingsAccountDefiniton not implemented for FineractX");
    }

    public void getSavingsAccounts(Exchange e) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(CXF_TRACE_HEADER, true);
        headers.put(HTTP_METHOD, "GET");
        headers.put(HTTP_PATH, amsSavingsAccountsPath);
        headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID, String.class)));
        cxfrsUtil.sendInOut("cxfrs:bean:ams.local.account", e, headers, null);
    }

    public void getClient(Exchange e) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(CXF_TRACE_HEADER, true);
        headers.put(HTTP_METHOD, "GET");
        headers.put(HTTP_PATH, amsClientsPath.replace("{clientId}", e.getProperty(CLIENT_ID, String.class)));
        headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID, String.class)));
        cxfrsUtil.sendInOut("cxfrs:bean:ams.local.customer", e, headers, null);
    }

    public void login(Exchange e) {
        // basic auth
    }
}