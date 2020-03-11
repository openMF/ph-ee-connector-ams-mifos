package org.mifos.connector.ams.quote;

import org.apache.camel.Exchange;
import org.apache.camel.component.cxf.common.message.CxfConstants;
import org.json.JSONObject;
import org.mifos.connector.ams.camel.cxfrs.CxfrsUtil;
import org.mifos.connector.ams.tenant.TenantService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.mifos.connector.ams.camel.config.CamelProperties.EXTERNAL_ACCOUNT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.PAYER_PARTY_IDENTIFIER;
import static org.mifos.connector.ams.camel.config.CamelProperties.SAVINGS_ACCOUNT;
import static org.mifos.connector.ams.camel.config.CamelProperties.TENANT_ID;
import static org.mifos.connector.ams.camel.cxfrs.HeaderBasedInterceptor.CXF_TRACE_HEADER;

@Component
@ConditionalOnExpression("${ams.local.quote-enabled}")
public class AmsService {

    @Value("${ams.local.interop-quotes-path}")
    private String amsInteropQuotesPath;

    @Value("${ams.local.interop-parties-path}")
    private String amsInteropPartiesPath;

    @Value("${ams.local.accounts-path}")
    private String amsAccountsPath;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private CxfrsUtil cxfrsUtil;

    public void getLocalQuote(Exchange e) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(CXF_TRACE_HEADER, true);
        headers.put("CamelHttpMethod", "POST");
        headers.put("CamelHttpPath", amsInteropQuotesPath);
        headers.put("Content-Type", "application/json");
        headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID, String.class), true));
        cxfrsUtil.sendInOut("cxfrs:bean:ams.local", e, headers, e.getIn().getBody());
    }

    public void getSavingsAccount(Exchange e) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(CXF_TRACE_HEADER, true);
        headers.put("CamelHttpMethod", "GET");
        headers.put("CamelHttpPath", amsAccountsPath);
        headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID, String.class), true));

        Map<String, String> queryMap = new LinkedHashMap<>();
        queryMap.put("externalId", e.getProperty(EXTERNAL_ACCOUNT_ID, String.class));
        headers.put(CxfConstants.CAMEL_CXF_RS_QUERY_MAP, queryMap);

        cxfrsUtil.sendInOut("cxfrs:bean:ams.local", e, headers, null);
        JSONObject response = new JSONObject(e.getOut().getBody(String.class));
        if (response.getInt("totalFilteredRecords") != 1) {
            throw new RuntimeException("There are invalid number of accounts found for externalId!");
        } else {
            e.setProperty(SAVINGS_ACCOUNT, response.getJSONArray("pageItems").getJSONObject(0));
        }
    }

    public void getExternalAccount(Exchange e) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(CXF_TRACE_HEADER, true);
        headers.put("CamelHttpMethod", "GET");
        headers.put("CamelHttpPath", amsInteropPartiesPath.replace("{idType}", "MSISDN") // TODO when Mojaloop implement other partyIdType's change this
                .replace("{idValue}", e.getProperty(PAYER_PARTY_IDENTIFIER, String.class)));
        headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID, String.class), true));
        cxfrsUtil.sendInOut("cxfrs:bean:ams.local", e, headers, null);
    }
}