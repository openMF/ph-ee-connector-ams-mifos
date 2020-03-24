package org.mifos.connector.ams.interop;

import org.apache.camel.Exchange;
import org.apache.camel.component.cxf.common.message.CxfConstants;
import org.mifos.connector.ams.camel.cxfrs.CxfrsUtil;
import org.mifos.connector.ams.tenant.TenantService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.mifos.connector.ams.camel.config.CamelProperties.PARTY_IDENTIFIER_FOR_EXT_ACC;
import static org.mifos.connector.ams.camel.config.CamelProperties.PARTY_ID_TYPE_FOR_EXT_ACC;
import static org.mifos.connector.ams.camel.config.CamelProperties.TENANT_ID;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSFER_ACTION;
import static org.mifos.connector.ams.camel.cxfrs.HeaderBasedInterceptor.CXF_TRACE_HEADER;

@Component
@ConditionalOnExpression("${ams.local.enabled}")
public class AmsService {

    @Value("${ams.local.interop-quotes-path}")
    private String amsInteropQuotesPath;

    @Value("${ams.local.interop-parties-path}")
    private String amsInteropPartiesPath;

    @Value("${ams.local.interop-transfers-path}")
    private String amsInteropTransfersPath;

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
        headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID, String.class)));
        cxfrsUtil.sendInOut("cxfrs:bean:ams.local", e, headers, e.getIn().getBody());
    }

    public void getExternalAccount(Exchange e) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(CXF_TRACE_HEADER, true);
        headers.put("CamelHttpMethod", "GET");
        headers.put("CamelHttpPath", amsInteropPartiesPath.replace("{idType}", e.getProperty(PARTY_ID_TYPE_FOR_EXT_ACC, String.class))
                .replace("{idValue}", e.getProperty(PARTY_IDENTIFIER_FOR_EXT_ACC, String.class)));
        headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID, String.class)));
        cxfrsUtil.sendInOut("cxfrs:bean:ams.local", e, headers, null);
    }

    public void sendTransfer(Exchange e) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(CXF_TRACE_HEADER, true);
        headers.put("CamelHttpMethod", "POST");
        headers.put("CamelHttpPath", amsInteropTransfersPath);

        Map<String, String> queryMap = new LinkedHashMap<>();
        queryMap.put("action", e.getProperty(TRANSFER_ACTION, String.class));
        headers.put(CxfConstants.CAMEL_CXF_RS_QUERY_MAP, queryMap);
        headers.put("Content-Type", "application/json");
        headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID, String.class)));
        cxfrsUtil.sendInOut("cxfrs:bean:ams.local", e, headers, e.getIn().getBody());
    }
}