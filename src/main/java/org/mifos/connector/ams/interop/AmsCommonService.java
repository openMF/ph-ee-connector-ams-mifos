package org.mifos.connector.ams.interop;

import org.apache.camel.Exchange;
import org.apache.camel.component.cxf.common.message.CxfConstants;
import org.mifos.connector.ams.camel.cxfrs.CxfrsUtil;
import org.mifos.connector.ams.tenant.TenantService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.camel.Exchange.*;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSFER_ACTION;
import static org.mifos.connector.ams.camel.cxfrs.HeaderBasedInterceptor.CXF_TRACE_HEADER;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.ACCOUNT_NUMBER;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.PARTY_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.PARTY_ID_TYPE;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TENANT_ID;

@Component
@ConditionalOnExpression("${ams.local.enabled}")
public class AmsCommonService {

    @Value("${ams.local.interop.quotes-path}")
    private String amsInteropQuotesPath;

    @Value("${ams.local.interop.parties-path}")
    private String amsInteropPartiesPath;

    @Value("${ams.local.interop.transfers-path}")
    private String amsInteropTransfersPath;

    @Value("${ams.local.loan.repayment-path}")
    private String amsLoanRepaymentPath;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private CxfrsUtil cxfrsUtil;
    @Autowired
    RestTemplate restTemplate;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String APPLICATION_TYPE  = "application/json";
    public void getLocalQuote(Exchange e) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(CXF_TRACE_HEADER, true);
        headers.put(HTTP_METHOD, "POST");
        headers.put(HTTP_PATH, amsInteropQuotesPath);
        headers.put("Content-Type", "application/json");
        headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID, String.class)));
        cxfrsUtil.sendInOut("cxfrs:bean:ams.local.interop", e, headers, e.getIn().getBody());
    }

    public void getExternalAccount(Exchange e) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(CXF_TRACE_HEADER, true);
        headers.put(HTTP_METHOD, "GET");
        headers.put(HTTP_PATH, amsInteropPartiesPath.replace("{idType}", e.getProperty(PARTY_ID_TYPE, String.class))
                .replace("{idValue}", e.getProperty(PARTY_ID, String.class)));
        headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID, String.class)));
        cxfrsUtil.sendInOut("cxfrs:bean:ams.local.interop", e, headers, null);
    }

    public void sendTransfer(Exchange e) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(CXF_TRACE_HEADER, true);
        headers.put(HTTP_METHOD, "POST");
        headers.put(HTTP_PATH, amsInteropTransfersPath);
        logger.info("Send Transfer Body: {}", e.getIn().getBody());
        Map<String, String> queryMap = new LinkedHashMap<>();
        queryMap.put("action", e.getProperty(TRANSFER_ACTION, String.class));
        headers.put(CxfConstants.CAMEL_CXF_RS_QUERY_MAP, queryMap);
        headers.put("Content-Type", "application/json");
        headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID, String.class)));
        cxfrsUtil.sendInOut("cxfrs:bean:ams.local.interop", e, headers, e.getIn().getBody());
    }
    public void repayLoan(Exchange e) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(CXF_TRACE_HEADER, true);
        headers.put(HTTP_METHOD, "POST");
        headers.put(HTTP_PATH, amsLoanRepaymentPath.replace("{accountNumber}", e.getProperty(ACCOUNT_NUMBER, String.class)));
        logger.debug("Loan Repayment Body: {}", e.getIn().getBody());
        headers.put("Content-Type", APPLICATION_TYPE);
        headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID, String.class)));
        cxfrsUtil.sendInOut("cxfrs:bean:ams.local.loan", e, headers, e.getIn().getBody());
    }
    public boolean sendCallback(String callbackURL, String body) {
        logger.info("Sending Callback...");
        ResponseEntity responseEntity;
        try{
            responseEntity =  restTemplate.postForEntity(callbackURL,body,null);
            if(responseEntity.getStatusCode().is2xxSuccessful()){
                logger.info("Callback sent");
                return true;
            }else{
                logger.info("Callback failed!!!");
                return false;
            }
        }catch (Exception exception){
            logger.info("Callback failed!!!");
            exception.printStackTrace();
        }
        return false;
    }


    public void registerInteropIdentifier(Exchange e) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(CXF_TRACE_HEADER, true);
        headers.put(HTTP_METHOD, "POST");
        headers.put(HTTP_PATH, amsInteropPartiesPath.replace("{idType}", e.getProperty(PARTY_ID_TYPE, String.class))
                .replace("{idValue}", e.getProperty(PARTY_ID, String.class)));
        headers.put("Content-Type", "application/json");
        headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID, String.class)));
        cxfrsUtil.sendInOut("cxfrs:bean:ams.local.interop", e, headers, e.getIn().getBody());
    }

    public void removeInteropIdentifier(Exchange e) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(CXF_TRACE_HEADER, true);
        headers.put(HTTP_METHOD, "DELETE");
        headers.put(HTTP_PATH, amsInteropPartiesPath.replace("{idType}", e.getProperty(PARTY_ID_TYPE, String.class))
                .replace("{idValue}", e.getProperty(PARTY_ID, String.class)));
        headers.put("Content-Type", "application/json");
        headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID, String.class)));
        e.getIn().setBody(null);
        cxfrsUtil.sendInOut("cxfrs:bean:ams.local.interop", e, headers, null);
    }
}