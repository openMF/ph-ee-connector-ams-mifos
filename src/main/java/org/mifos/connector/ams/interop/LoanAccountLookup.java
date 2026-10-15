package org.mifos.connector.ams.interop;

import org.apache.camel.Exchange;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class LoanAccountLookup {

    private static final int MAX_SUCCESS_STATUS = 202;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final AmsService amsService;

    @Autowired
    public LoanAccountLookup(AmsService amsService) {
        this.amsService = amsService;
    }

    public void checkStatus(Exchange exchange) {
        JSONObject loan = loadLoan(exchange);
        if (loan == null) {
            return;
        }
        JSONObject response = new JSONObject();
        response.put("accountStatus", statusCode(loan));
        response.put("subStatus", "");
        response.put("lei", "");
        exchange.getIn().setBody(response.toString());
    }

    public void checkName(Exchange exchange) {
        JSONObject loan = loadLoan(exchange);
        if (loan == null) {
            return;
        }
        String clientName = loan.optString("clientName", "");
        JSONObject name = new JSONObject();
        name.put("title", "");
        name.put("firstName", "");
        name.put("middleName", "");
        name.put("lastName", "");
        name.put("fullName", clientName);
        name.put("nativeName", clientName);
        JSONObject response = new JSONObject();
        response.put("name", name);
        response.put("lei", "");
        exchange.getIn().setBody(response.toString());
    }

    public void checkBalance(Exchange exchange) {
        JSONObject loan = loadLoan(exchange);
        if (loan == null) {
            return;
        }
        JSONObject response = new JSONObject();
        JSONObject summary = loan.optJSONObject("summary");
        if (summary != null && summary.has("totalOutstanding")) {
            response.put("totalOutstanding", summary.getBigDecimal("totalOutstanding"));
            response.put("currentBalance", summary.getBigDecimal("totalOutstanding"));
        }
        JSONObject currency = loan.optJSONObject("currency");
        if (currency != null) {
            response.put("currency", currency.optString("code", ""));
        }
        response.put("accountStatus", statusCode(loan));
        exchange.getIn().setBody(response.toString());
    }

    private JSONObject loadLoan(Exchange exchange) {
        amsService.getLoan(exchange);
        Integer statusCode = exchange.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class);
        String body = exchange.getIn().getBody(String.class);
        if (statusCode != null && statusCode > MAX_SUCCESS_STATUS) {
            exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, 404);
            return null;
        }
        if (body == null || body.trim().isEmpty()) {
            exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, 404);
            return null;
        }
        JSONObject payload;
        try {
            payload = new JSONObject(body);
        } catch (RuntimeException exception) {
            logger.debug("Loan account response was not a JSON object: {}", exception.getMessage());
            exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, 404);
            return null;
        }
        JSONArray pageItems = payload.optJSONArray("pageItems");
        if (pageItems == null || pageItems.isEmpty()) {
            exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, 404);
            return null;
        }
        return pageItems.getJSONObject(0);
    }

    private String statusCode(JSONObject loan) {
        JSONObject status = loan.optJSONObject("status");
        if (status == null) {
            return "";
        }
        return status.optString("code", "");
    }
}
