package org.mifos.connector.ams.interop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.doAnswer;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class LoanAccountLookupTest {

    private CamelContext camelContext;
    private AmsService amsService;
    private LoanAccountLookup loanAccountLookup;

    @BeforeEach
    public void setUp() throws Exception {
        camelContext = new DefaultCamelContext();
        camelContext.start();
        amsService = Mockito.mock(AmsService.class);
        loanAccountLookup = new LoanAccountLookup(amsService);
    }

    @AfterEach
    public void tearDown() throws Exception {
        camelContext.stop();
    }

    @Test
    public void checkStatusMapsLoanStatusCode() {
        stubLoan("{"
                + "\"pageItems\":[{\"status\":{\"code\":\"loanStatusType.active\",\"value\":\"Active\"}}]"
                + "}");

        Exchange exchange = new DefaultExchange(camelContext);
        loanAccountLookup.checkStatus(exchange);

        JSONObject response = new JSONObject(exchange.getIn().getBody(String.class));
        assertEquals("loanStatusType.active", response.getString("accountStatus"));
        assertEquals("", response.getString("subStatus"));
        assertEquals("", response.getString("lei"));
        assertEquals(200, exchange.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class));
    }

    @Test
    public void checkStatusReturns404WhenLoanListIsEmpty() {
        stubLoan("{\"pageItems\":[]}");

        Exchange exchange = new DefaultExchange(camelContext);
        loanAccountLookup.checkStatus(exchange);

        assertEquals(404, exchange.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class));
        assertEquals("{\"pageItems\":[]}", exchange.getIn().getBody(String.class));
    }

    @Test
    public void checkStatusReturns404WhenFineractFails() {
        String errorBody = "{\"httpStatusCode\":\"404\"}";
        doAnswer(invocation -> {
            Exchange exchange = invocation.getArgument(0);
            exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, 404);
            exchange.getIn().setBody(errorBody);
            return null;
        }).when(amsService).getLoan(Mockito.any(Exchange.class));

        Exchange exchange = new DefaultExchange(camelContext);
        loanAccountLookup.checkStatus(exchange);

        assertEquals(404, exchange.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class));
        assertEquals(errorBody, exchange.getIn().getBody(String.class));
    }

    @Test
    public void checkNameUsesClientName() {
        stubLoan("{\"pageItems\":[{\"clientName\":\"Jane Borrower\"}]}");

        Exchange exchange = new DefaultExchange(camelContext);
        loanAccountLookup.checkName(exchange);

        JSONObject response = new JSONObject(exchange.getIn().getBody(String.class));
        assertEquals("Jane Borrower", response.getJSONObject("name").getString("fullName"));
        assertEquals("Jane Borrower", response.getJSONObject("name").getString("nativeName"));
        assertEquals("", response.getString("lei"));
    }

    @Test
    public void checkBalanceMapsSummaryWhenPresent() {
        stubLoan("{"
                + "\"pageItems\":[{"
                + "\"status\":{\"code\":\"loanStatusType.active\"},"
                + "\"currency\":{\"code\":\"KES\"},"
                + "\"summary\":{\"totalOutstanding\":11.50}"
                + "}]"
                + "}");

        Exchange exchange = new DefaultExchange(camelContext);
        loanAccountLookup.checkBalance(exchange);

        JSONObject response = new JSONObject(exchange.getIn().getBody(String.class));
        assertEquals(11.50, response.getBigDecimal("totalOutstanding").doubleValue());
        assertEquals(11.50, response.getBigDecimal("currentBalance").doubleValue());
        assertEquals("KES", response.getString("currency"));
        assertEquals("loanStatusType.active", response.getString("accountStatus"));
    }

    @Test
    public void checkBalanceOmitsOutstandingWhenSummaryIsMissing() {
        stubLoan("{"
                + "\"pageItems\":[{"
                + "\"status\":{\"code\":\"loanStatusType.active\"},"
                + "\"currency\":{\"code\":\"KES\"}"
                + "}]"
                + "}");

        Exchange exchange = new DefaultExchange(camelContext);
        loanAccountLookup.checkBalance(exchange);

        JSONObject response = new JSONObject(exchange.getIn().getBody(String.class));
        assertFalse(response.has("totalOutstanding"));
        assertFalse(response.has("currentBalance"));
        assertEquals("KES", response.getString("currency"));
        assertEquals("loanStatusType.active", response.getString("accountStatus"));
    }

    private void stubLoan(String body) {
        doAnswer(invocation -> {
            Exchange exchange = invocation.getArgument(0);
            exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, 200);
            exchange.getIn().setBody(body);
            return null;
        }).when(amsService).getLoan(Mockito.any(Exchange.class));
    }
}
