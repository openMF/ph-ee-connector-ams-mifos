package org.mifos.connector.ams.camel.local_quote;

import org.apache.camel.LoggingLevel;
import org.apache.camel.Processor;
import org.apache.camel.model.dataformat.JsonLibrary;

import org.mifos.common.ams.dto.QuoteFspRequestDTO;
import org.mifos.common.ams.dto.QuoteFspResponseDTO;
import org.mifos.common.camel.ErrorHandlerRouteBuilder;
import org.mifos.common.mojaloop.dto.FspMoneyData;
import org.mifos.common.mojaloop.dto.TransactionType;
import org.mifos.common.mojaloop.type.AmountType;
import org.mifos.common.mojaloop.type.InitiatorType;
import org.mifos.common.mojaloop.type.Scenario;
import org.mifos.common.mojaloop.type.TransactionRole;
import org.mifos.connector.ams.camel.config.CamelProperties;
import org.mifos.connector.ams.camel.cxfrs.CxfrsUtil;
import org.mifos.connector.ams.tenant.TenantService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.mifos.connector.ams.camel.cxfrs.HeaderBasedInterceptor.CXF_TRACE_HEADER;


@Component
public class LocalQuoteRouteBuilder extends ErrorHandlerRouteBuilder {

    public static final String TENANT_ID_PROPERTY = "TENANT_ID";

    @Value("${ams.local.base-url}")
    private String fpsLocalBaseUrl;

    @Value("${ams.local.quote-path}")
    private String fpsLocalQuotePath;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private LocalQuoteResponseProcessor localQuoteResponseProcessor;

    @Autowired
    private Processor pojoToString;

    @Autowired
    private CxfrsUtil cxfrsUtil;

    public LocalQuoteRouteBuilder() {
        super.configure();
    }

    @Override
    public void configure() {
        from("direct:send-local-quote")
                .id("send-local-quote")
                .log(LoggingLevel.INFO, "Sending local quote request")
                .setBody(exchange -> {
                    TransactionType transactionType = new TransactionType();
                    transactionType.setInitiator(TransactionRole.PAYER);
                    transactionType.setInitiatorType(InitiatorType.CONSUMER);
                    transactionType.setScenario(Scenario.PAYMENT);

                    String requestCode = UUID.randomUUID().toString();
                    String quoteId = UUID.randomUUID().toString();
                    String accountId = UUID.randomUUID().toString();

                    FspMoneyData amount = new FspMoneyData(BigDecimal.TEN, "USD");
                    QuoteFspRequestDTO request = new QuoteFspRequestDTO(exchange.getProperty(CamelProperties.TRANSACTION_ID, String.class),
                            requestCode,
                            quoteId,
                            accountId,
                            amount,
                            AmountType.SEND,
                            TransactionRole.PAYER,
                            transactionType);

                    exchange.setProperty(TENANT_ID_PROPERTY, "default");
                    return request;
                })
                .process(pojoToString)
                .process(e -> {
                    Map<String, Object> headers = new HashMap<>();
                    headers.put(CXF_TRACE_HEADER, true);
                    headers.put("CamelHttpMethod", "POST");
                    headers.put("CamelHttpPath", fpsLocalBaseUrl + fpsLocalQuotePath);
                    headers.put("Content-Type", "application/json");
                    headers.putAll(tenantService.getHeaders(e.getProperty(TENANT_ID_PROPERTY, String.class), true));
                    cxfrsUtil.sendInOut("cxfrs:bean:ams.local", e, headers, e.getIn().getBody());
                })
                .unmarshal().json(JsonLibrary.Jackson, QuoteFspResponseDTO.class)
                .process(localQuoteResponseProcessor);
    }
}