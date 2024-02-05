package org.mifos.connector.ams.zeebe.workers.utils;

import com.baasflow.commons.events.EventService;
import com.baasflow.commons.events.EventStatus;
import com.baasflow.commons.events.EventType;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Map;

@Component
public class NotificationHelper {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    EventService eventService;


    public void send(String event, BigDecimal amount, String currency, String debtorName, String paymentScheme, String iban) {
        String payload = new JSONObject()
                .put("amount", amount)
                .put("currency", currency)
                .put("debtorName", debtorName)
                .put("paymentScheme", paymentScheme)
                .put("iban", iban)
                .toString();

        eventService.sendEvent(
                "ams_connector",
                event,
                EventType.business,
                EventStatus.success,
                payload,
                null,
                Map.of());
    }
}
