package org.mifos.connector.ams.quote;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.mifos.phee.common.ams.dto.LoginFineractXResponseDTO;
import org.springframework.stereotype.Component;

@Component
public class FspLoginResponseProcessor implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        LoginFineractXResponseDTO loginResponse = exchange.getIn().getBody(LoginFineractXResponseDTO.class);
    }
}
