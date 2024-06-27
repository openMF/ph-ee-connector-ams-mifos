package org.mifos.connector.ams.rest.authorize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;


@RestController
@RequestMapping("/api")
public class RestEndpoints {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @PostMapping("/authorize")
    public AuthorizeResponse authorize(@RequestBody AuthorizeRequest request) {
        logger.trace("Authorize request: {}", request);

        FineractAuthorizeRequest fineractRequest = new FineractAuthorizeRequest(request.transactionAmount, request.originalAmount, request.sequenceDateTime, request.dateTimeFormat);
        logger.trace("fineract request: {}", fineractRequest);

        FineractAuthorizeResponse fineractResponse = call(fineractRequest);
        AuthorizeResponse response = new AuthorizeResponse();
        logger.trace("fineract response: {}", response);
        return response;
    }

    private FineractAuthorizeResponse call(FineractAuthorizeRequest fineractRequest) {
        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<FineractAuthorizeRequest> requestEntity = new HttpEntity<>(fineractRequest, headers);

        ResponseEntity<FineractAuthorizeResponse> response = restTemplate.exchange("http://localhost:8081/api/fineract/authorize", HttpMethod.POST, requestEntity, FineractAuthorizeResponse.class);
        return response.getBody();
    }
}
