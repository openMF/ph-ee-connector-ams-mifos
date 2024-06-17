package org.mifos.connector.ams.rest.authorize;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
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
import java.util.Random;


@RestController
@RequestMapping("/api")
public class AuthorizeEndpoint {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${fineract.authorize-url}")
    private String authorizeUrl;

    @Value("${fineract.authorize-stub-response:false}")
    private boolean authorizeStubResponse;


    @PostMapping("/authorize")
    public AuthorizeResponse authorize(@RequestBody AuthorizeRequest request) {
        logger.trace("authorize request: {}", request);

        FineractAuthorizeRequest fineractRequest = new FineractAuthorizeRequest(request.transactionAmount, request.originalAmount, request.sequenceDateTime, request.accId);
        logger.trace("fineract request: {}", fineractRequest);

        try {
            FineractAuthorizeResponse fineractResponse = call(fineractRequest);
            logger.trace("fineract response: {}", fineractResponse);

            AuthorizeResponse response = new AuthorizeResponse(fineractResponse);
            logger.trace("authorize response: {}", response);
            return response;

        } catch (Exception e) {
            logger.error("failed to call fineract", e);
            if (authorizeStubResponse) {
                AuthorizeResponse stubResponse = createStubResponse();
                logger.trace("stub response: {}", stubResponse);
                return stubResponse;
            } else throw e;
        }
    }

    private @NotNull AuthorizeResponse createStubResponse() {
        logger.warn("returning stub response because fineract stub response is enabled in the configuration");
        Random random = new Random();
        return new AuthorizeResponse(
                BigDecimal.valueOf(random.nextInt(10000)),
                BigDecimal.valueOf(random.nextInt(10000)),
                BigDecimal.valueOf(random.nextInt(10000)),
                BigDecimal.valueOf(random.nextInt(10000))
        );
    }

    private FineractAuthorizeResponse call(FineractAuthorizeRequest fineractRequest) {
        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<FineractAuthorizeRequest> requestEntity = new HttpEntity<>(fineractRequest, headers);

        ResponseEntity<FineractAuthorizeResponse> response = restTemplate.exchange(authorizeUrl, HttpMethod.POST, requestEntity, FineractAuthorizeResponse.class);
        return response.getBody();
    }
}
