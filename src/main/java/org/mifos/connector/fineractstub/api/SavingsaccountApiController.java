package org.mifos.connector.fineractstub.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Schema;
import java.io.IOException;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import org.mifos.connector.fineractstub.model.InteropIdentifier;
import org.mifos.connector.fineractstub.model.InteropTransfers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@SuppressWarnings("checkstyle:Dynamic")
@javax.annotation.Generated(value = "io.swagger.codegen.v3.generators.java.SpringCodegen", date = "2023-08-10T10:13:07.472376795Z[GMT]")
@RestController
public class SavingsaccountApiController implements SavingsaccountApi {

    private static final Logger log = LoggerFactory.getLogger(SavingsaccountApiController.class);

    private final ObjectMapper objectMapper;

    private final HttpServletRequest request;

    @org.springframework.beans.factory.annotation.Autowired
    public SavingsaccountApiController(ObjectMapper objectMapper, HttpServletRequest request) {
        this.objectMapper = objectMapper;
        this.request = request;
    }

    @Override
    public Optional<ObjectMapper> getObjectMapper() {
        return Optional.ofNullable(objectMapper);
    }

    @Override
    public Optional<HttpServletRequest> getRequest() {
        return Optional.ofNullable(request);
    }

    public ResponseEntity<InteropIdentifier> getSavingsAccount() {
        String accept = request.getHeader("Accept");
        try {
            return new ResponseEntity<InteropIdentifier>(objectMapper.readValue(
                    "{\n  \"accountId\" : \"cde3e5ee-214b-423f-97b0-0a0206aecaaf\",\n  \"resourceId\" : \"1\",\n  \"resourceIdentifier\" : \"1\"\n}",
                    InteropIdentifier.class), HttpStatus.OK);
        } catch (IOException e) {
            log.error("Couldn't serialize response for content type application/json", e);
            return new ResponseEntity<InteropIdentifier>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    public ResponseEntity<Void> transfers(
            @Parameter(in = ParameterIn.DEFAULT, description = "post savings Account transfers", schema = @Schema()) @Valid @RequestBody InteropTransfers body) {
        String accept = request.getHeader("Accept");
        return new ResponseEntity<Void>(HttpStatus.OK);
    }

}
