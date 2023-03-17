package org.mifos.connector.ams.defination;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.mifos.connector.ams.model.BalanceResponseDTO;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;

import java.util.concurrent.ExecutionException;

public interface AccountBalance {

    @GetMapping("/ams/accounts/{IdentifierType}/{IdentifierId}/balance")
    BalanceResponseDTO accountBalance(@PathVariable(value = "IdentifierType") String IdentifierType,
                                      @PathVariable(value = "IdentifierId") String IdentifierId,
                                      @RequestHeader(value = "Platform-TenantId") String tenantId) throws JsonProcessingException;
}
