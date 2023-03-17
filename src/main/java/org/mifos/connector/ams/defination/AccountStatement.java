package org.mifos.connector.ams.defination;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.mifos.connector.ams.model.StatusResponseDTO;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;

public interface AccountStatement {
    @GetMapping("/ams/accounts/{IdentifierType}/{IdentifierId}/statemententries")
     String accountStatement(@PathVariable(value = "IdentifierType") String IdentifierType,
                                                @PathVariable(value = "IdentifierId") String IdentifierId,
                                                @RequestHeader(value = "Platform-TenantId") String tenantId) throws JsonProcessingException;
    }
