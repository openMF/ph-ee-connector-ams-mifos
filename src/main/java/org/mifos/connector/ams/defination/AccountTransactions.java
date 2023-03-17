package org.mifos.connector.ams.defination;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.mifos.connector.ams.model.StatusResponseDTO;
import org.mifos.connector.ams.model.TransactionsResponseDTO;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;

public interface AccountTransactions {
    @GetMapping("/ams/accounts/{IdentifierType}/{IdentifierId}/transactions")
    TransactionsResponseDTO accountTransaction(@PathVariable(value = "IdentifierType") String IdentifierType,
                                               @PathVariable(value = "IdentifierId") String IdentifierId,
                                               @RequestHeader(value = "Platform-TenantId") String tenantId) throws JsonProcessingException;
}
