package org.mifos.connector.ams.zeebe.workers.utils;

import java.time.LocalDateTime;

public record TransactionDetails(Long client_id, Long amsTransactionId, String internalCorrelationId, String camt052, LocalDateTime created_at, LocalDateTime updated_at) {
}
