package org.mifos.connector.ams.zeebe.workers.utils;

import org.springframework.http.HttpHeaders;

public record TransactionItem(Integer requestId, String relativeUrl, String method, Integer reference, HttpHeaders headers, String body) {
}
