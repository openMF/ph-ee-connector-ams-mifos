package org.mifos.connector.ams.zeebe.workers.utils;

import java.util.List;

public record TransactionItem(Integer requestId, String relativeUrl, String method, List<Header> headers, String body) {
}
