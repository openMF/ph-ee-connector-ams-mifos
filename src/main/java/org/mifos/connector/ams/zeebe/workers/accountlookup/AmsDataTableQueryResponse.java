package org.mifos.connector.ams.zeebe.workers.accountlookup;

public record AmsDataTableQueryResponse(int accountAmsStatus, String fiat_account_id, String ecurrency_account_id) {
}
