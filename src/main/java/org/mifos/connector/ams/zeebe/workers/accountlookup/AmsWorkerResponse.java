package org.mifos.connector.ams.zeebe.workers.accountlookup;

public record AmsWorkerResponse(AccountAmsStatus accountAmsStatus, String fiat_account_id, String ecurrency_account_id) {
}
