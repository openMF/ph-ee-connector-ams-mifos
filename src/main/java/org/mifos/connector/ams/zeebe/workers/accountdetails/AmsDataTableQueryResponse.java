package org.mifos.connector.ams.zeebe.workers.accountdetails;

public record AmsDataTableQueryResponse(Long conversion_account_id, Long disposal_account_id, Long internal_account_id) {
}
