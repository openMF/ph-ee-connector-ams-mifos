package org.mifos.connector.ams.zeebe.workers.utils;

public record GetSavingsAccountsAccountIdResponse(Integer id, GetSavingsStatus status, GetSavingsCurrency currency) {
}
