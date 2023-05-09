package org.mifos.connector.ams.zeebe;

import org.springframework.stereotype.Component;

@Component
public class ZeebeeWorkers {

    public static final String WORKER_PARTY_LOOKUP_LOCAL = "party-lookup-local-";
    public static final String WORKER_PAYEE_COMMIT_TRANSFER = "payee-commit-transfer-";
    public static final String WORKER_PAYEE_QUOTE = "payee-quote-";
    public static final String WORKER_PAYER_LOCAL_QUOTE = "payer-local-quote-";
    public static final String WORKER_INTEROP_PARTY_REGISTRATION = "interop-party-registration-";
    public static final String WORKER_PAYEE_DEPOSIT_TRANSFER = "payee-deposit-transfer-";
    public static final String WORKER_PARTY_ACCOUNT_LOOKUP = "party-account-lookup-";

}
