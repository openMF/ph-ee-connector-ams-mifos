package org.mifos.connector.ams.zeebe;

import org.mifos.connector.common.ams.dto.TransferActionType;

import java.util.HashMap;
import java.util.Map;

public class ZeebeVariables {

    public static final Map<String, String> ACTION_FAILURE_MAP = new HashMap<>();

    public static final String ACCOUNT = "account";
    public static final String ACCOUNT_CURRENCY = "accountCurrency";
    public static final String ACCOUNT_ID = "accountId";
    public static final String CHANNEL_REQUEST = "channelRequest";
    public static final String ERROR_INFORMATION = "errorInformation";
    public static final String EXTERNAL_ACCOUNT_ID = "externalAccountId";
    public static final String LOCAL_QUOTE_FAILED = "localQuoteFailed";
    public static final String LOCAL_QUOTE_RESPONSE = "localQuoteResponse";
    public static final String PARTY_ID = "partyId";
    public static final String PARTY_ID_TYPE = "partyIdType";
    public static final String PAYEE_PARTY_RESPONSE = "payeePartyResponse";
    public static final String QUOTE_FAILED = "quoteFailed";
    public static final String QUOTE_SWITCH_REQUEST = "quoteSwitchRequest";
    public static final String TENANT_ID = "tenantId";
    public static final String TRANSACTION_ID = "transactionId";
    public static final String TRANSFER_CODE = "transferCode";
    public static final String TRANSFER_CREATE_FAILED = "transferCreateFailed";
    public static final String TRANSFER_PREPARE_FAILED = "transferPrepareFailed";
    public static final String TRANSFER_RELEASE_FAILED = "transferReleaseFailed";
    public static final String TRANSFER_RESPONSE_PREFIX = "transferResponse";

    static {
        ACTION_FAILURE_MAP.put(TransferActionType.PREPARE.name(), TRANSFER_PREPARE_FAILED);
        ACTION_FAILURE_MAP.put(TransferActionType.CREATE.name(), TRANSFER_CREATE_FAILED);
        ACTION_FAILURE_MAP.put(TransferActionType.RELEASE.name(), TRANSFER_RELEASE_FAILED);
    }

    private ZeebeVariables() {}
}
