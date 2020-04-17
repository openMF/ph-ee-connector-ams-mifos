package org.mifos.connector.ams.zeebe;

import org.mifos.phee.common.ams.dto.TransferActionType;

import java.util.HashMap;
import java.util.Map;

public class ZeebeExpressionVariables {

    public static final Map<String, String> ACTION_FAILURE_MAP = new HashMap<>();

    public static final String LOCAL_QUOTE_FAILED = "localQuoteFailed";
    public static final String QUOTE_FAILED = "quoteFailed";
    public static final String TRANSFER_PREPARE_FAILED = "transferPrepareFailed";
    public static final String TRANSFER_CREATE_FAILED = "transferCreateFailed";
    public static final String TRANSFER_RELEASE_FAILED = "transferReleaseFailed";

    static {
        ACTION_FAILURE_MAP.put(TransferActionType.PREPARE.name(), TRANSFER_PREPARE_FAILED);
        ACTION_FAILURE_MAP.put(TransferActionType.CREATE.name(), TRANSFER_CREATE_FAILED);
        ACTION_FAILURE_MAP.put(TransferActionType.RELEASE.name(), TRANSFER_RELEASE_FAILED);
    }

    private ZeebeExpressionVariables() {}
}
