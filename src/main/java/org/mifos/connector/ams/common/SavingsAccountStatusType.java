package org.mifos.connector.ams.common;

public enum SavingsAccountStatusType {

    INVALID(0, "savingsAccountStatusType.invalid"), //
    SUBMITTED_AND_PENDING_APPROVAL(100, "savingsAccountStatusType.submitted.and.pending.approval"), //
    APPROVED(200, "savingsAccountStatusType.approved"), //
    ACTIVE(300, "savingsAccountStatusType.active"), //
    TRANSFER_IN_PROGRESS(303, "savingsAccountStatusType.transfer.in.progress"), //
    TRANSFER_ON_HOLD(304, "savingsAccountStatusType.transfer.on.hold"), //
    WITHDRAWN_BY_APPLICANT(400, "savingsAccountStatusType.withdrawn.by.applicant"), //
    REJECTED(500, "savingsAccountStatusType.rejected"), //
    CLOSED(600, "savingsAccountStatusType.closed"), PRE_MATURE_CLOSURE(700, "savingsAccountStatusType.pre.mature.closure"), MATURED(800,
            "savingsAccountStatusType.matured");

    private final Integer value;
    private final String code;

    SavingsAccountStatusType(final Integer value, final String code) {
        this.value = value;
        this.code = code;
    }

    public Integer getValue() {
        return this.value;
    }

    public String getCode() {
        return this.code;
    }
}