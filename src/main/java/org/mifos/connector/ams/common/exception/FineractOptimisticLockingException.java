package org.mifos.connector.ams.common.exception;

public class FineractOptimisticLockingException extends RuntimeException {
    public FineractOptimisticLockingException() {
    }

    public FineractOptimisticLockingException(String message) {
        super(message);
    }

    public FineractOptimisticLockingException(String message, Throwable cause) {
        super(message, cause);
    }

    public FineractOptimisticLockingException(Throwable cause) {
        super(cause);
    }

    public FineractOptimisticLockingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
