package org.mifos.connector.ams.tenant;

public class TenantNotExistException extends RuntimeException {

    public TenantNotExistException(String message) {
        super(message);
    }
}
