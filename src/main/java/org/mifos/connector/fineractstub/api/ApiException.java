package org.mifos.connector.fineractstub.api;

@SuppressWarnings("checkstyle:Dynamic")
@javax.annotation.Generated(value = "io.swagger.codegen.v3.generators.java.SpringCodegen", date = "2023-08-10T10:13:07.472376795Z[GMT]")
public class ApiException extends Exception {

    private final int code;

    public ApiException(int code, String msg) {
        super(msg);
        this.code = code;
    }
}
