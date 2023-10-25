package org.mifos.connector.ams.interop.errordto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.ArrayList;

@Getter
@Setter
@ToString
public class ErrorResponse {

    public String developerMessage;
    public String httpStatusCode;
    public String defaultUserMessage;
    public String userMessageGlobalisationCode;
    public ArrayList<FineractError> errors;

}
