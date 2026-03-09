package org.mifos.connector.ams.interop.errordto;

import java.util.ArrayList;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

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
