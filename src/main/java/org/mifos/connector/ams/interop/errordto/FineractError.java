package org.mifos.connector.ams.interop.errordto;

import java.util.ArrayList;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class FineractError {

    public String developerMessage;
    public String defaultUserMessage;
    public String userMessageGlobalisationCode;
    public String parameterName;
    public String value;
    public ArrayList<Args> args;

}
