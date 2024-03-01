package org.mifos.connector.ams.fineract.currentaccount.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Identifier {
    private String idType;
    private String value;
    private String subValue;
}
