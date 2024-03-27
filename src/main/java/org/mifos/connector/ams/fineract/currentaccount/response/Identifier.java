package org.mifos.connector.ams.fineract.currentaccount.response;

import lombok.Data;

@Data
public class Identifier {
    private String idType;
    private String value;
    private String subValue;
}
