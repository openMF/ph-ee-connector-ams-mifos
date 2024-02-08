package org.mifos.connector.ams.fineract.currentaccount.response;


import lombok.Data;

import java.util.List;

@Data
public class IdentifiersResponse {
    List<Identifier> primaryIdentifiers;
    List<Identifier> secondaryIdentifiers;

    @Data
    public class Identifier {
        private String idType;
        private String value;
        private String subValue;

    }

}

