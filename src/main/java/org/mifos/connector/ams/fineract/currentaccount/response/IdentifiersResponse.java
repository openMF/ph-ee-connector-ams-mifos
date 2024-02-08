package org.mifos.connector.ams.fineract.currentaccount.response;


import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class IdentifiersResponse {
    private List<Identifier> primaryIdentifiers;
    private List<Identifier> secondaryIdentifiers;
}

