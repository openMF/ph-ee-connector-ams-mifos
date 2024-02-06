package org.mifos.connector.ams.fineract.currentaccount.request;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Set;

@Data
@Accessors(fluent = true)
public class Query {
    public Set<String> resultColumns;
}
