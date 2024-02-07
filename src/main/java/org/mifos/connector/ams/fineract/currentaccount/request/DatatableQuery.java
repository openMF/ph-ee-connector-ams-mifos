package org.mifos.connector.ams.fineract.currentaccount.request;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(fluent = true)
public class DatatableQuery {
    public String table;
    public Query query;
}
