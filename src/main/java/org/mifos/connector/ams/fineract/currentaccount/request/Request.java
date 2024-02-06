package org.mifos.connector.ams.fineract.currentaccount.request;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

@Data
@Accessors(fluent = true)
public class Request {
    public BaseQuery baseQuery;
    public List<DatatableQuery> datatableQueries;
}
