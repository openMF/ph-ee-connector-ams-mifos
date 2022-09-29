package org.mifos.connector.ams.zeebe.workers;

import org.springframework.stereotype.Component;
// import org.springframework.web.bind.annotation.GetMapping;

import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

// @Path("${data-table-query-api.url}")
@Component
public interface AmsDataTableQueryApi {

    // @GetMapping("${data-table-query-api.endpoint}")
    public AmsDataTableQueryResponse queryDataTable(@QueryParam("dataTableId") String myGreatDataTable,
                                 @QueryParam("dataTableFilterColumnName") String dataTableFilterColumnName,
                                 @QueryParam("filterValue") String filterValue,
                                 @QueryParam("resultColumns") String resultColoumns);
}
