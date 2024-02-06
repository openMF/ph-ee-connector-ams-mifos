package org.mifos.connector.ams.fineract.currentaccount.request;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.ArrayList;

@Data
@Accessors(fluent = true)
public class FineractCurrentAccountRequest {
    public Request request;
    public String locale;
    public int page;
    public int size;
    public ArrayList<Object> sorts;
}
