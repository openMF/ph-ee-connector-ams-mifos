package org.mifos.connector.ams.model;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;

@Getter
@Setter
public class TransactionsResponseDTO {
    ArrayList < Object > transactions = new ArrayList < Object > ();
    private float resourceId;
    Changes ChangesObject;
    private String resourceIdentifier;


    public float getResourceId() {
        return resourceId;
    }

    public Changes getChanges() {
        return ChangesObject;
    }

    public String getResourceIdentifier() {
        return resourceIdentifier;
    }

    // Setter Methods

    public void setResourceId(float resourceId) {
        this.resourceId = resourceId;
    }

    public void setChanges(Changes changesObject) {
        this.ChangesObject = changesObject;
    }

    public void setResourceIdentifier(String resourceIdentifier) {
        this.resourceIdentifier = resourceIdentifier;
    }
}
class Changes {
}
