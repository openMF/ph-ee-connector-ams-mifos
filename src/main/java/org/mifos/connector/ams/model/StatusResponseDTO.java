package org.mifos.connector.ams.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class StatusResponseDTO {
    private String accountStatus;
    private String subStatus;
    private String lei;

}
