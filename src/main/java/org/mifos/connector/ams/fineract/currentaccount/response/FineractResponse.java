package org.mifos.connector.ams.fineract.currentaccount.response;

import com.fasterxml.jackson.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

@JsonPropertyOrder({
        "status_type",
        "account_no",
        "external_id",
        "id",
        "account_flag_list_cd_flag_code",
        "product_id"
})
public class FineractResponse {

    @JsonIgnore
    private final Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();
    @JsonProperty("status_type")
    private String statusType;
    @JsonProperty("account_no")
    private String accountNo;
    @JsonProperty("external_id")
    private String externalId;
    @JsonProperty("id")
    private String id;
    @JsonProperty("account_flag_list_cd_flag_code")
    private String flagCode;
    @JsonProperty("product_id")
    private String productId;

    @JsonProperty("product_id")
    public String getProductId() {
        return productId;
    }

    @JsonProperty("product_id")
    public void setProductId(String productId) {
        this.productId = productId;
    }

    @JsonProperty("status_type")
    public String getStatusType() {
        return statusType;
    }

    @JsonProperty("status_type")
    public void setStatusType(String statusType) {
        this.statusType = statusType;
    }

    @JsonProperty("account_no")
    public String getAccountNo() {
        return accountNo;
    }

    @JsonProperty("account_no")
    public void setAccountNo(String accountNo) {
        this.accountNo = accountNo;
    }

    @JsonProperty("external_id")
    public String getExternalId() {
        return externalId;
    }

    @JsonProperty("external_id")
    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }

    @JsonProperty("id")
    public String getId() {
        return id;
    }

    @JsonProperty("id")
    public void setId(String id) {
        this.id = id;
    }

    @JsonProperty("account_flag_list_cd_flag_code")
    public String getFlagCode() {
        return flagCode;
    }

    @JsonProperty("account_flag_list_cd_flag_code")
    public void setFlagCode(String flagCode) {
        this.flagCode = flagCode;
    }


    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

}
