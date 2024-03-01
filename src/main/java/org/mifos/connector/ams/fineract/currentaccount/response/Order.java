package org.mifos.connector.ams.fineract.currentaccount.response;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.LinkedHashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "direction",
        "property",
        "ignoreCase",
        "nullHandling"
})

public class Order {

    @JsonProperty("direction")
    private String direction;
    @JsonProperty("property")
    private String property;
    @JsonProperty("ignoreCase")
    private Boolean ignoreCase;
    @JsonProperty("nullHandling")
    private String nullHandling;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();

    @JsonProperty("direction")
    public String getDirection() {
        return direction;
    }

    @JsonProperty("direction")
    public void setDirection(String direction) {
        this.direction = direction;
    }

    @JsonProperty("property")
    public String getProperty() {
        return property;
    }

    @JsonProperty("property")
    public void setProperty(String property) {
        this.property = property;
    }

    @JsonProperty("ignoreCase")
    public Boolean getIgnoreCase() {
        return ignoreCase;
    }

    @JsonProperty("ignoreCase")
    public void setIgnoreCase(Boolean ignoreCase) {
        this.ignoreCase = ignoreCase;
    }

    @JsonProperty("nullHandling")
    public String getNullHandling() {
        return nullHandling;
    }

    @JsonProperty("nullHandling")
    public void setNullHandling(String nullHandling) {
        this.nullHandling = nullHandling;
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
