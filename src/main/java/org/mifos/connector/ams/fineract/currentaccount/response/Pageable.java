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
        "sort",
        "pageNumber",
        "pageSize"
})

public class Pageable {

    @JsonProperty("sort")
    private Sort sort;
    @JsonProperty("pageNumber")
    private Integer pageNumber;
    @JsonProperty("pageSize")
    private Integer pageSize;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();

    @JsonProperty("sort")
    public Sort getSort() {
        return sort;
    }

    @JsonProperty("sort")
    public void setSort(Sort sort) {
        this.sort = sort;
    }

    @JsonProperty("pageNumber")
    public Integer getPageNumber() {
        return pageNumber;
    }

    @JsonProperty("pageNumber")
    public void setPageNumber(Integer pageNumber) {
        this.pageNumber = pageNumber;
    }

    @JsonProperty("pageSize")
    public Integer getPageSize() {
        return pageSize;
    }

    @JsonProperty("pageSize")
    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
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
