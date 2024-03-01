package org.mifos.connector.ams.fineract.currentaccount.response;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "total",
        "content",
        "pageable"
})
public class PageFineractResponse {

    @JsonProperty("total")
    private Integer total;
    @JsonProperty("content")
    private List<FineractResponse> content;
    @JsonProperty("pageable")
    private Pageable pageable;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();

    @JsonProperty("total")
    public Integer getTotal() {
        return total;
    }

    @JsonProperty("total")
    public void setTotal(Integer total) {
        this.total = total;
    }

    @JsonProperty("content")
    public List<FineractResponse> getContent() {
        return content;
    }

    @JsonProperty("content")
    public void setContent(List<FineractResponse> content) {
        this.content = content;
    }

    @JsonProperty("pageable")
    public Pageable getPageable() {
        return pageable;
    }

    @JsonProperty("pageable")
    public void setPageable(Pageable pageable) {
        this.pageable = pageable;
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
