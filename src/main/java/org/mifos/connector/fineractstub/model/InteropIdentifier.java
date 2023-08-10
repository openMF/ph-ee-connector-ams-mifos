package org.mifos.connector.fineractstub.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import org.springframework.validation.annotation.Validated;

/**
 * InteropIdentifier
 */
@Validated
@javax.annotation.Generated(value = "io.swagger.codegen.v3.generators.java.SpringCodegen", date = "2023-08-10T10:13:07.472376795Z[GMT]")


public class InteropIdentifier   {
  @JsonProperty("accountId")
  private String accountId = null;

  @JsonProperty("resourceId")
  private String resourceId = null;

  @JsonProperty("resourceIdentifier")
  private String resourceIdentifier = null;

  public InteropIdentifier accountId(String accountId) {
    this.accountId = accountId;
    return this;
  }

  /**
   * Get accountId
   * @return accountId
   **/
  @Schema(example = "cde3e5ee-214b-423f-97b0-0a0206aecaaf", description = "")
  
    public String getAccountId() {
    return accountId;
  }

  public void setAccountId(String accountId) {
    this.accountId = accountId;
  }

  public InteropIdentifier resourceId(String resourceId) {
    this.resourceId = resourceId;
    return this;
  }

  /**
   * Get resourceId
   * @return resourceId
   **/
  @Schema(example = "1", description = "")
  
    public String getResourceId() {
    return resourceId;
  }

  public void setResourceId(String resourceId) {
    this.resourceId = resourceId;
  }

  public InteropIdentifier resourceIdentifier(String resourceIdentifier) {
    this.resourceIdentifier = resourceIdentifier;
    return this;
  }

  /**
   * Get resourceIdentifier
   * @return resourceIdentifier
   **/
  @Schema(example = "1", description = "")
  
    public String getResourceIdentifier() {
    return resourceIdentifier;
  }

  public void setResourceIdentifier(String resourceIdentifier) {
    this.resourceIdentifier = resourceIdentifier;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InteropIdentifier interopIdentifier = (InteropIdentifier) o;
    return Objects.equals(this.accountId, interopIdentifier.accountId) &&
        Objects.equals(this.resourceId, interopIdentifier.resourceId) &&
        Objects.equals(this.resourceIdentifier, interopIdentifier.resourceIdentifier);
  }

  @Override
  public int hashCode() {
    return Objects.hash(accountId, resourceId, resourceIdentifier);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class InteropIdentifier {\n");
    
    sb.append("    accountId: ").append(toIndentedString(accountId)).append("\n");
    sb.append("    resourceId: ").append(toIndentedString(resourceId)).append("\n");
    sb.append("    resourceIdentifier: ").append(toIndentedString(resourceIdentifier)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}
