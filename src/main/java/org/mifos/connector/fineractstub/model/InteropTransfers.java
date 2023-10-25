package org.mifos.connector.fineractstub.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import org.springframework.validation.annotation.Validated;
import javax.validation.Valid;

/**
 * InteropTransfers
 */
@SuppressWarnings("checkstyle:Dynamic")
@Validated
@javax.annotation.Generated(value = "io.swagger.codegen.v3.generators.java.SpringCodegen", date = "2023-08-10T10:13:07.472376795Z[GMT]")


public class InteropTransfers   {
  @JsonProperty("transactionCode")
  private String transactionCode = null;

  @JsonProperty("transferCode")
  private String transferCode = null;

  @JsonProperty("accountId")
  private String accountId = null;

  @JsonProperty("amount")
  private InteropTransfersAmount amount = null;

  @JsonProperty("transactionRole")
  private String transactionRole = null;

  public InteropTransfers transactionCode(String transactionCode) {
    this.transactionCode = transactionCode;
    return this;
  }

  /**
   * Get transactionCode
   * @return transactionCode
   **/
  @Schema(example = "bd7046b7-e463-47e8-81aa-ec1c952e7fff", description = "")
  
    public String getTransactionCode() {
    return transactionCode;
  }

  public void setTransactionCode(String transactionCode) {
    this.transactionCode = transactionCode;
  }

  public InteropTransfers transferCode(String transferCode) {
    this.transferCode = transferCode;
    return this;
  }

  /**
   * Get transferCode
   * @return transferCode
   **/
  @Schema(example = "c695b904-e0eb-45ab-889e-1522c286dc20", description = "")
  
    public String getTransferCode() {
    return transferCode;
  }

  public void setTransferCode(String transferCode) {
    this.transferCode = transferCode;
  }

  public InteropTransfers accountId(String accountId) {
    this.accountId = accountId;
    return this;
  }

  /**
   * Get accountId
   * @return accountId
   **/
  @Schema(example = "000000001", description = "")
  
    public String getAccountId() {
    return accountId;
  }

  public void setAccountId(String accountId) {
    this.accountId = accountId;
  }

  public InteropTransfers amount(InteropTransfersAmount amount) {
    this.amount = amount;
    return this;
  }

  /**
   * Get amount
   * @return amount
   **/
  @Schema(description = "")
  
    @Valid
    public InteropTransfersAmount getAmount() {
    return amount;
  }

  public void setAmount(InteropTransfersAmount amount) {
    this.amount = amount;
  }

  public InteropTransfers transactionRole(String transactionRole) {
    this.transactionRole = transactionRole;
    return this;
  }

  /**
   * Get transactionRole
   * @return transactionRole
   **/
  @Schema(example = "PAYEE", description = "")
  
    public String getTransactionRole() {
    return transactionRole;
  }

  public void setTransactionRole(String transactionRole) {
    this.transactionRole = transactionRole;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InteropTransfers interopTransfers = (InteropTransfers) o;
    return Objects.equals(this.transactionCode, interopTransfers.transactionCode) &&
        Objects.equals(this.transferCode, interopTransfers.transferCode) &&
        Objects.equals(this.accountId, interopTransfers.accountId) &&
        Objects.equals(this.amount, interopTransfers.amount) &&
        Objects.equals(this.transactionRole, interopTransfers.transactionRole);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transactionCode, transferCode, accountId, amount, transactionRole);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class InteropTransfers {\n");
    
    sb.append("    transactionCode: ").append(toIndentedString(transactionCode)).append("\n");
    sb.append("    transferCode: ").append(toIndentedString(transferCode)).append("\n");
    sb.append("    accountId: ").append(toIndentedString(accountId)).append("\n");
    sb.append("    amount: ").append(toIndentedString(amount)).append("\n");
    sb.append("    transactionRole: ").append(toIndentedString(transactionRole)).append("\n");
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
