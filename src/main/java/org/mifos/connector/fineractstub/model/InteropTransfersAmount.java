package org.mifos.connector.fineractstub.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Objects;
import org.springframework.validation.annotation.Validated;

/**
 * InteropTransfersAmount
 */
@SuppressWarnings("checkstyle:Dynamic")
@Validated
@javax.annotation.Generated(value = "io.swagger.codegen.v3.generators.java.SpringCodegen", date = "2023-08-10T10:13:07.472376795Z[GMT]")

public class InteropTransfersAmount {

    @JsonProperty("amount")
    private String amount = null;

    @JsonProperty("currency")
    private String currency = null;

    public InteropTransfersAmount amount(String amount) {
        this.amount = amount;
        return this;
    }

    /**
     * Get amount
     *
     * @return amount
     **/
    @Schema(example = "400", description = "")

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public InteropTransfersAmount currency(String currency) {
        this.currency = currency;
        return this;
    }

    /**
     * Get currency
     *
     * @return currency
     **/
    @Schema(example = "USD", description = "")

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    @Override
    public boolean equals(java.lang.Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InteropTransfersAmount interopTransfersAmount = (InteropTransfersAmount) o;
        return Objects.equals(this.amount, interopTransfersAmount.amount) && Objects.equals(this.currency, interopTransfersAmount.currency);
    }

    @Override
    public int hashCode() {
        return Objects.hash(amount, currency);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("class InteropTransfersAmount {\n");

        sb.append("    amount: ").append(toIndentedString(amount)).append("\n");
        sb.append("    currency: ").append(toIndentedString(currency)).append("\n");
        sb.append("}");
        return sb.toString();
    }

    /**
     * Convert the given object to string with each line indented by 4 spaces (except the first line).
     */
    private String toIndentedString(java.lang.Object o) {
        if (o == null) {
            return "null";
        }
        return o.toString().replace("\n", "\n    ");
    }
}
