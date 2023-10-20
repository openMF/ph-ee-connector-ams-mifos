package org.mifos.connector.fineractstub.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Objects;
import javax.validation.constraints.NotNull;
import org.springframework.validation.annotation.Validated;

/**
 * SavingsDeposit
 */
@SuppressWarnings("checkstyle:Dynamic")
@Validated
@javax.annotation.Generated(value = "io.swagger.codegen.v3.generators.java.SpringCodegen", date = "2023-08-10T10:13:07.472376795Z[GMT]")

public class SavingsDeposit {

    @JsonProperty("transactionDate")
    private String transactionDate = null;

    @JsonProperty("transactionAmount")
    private String transactionAmount = null;

    @JsonProperty("paymentTypeId")
    private String paymentTypeId = null;

    @JsonProperty("locale")
    private String locale = null;

    @JsonProperty("dateFormat")
    private String dateFormat = null;

    public SavingsDeposit transactionDate(String transactionDate) {
        this.transactionDate = transactionDate;
        return this;
    }

    /**
     * Get transactionDate
     *
     * @return transactionDate
     **/
    @Schema(example = "08 August 2023", required = true, description = "")
    @NotNull

    public String getTransactionDate() {
        return transactionDate;
    }

    public void setTransactionDate(String transactionDate) {
        this.transactionDate = transactionDate;
    }

    public SavingsDeposit transactionAmount(String transactionAmount) {
        this.transactionAmount = transactionAmount;
        return this;
    }

    /**
     * Get transactionAmount
     *
     * @return transactionAmount
     **/
    @Schema(example = "10000", required = true, description = "")
    @NotNull

    public String getTransactionAmount() {
        return transactionAmount;
    }

    public void setTransactionAmount(String transactionAmount) {
        this.transactionAmount = transactionAmount;
    }

    public SavingsDeposit paymentTypeId(String paymentTypeId) {
        this.paymentTypeId = paymentTypeId;
        return this;
    }

    /**
     * Get paymentTypeId
     *
     * @return paymentTypeId
     **/
    @Schema(example = "1", required = true, description = "")
    @NotNull

    public String getPaymentTypeId() {
        return paymentTypeId;
    }

    public void setPaymentTypeId(String paymentTypeId) {
        this.paymentTypeId = paymentTypeId;
    }

    public SavingsDeposit locale(String locale) {
        this.locale = locale;
        return this;
    }

    /**
     * Get locale
     *
     * @return locale
     **/
    @Schema(example = "en", required = true, description = "")
    @NotNull

    public String getLocale() {
        return locale;
    }

    public void setLocale(String locale) {
        this.locale = locale;
    }

    public SavingsDeposit dateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
        return this;
    }

    /**
     * Get dateFormat
     *
     * @return dateFormat
     **/
    @Schema(example = "dd MMMM yyyy", required = true, description = "")
    @NotNull

    public String getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    @Override
    public boolean equals(java.lang.Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SavingsDeposit savingsDeposit = (SavingsDeposit) o;
        return Objects.equals(this.transactionDate, savingsDeposit.transactionDate)
                && Objects.equals(this.transactionAmount, savingsDeposit.transactionAmount)
                && Objects.equals(this.paymentTypeId, savingsDeposit.paymentTypeId) && Objects.equals(this.locale, savingsDeposit.locale)
                && Objects.equals(this.dateFormat, savingsDeposit.dateFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionDate, transactionAmount, paymentTypeId, locale, dateFormat);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("class SavingsDeposit {\n");

        sb.append("    transactionDate: ").append(toIndentedString(transactionDate)).append("\n");
        sb.append("    transactionAmount: ").append(toIndentedString(transactionAmount)).append("\n");
        sb.append("    paymentTypeId: ").append(toIndentedString(paymentTypeId)).append("\n");
        sb.append("    locale: ").append(toIndentedString(locale)).append("\n");
        sb.append("    dateFormat: ").append(toIndentedString(dateFormat)).append("\n");
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
