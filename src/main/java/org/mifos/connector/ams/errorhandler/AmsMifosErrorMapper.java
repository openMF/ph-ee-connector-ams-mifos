package org.mifos.connector.ams.errorhandler;

import org.mifos.connector.common.exception.PaymentHubError;
import org.mifos.connector.common.exception.mapper.ErrorMapper;
import org.springframework.stereotype.Component;

@Component
public class AmsMifosErrorMapper extends ErrorMapper {

    @Override
    public void configure() {
        add("error.msg.interop.account.not.found", PaymentHubError.PayerNotFound);
        add("validation.msg.interoperation.transfer.accountId.cannot.be.blank", PaymentHubError.PayerNotFound);

        add("error.msg.savingsaccount.transaction.insufficient.account.balance.withdraw", PaymentHubError.PayerInsufficientBalance);
        add("error.msg.savingsaccount.transaction.insufficient.account.balance", PaymentHubError.PayerInsufficientBalance);

        add("validation.msg.savingsaccount.transaction.transactionAmount.not.greater.than.zero", PaymentHubError.ExtValidationError);
        add("validation.msg.invalid.decimal.format", PaymentHubError.ExtValidationError);

        add(PaymentHubError.PayeeFspNotConfigured.getErrorCode(), PaymentHubError.PayeeFspNotConfigured);

        add("error.msg.currency.currencyCode.invalid", PaymentHubError.PayeeCurrencyInvalid);
        add("error.msg.currency.currencyCode.invalid.or.not.supported", PaymentHubError.PayeeCurrencyInvalid);
        add("error.msg.currency.currencyCode.inUse", PaymentHubError.PayeeCurrencyInvalid);

        // 404 httpstatus and response is empty -> payerFspNotFound
        // payeefsp and payerfsp not configured
        // currency invalid
    }
}
