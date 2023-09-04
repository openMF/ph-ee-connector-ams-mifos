package org.mifos.connector.ams.interop;

import org.apache.camel.Exchange;
import org.springframework.stereotype.Service;

@Service
public interface AmsService {

    void getLocalQuote(Exchange e);

    void getExternalAccount(Exchange e);

    void registerInteropIdentifier(Exchange e);

    void removeInteropIdentifier(Exchange e);

    void sendTransfer(Exchange e);

    void repayLoan(Exchange e);

    void login(Exchange e);

    void getSavingsAccount(Exchange e);

    void getSavingsAccountDefiniton(Exchange e);

    void getSavingsAccounts(Exchange e);

    void getClient(Exchange e);

    void getClientByMobileNo(Exchange e);

    void getSavingsAccountsTransactions(Exchange e);

    default void getClientImage(Exchange e) {}

}
