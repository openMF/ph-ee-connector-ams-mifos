package org.mifos.connector.ams.interop;

import org.apache.camel.Exchange;

public interface AmsService {

    void getLocalQuote(Exchange e);

    void getExternalAccount(Exchange e);

    void sendTransfer(Exchange e);

    void login(Exchange e);

    void getSavingsAccount(Exchange e);

    void getClient(Exchange e);

}
