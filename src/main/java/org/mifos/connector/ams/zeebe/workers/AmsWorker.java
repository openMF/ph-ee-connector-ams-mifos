package org.mifos.connector.ams.zeebe.workers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import com.google.common.base.Optional;

import org.apache.commons.validator.routines.IBANValidator;
import org.apache.fineract.client.models.GetSavingsAccountsAccountIdResponse;
import org.apache.fineract.client.services.SavingsAccountApi;
import org.apache.fineract.client.util.FineractClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.client.api.worker.JobHandler;

@Component
public class AmsWorker implements JobHandler {
	
	Logger logger = LoggerFactory.getLogger(AmsWorker.class);
    
    @Autowired
    private FineractClient fineractClient;

    private IBANValidator ibanValidator = IBANValidator.DEFAULT_IBAN_VALIDATOR;

    private static final String[] ACCEPTED_CURRENCIES = new String[] { "HUF" };

    public AmsWorker() {
    }

    @Override
    public void handle(JobClient jobClient, ActivatedJob activatedJob) {
        Map<String, Object> variables = activatedJob.getVariablesAsMap();
        AccountAmsStatus status = AccountAmsStatus.NOT_READY_TO_RECEIVE_MONEY;
        GetSavingsAccountsAccountIdResponse fiatCurrency = null;
        GetSavingsAccountsAccountIdResponse eCurrency = null;

        String iban = (String) variables.get("valueFilter");

        if (ibanValidator.isValid(iban)) {

//            fineractClient.datatablesApiResource.queryDataTable((String) variables.get("datatable"),
//                    (String) variables.get("columnFilter"),
//                    (String) variables.get("valueFilter"),
//                    (String) variables.get("resultColumns"));
            
            Long accountFiatCurrencyId = 1L;
            Long accountECurrencyId = 2L;
            
            SavingsAccountApi savingsAccounts = fineractClient.savingsAccounts;
            try {
            	fiatCurrency = savingsAccounts.retrieveOne24(accountFiatCurrencyId, false, null).execute().body();
            	eCurrency = savingsAccounts.retrieveOne24(accountECurrencyId, false, null).execute().body();
            	
            	if (Arrays.stream(ACCEPTED_CURRENCIES).anyMatch(fiatCurrency.getCurrency().getCode()::equalsIgnoreCase)
            			&& fiatCurrency.getStatus().getId() == 300
            			&& eCurrency.getStatus().getId() == 300) {
            		status = AccountAmsStatus.READY_TO_RECEIVE_MONEY;
            	}
            	
            } catch (IOException e) {
            	logger.error(e.getMessage(), e);
            }
            
        }
        
        variables.put("status", status);
        variables.put("eCurrency", Optional.of(eCurrency).transform(GetSavingsAccountsAccountIdResponse::getId).orNull());
        variables.put("fiatCurrency", Optional.of(fiatCurrency).transform(GetSavingsAccountsAccountIdResponse::getId).orNull());

        jobClient.newCompleteCommand(activatedJob.getKey())
                .variables(variables)
                .send();
    }
}
