package org.mifos.connector.ams.zeebe.workers.accountlookup;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.client.api.worker.JobHandler;
import org.apache.commons.validator.routines.IBANValidator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Map;

@Component
public class AmsWorker implements JobHandler {

    @Autowired
    private AmsDataTableQueryApi dataTableQueryApi;

    private IBANValidator ibanValidator = IBANValidator.DEFAULT_IBAN_VALIDATOR;

    private static final String[] ACCEPTED_CURRENCIES = new String[] { "HUF" };

    public AmsWorker() {
    }

    public AmsWorker(AmsDataTableQueryApi dataTableQueryApi) {
        this.dataTableQueryApi = dataTableQueryApi;
    }

    @Override
    public void handle(JobClient jobClient, ActivatedJob activatedJob) {
        Map<String, Object> variables = activatedJob.getVariablesAsMap();

        String iban = (String) variables.get("filterValue");

        if (ibanValidator.isValid(iban)) {

            AmsDataTableQueryResponse response = dataTableQueryApi.queryDataTable((String) variables.get("dataTableId"),
                    (String) variables.get("dataTableFilterColumnName"),
                    (String) variables.get("filterValue"),
                    (String) variables.get("resultColumns"));

            AccountAmsStatus status = AccountAmsStatus.NOT_READY_TO_RECEIVE_MONEY;

            if (Arrays.stream(ACCEPTED_CURRENCIES).anyMatch(response.fiat_account_id()::equalsIgnoreCase)
                    && response.accountAmsStatus() == 300) {
                status = AccountAmsStatus.READY_TO_RECEIVE_MONEY;
            }

            AmsWorkerResponse workerResponse = new AmsWorkerResponse(status, response.fiat_account_id(), response.ecurrency_account_id());

            variables.put("amsWorkerResponse", workerResponse);

            jobClient.newCompleteCommand(activatedJob.getKey())
                    .variables(variables)
                    .send();
        }
    }
}
