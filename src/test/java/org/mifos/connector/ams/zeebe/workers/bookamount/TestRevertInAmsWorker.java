package org.mifos.connector.ams.zeebe.workers.bookamount;

import com.baasflow.commons.events.EventService;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.mifos.connector.ams.fineract.TenantConfigs;
import org.mifos.connector.ams.fineract.savingsaccounttransaction.response.TransactionQueryPayload;
import org.mifos.connector.ams.mapstruct.Pain001Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.AuthTokenHelper;
import org.mifos.connector.ams.zeebe.workers.utils.BatchItemBuilder;
import org.mifos.connector.ams.zeebe.workers.utils.ContactDetailsUtil;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestRevertInAmsWorker {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Test
    public void testCurrentAccount() throws Exception {
        Consumer<TransactionItem> itemValidator = item -> {
            String json = item.toString();
            assertFalse(json.contains("AmountDetails"));
            assertFalse(json.contains("AdditionalTransactionInformation"));
            assertFalse(json.contains("Batch"));
            assertFalse(json.contains("TransactionCreationChannel"));
        };

        RevertInAmsWorker worker = setupWorker(itemValidator);
        String pain001 = Files.readString(Path.of(getClass().getResource("/pain001.json").toURI()));

        Map<String, Object> output = worker.revertInAms(
                "internalCorrelationId",
                "transactionFeeCorrelationId",
                pain001,
                "1",
                "2",
                "2024-01-31,",
                "HCT_INST",
                "transactionGroupId",
                "transactionCategoryPurposeCode",
                BigDecimal.TEN,
                "HUF",
                "transactionFeeCategoryPurposeCode",
                BigDecimal.ONE,
                "binx",
                "CURRENT",
                false
        );

        logger.info("output: {}", output);
    }

    @Test
    public void testSavingsAccount() throws Exception {
        Consumer<TransactionItem> itemValidator = item -> {
            String str = item.toString();
            if (str.contains("Identification")) {
                assertTrue(str.contains("AmountDetails"));
//                assertTrue(str.contains("AdditionalTransactionInformation"));
//                assertTrue(str.contains("Batch"));
//                assertTrue(str.contains("TransactionCreationChannel"));
            }
        };

        String pain001 = Files.readString(Path.of(getClass().getResource("/pain001.json").toURI()));
        RevertInAmsWorker worker = setupWorker(itemValidator);

        Map<String, Object> output = worker.revertInAms(
                "internalCorrelationId",
                "transactionFeeCorrelationId",
                pain001,
                "1",
                "2",
                "2024-01-31,",
                "HCT_INST",
                "transactionGroupId",
                "transactionCategoryPurposeCode",
                BigDecimal.TEN,
                "HUF",
                "transactionFeeCategoryPurposeCode",
                BigDecimal.ONE,
                "binx",
                "SAVINGS",
                false
        );

        logger.info("output: {}", output);
    }

    @NotNull
    private RevertInAmsWorker setupWorker(Consumer<TransactionItem> validator) {
        RevertInAmsWorker worker = new RevertInAmsWorker() {
            @Override
            protected String doBatch(List<TransactionItem> items, String tenantId, String transactionGroupId, String disposalAccountId, String conversionAccountId, String internalCorrelationId, String calledFrom) {
                logger.debug("executing batch of {} items:", items.size());
                items.forEach(item -> {
                    logger.info("- {}", item);
                    validator.accept(item);
                });

                return null;
            }
        };
        worker.eventService = mock(EventService.class);
        worker.incomingMoneyApi = "/savingsaccounts/";
        worker.tenantConfigs = mock(TenantConfigs.class);
        when(worker.tenantConfigs.findPaymentTypeId(any(), any())).thenReturn("some code");
        when(worker.tenantConfigs.findResourceCode(any(), any())).thenReturn("some other code");

        worker.batchItemBuilder = new BatchItemBuilder();

        AuthTokenHelper mockAuthTokenHelper = mock(AuthTokenHelper.class);
        when(mockAuthTokenHelper.generateAuthToken()).thenReturn("token");
        worker.batchItemBuilder.authTokenHelper = mockAuthTokenHelper;
        worker.authTokenHelper = mockAuthTokenHelper;
        worker.pain001Camt053Mapper = Pain001Camt053Mapper.MAPPER;
        worker.contactDetailsUtil = new ContactDetailsUtil();
        worker.restTemplate = mock(org.springframework.web.client.RestTemplate.class);
        ResponseEntity mockResponse = mock(ResponseEntity.class);
        when(worker.restTemplate.exchange((String) any(), any(), any(), (Class) any())).thenReturn(mockResponse);
        when(mockResponse.getBody()).thenReturn(mock(TransactionQueryPayload.class));
        return worker;
    }
}
