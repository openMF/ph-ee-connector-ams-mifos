package org.mifos.connector.ams.zeebe.workers.bookamount;

import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import org.mifos.connector.ams.common.exception.FineractOptimisticLockingException;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;

import java.util.List;

public interface InterfaceMoneyOutWorker<VOID, LONG, PAIR> {
    LONG holdBatch(List<TransactionItem> items,
                   String tenantId,
                   String transactionGroupId,
                   String disposalAccountId,
                   String conversionAccountId,
                   String internalCorrelationId,
                   String calledFrom);

    PAIR doBatch(List<TransactionItem> items,
                 String tenantId,
                 String transactionGroupId,
                 String disposalAccountId,
                 String conversionAccountId,
                 String internalCorrelationId,
                 String calledFrom);

    VOID doBatchOnUs(List<TransactionItem> items,
                     String tenantId,
                     String transactionGroupId,
                     String disposalAccountId,
                     String conversionAccountId,
                     String internalCorrelationId,
                     String calledFrom);


    PAIR recoverDoBatch(FineractOptimisticLockingException e);

    PAIR recoverDoBatch(ZeebeBpmnError e);

    PAIR recoverDoBatch(RuntimeException e);

    LONG recoverHoldBatch(FineractOptimisticLockingException e);

    LONG recoverHoldBatch(ZeebeBpmnError e);

    LONG recoverHoldBatch(RuntimeException e);

    VOID recoverDoBatchOnUs(FineractOptimisticLockingException e);

    VOID recoverDoBatchOnUs(ZeebeBpmnError e);

    VOID recoverDoBatchOnUs(RuntimeException e);
}
