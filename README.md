# ph-ee-connector-ams-mifos
Payment Hub Enterprise Edition connector for local AMS.
By default local quote is disabled and only empty Zeebe workers are started.
Currently supported AMS backends: (configure corresponding yml files to modify properties)
* Fineract 1.2 -> use spring profile fin12

**Important:** Use filesystem path to set keystore for TLS Client configuration.

# Zeebe worker api mapping
| Route                                         | Endpoint                                                                               | HTTP Method |
|-----------------------------------------------|----------------------------------------------------------------------------------------|-------------|
| direct:get-external-account                   | /interoperation/accounts/{externalAccountId}                                           | GET         |
| direct:send-local-quote                       | /interoperation/quotes                                                                 | GET         |
| direct:send-transfers                         | /transfers                                                                             | POST        |
| direct:get-party                              | /customers/{customerIdentifier}                                                        | GET         |
| direct:register-party-finx                    | /interoperation/parties/{idType}/{idValue}                                             | POST        |
| direct:register-party-fincn                   | /parties/{idType}/{idValue}                                                            | POST        |
| direct:add-interop-identifier-to-account      | adds interoperability identifier depending upon fineract cloud native or fineract X    | POST        |
| direct:remove-interop-identifier-from-account | removes interoperability identifier depending upon fineract cloud native of fineract X | PUT         |
| rest:POST:/transfer/deposit                   | calls send transfer                                                                    | POST        |

# Routes to worker and bpmn mapping

| Route                   | Worker                        | BPMN                                                                                                                                                                                                                                                                                                                           |
|-------------------------|-------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| direct:send-transfers   | book-funds,block-funds        | debit-party-process-DFSPID.bpmn,gsma-bill-payment.bpmn,gsma-inttransfer-payer.bpmn,gsma-link-based-transfer.bpmn,gsma-p2p-wo-local-quote.bpmn,gsma-p2p.bpmn,international-remittance-payer-process-DFSPID.bpmn,payer-fund-transfer-DFSPID.bpmn,payer-fund-transfer-terminate-DFSPID.bpmn,payer-transaction-request-DFSPID.bpmn |
|                         | release-block                 | gsma-bill-payment.bpmn,gsma-inttransfer-payer.bpmn,gsma-link-based-transfer.bpmn,gsma-p2p-wo-local-quote.bpmn,gsma-p2p.bpmn,international-remittance-payer-process-DFSPID.bpmn,payer-fund-transfer-DFSPID.bpmn,payer-fund-transfer-terminate-DFSPID.bpmn,payer-transaction-request-DFSPID.bpmn                                 |
|                         | payee-commit-transfer-dfspId  | payee-quote-transfer-DFSPID.bpmn,payer-quote-transfer-DFSPID.bpmn                                                                                                                                                                                                                                                              |
|                         | payee-deposit-transfer-dfspId | international-remittance-payee-process-DFSPID.bpmn                                                                                                                                                                                                                                                                             |
| direct:get-party        | party-lookup-local-dfspId     | debit-party-process-DFSPID.bpmn,gsma-bill-payment.bpmn,gsma-inttransfer-payer.bpmn,gsma-link-based-transfer.bpmn,gsma-p2p-wo-local-quote.bpmn,gsma-p2p.bpmn,payee-party-lookup-DFSPID.bpmn                                                                                                                                     |
| direct:send-local-quote | payer-local-quote-dfspId      | gsma-p2p.bpmn,payer-fund-transfer-DFSPID.bpmn,payer-fund-transfer-terminate-DFSPID.bpmn,orchestration/feel/payer-transaction-request-DFSPID.bpmn                                                                                                                                                                               |
|                         | payee-quote-dfspId            | gsma-payee-process.bpmn,payee-quote-transfer-DFSPID.bpmn                                                                                                                                                                                                                                                                       |


## Spotless
Use below command to execute the spotless apply.
```shell
./gradlew spotlessApply
```

# Checkstyle
Use below command to execute the checkstyle test.
```shell
./gradlew checkstyleMain
```
