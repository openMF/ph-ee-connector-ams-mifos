package org.mifos.connector.ams.zeebe.workers.bookamount;

import com.baasflow.commons.events.EventService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import hu.dpc.rt.utils.converter.Pacs004ToCamt053Converter;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import iso.std.iso._20022.tech.json.camt_053_001.*;
import iso.std.iso._20022.tech.json.camt_053_001.ActiveOrHistoricCurrencyAndAmountRange2.CreditDebitCode;
import iso.std.iso._20022.tech.json.pain_001_001.CreditTransferTransaction40;
import iso.std.iso._20022.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;
import iso.std.iso._20022.tech.json.pain_001_001.PartyIdentification135;
import iso.std.iso._20022.tech.json.pain_001_001.RemittanceInformation16;
import iso.std.iso._20022.tech.xsd.pacs_004_001.PaymentTransactionInformation27;
import iso.std.iso._20022.tech.xsd.pacs_004_001.RemittanceInformation5;
import jakarta.xml.bind.JAXBException;
import lombok.extern.slf4j.Slf4j;
import org.mifos.connector.ams.common.SerializationHelper;
import org.mifos.connector.ams.fineract.Config;
import org.mifos.connector.ams.fineract.ConfigFactory;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.log.LogInternalCorrelationId;
import org.mifos.connector.ams.log.TraceZeebeArguments;
import org.mifos.connector.ams.mapstruct.Pain001Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.*;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Component
@Slf4j
public class BookOnConversionAccountInAmsWorker extends AbstractMoneyInOutWorker {

    @Autowired
    private Pain001Camt053Mapper camt053Mapper;

    @Value("${fineract.incoming-money-api}")
    protected String incomingMoneyApi;

    @Value("${fineract.current-account-api}")
    protected String currentAccountApi;

    @Autowired
    private ConfigFactory paymentTypeConfigFactory;

    @Autowired
    private JAXBUtils jaxbUtils;

    @Autowired
    private BatchItemBuilder batchItemBuilder;

    @Autowired
    private ContactDetailsUtil contactDetailsUtil;

    @Autowired
    private EventService eventService;

    @Autowired
    private SerializationHelper serializationHelper;

    @Autowired
    @Qualifier("painMapper")
    private ObjectMapper painMapper;

    @JobWorker
    @LogInternalCorrelationId
    @TraceZeebeArguments
    public void bookOnConversionAccountInAms(JobClient jobClient,
                                             ActivatedJob activatedJob,
                                             @Variable String originalPain001,
                                             @Variable String internalCorrelationId,
                                             @Variable String transactionFeeInternalCorrelationId,
                                             @Variable String paymentScheme,
                                             @Variable String transactionDate,
                                             @Variable String conversionAccountAmsId,
                                             @Variable String transactionGroupId,
                                             @Variable String transactionCategoryPurposeCode,
                                             @Variable String transactionFeeCategoryPurposeCode,
                                             @Variable BigDecimal amount,
                                             @Variable String currency,
                                             @Variable BigDecimal transactionFeeAmount,
                                             @Variable String tenantIdentifier,
                                             @Variable String debtorIban,
                                             @Variable String accountProductType,
                                             @Variable String valueDated
                                             ) {
        log.info("bookOnConversionAccountInAms");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "bookOnConversionAccountInAms", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> bookOnConversionAccountInAms(originalPain001,
                        internalCorrelationId,
                        transactionFeeInternalCorrelationId,
                        paymentScheme,
                        transactionDate,
                        conversionAccountAmsId,
                        transactionGroupId,
                        transactionCategoryPurposeCode,
                        transactionFeeCategoryPurposeCode,
                        amount,
                        currency,
                        transactionFeeAmount,
                        tenantIdentifier,
                        debtorIban,
                        accountProductType,
                        valueDated));
    }

    private Void bookOnConversionAccountInAms(String originalPain001,
                                              String internalCorrelationId,
                                              String transactionFeeInternalCorrelationId,
                                              String paymentScheme,
                                              String transactionDate,
                                              String conversionAccountAmsId,
                                              String transactionGroupId,
                                              String transactionCategoryPurposeCode,
                                              String transactionFeeCategoryPurposeCode,
                                              BigDecimal amount,
                                              String currency,
                                              BigDecimal transactionFeeAmount,
                                              String tenantIdentifier,
                                              String debtorIban,
                                              String accountProductType,
                                              String valueDated) {
        try {
            // STEP 0 - collect / extract information
            MDC.put("internalCorrelationId", internalCorrelationId);
            log.info("Starting book debit on conversion account worker");
            log.info("Withdrawing amount {} from conversion account {} of tenant {}", amount, conversionAccountAmsId, tenantIdentifier);
            transactionDate = transactionDate.replaceAll("-", "");
            String apiPath = accountProductType.equalsIgnoreCase("SAVINGS") ? incomingMoneyApi.substring(1) : currentAccountApi.substring(1);
            Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
            Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = painMapper.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
            BankToCustomerStatementV08 convertedStatement = camt053Mapper.toCamt053Entry(pain001.getDocument());
            ReportEntry10 convertedCamt053Entry = convertedStatement.getStatement().get(0).getEntry().get(0);
            EntryTransaction10 transactionDetails = convertedCamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0);
            CreditTransferTransaction40 creditTransferTransaction = pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0);
            String unstructured = Optional.ofNullable(creditTransferTransaction.getRemittanceInformation()).map(RemittanceInformation16::getUnstructured).map(List::toString).orElse("");
            PartyIdentification135 creditor = creditTransferTransaction.getCreditor();
            String endToEndId = creditTransferTransaction.getPaymentIdentification().getEndToEndIdentification();
            String creditorId = contactDetailsUtil.getId(creditor.getContactDetails());
            String creditorIban = creditTransferTransaction.getCreditorAccount().getIdentification().getIban();
            String creditorName = creditor.getName();
            List<TransactionItem> items = new ArrayList<>();
            String partnerAccountSecondaryIdentifier = contactDetailsUtil.getId(pain001.getDocument().getPaymentInformation().get(0).getDebtor().getContactDetails());

            // STEP 1a - batch: withdraw amount
            String currentAccountWithdrawalRelativeUrl = String.format("%s%s/transactions?command=%s", apiPath, conversionAccountAmsId, "withdrawal");
            String withdrawAmountOperation = "bookOnConversionAccountInAms.ConversionAccount.WithdrawTransactionAmount";
            String withdrawAmountConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
            String withdrawAmountPaymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawAmountConfigOperationKey);
            Integer withdrawAmountPaymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawAmountConfigOperationKey);

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                String withdrawAmountBodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, amount, withdrawAmountPaymentTypeId, "", FORMAT, locale));
                batchItemBuilder.add(tenantIdentifier, items, currentAccountWithdrawalRelativeUrl, withdrawAmountBodyItem, false);
            } // CURRENT account sends a single call only at the details step


            // STEP 1b - batch: withdraw amount details
            transactionDetails.setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedCamt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);
            convertedCamt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            transactionDetails.setAdditionalTransactionInformation(withdrawAmountPaymentTypeCode);
            String withdrawDetailsCamt053Entry = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);
            String withdrawDetailsCamt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";

            if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                String withdrawDetailsCamt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(internalCorrelationId, withdrawDetailsCamt053Entry, debtorIban, withdrawAmountPaymentTypeCode, transactionGroupId, creditorName, creditorIban, null, creditorId, unstructured, transactionCategoryPurposeCode, paymentScheme, conversionAccountAmsId, null, endToEndId));
                batchItemBuilder.add(tenantIdentifier, items, withdrawDetailsCamt053RelativeUrl, withdrawDetailsCamt053Body, true);
            } else {  // CURRENT account executes withdrawal and details in one step
                String withdrawAmountTransactionBody = painMapper.writeValueAsString(new CurrentAccountTransactionBody(amount, FORMAT, locale, withdrawAmountPaymentTypeId, currency, List.of(
                        new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                                debtorIban,
                                withdrawDetailsCamt053Entry,
                                internalCorrelationId,
                                creditorName,
                                creditorIban,
                                transactionGroupId,
                                endToEndId,
                                transactionCategoryPurposeCode,
                                paymentScheme,
                                unstructured,
                                conversionAccountAmsId,
                                null,
                                partnerAccountSecondaryIdentifier,
                                null,
                                valueDated
                        )), "dt_current_transaction_details"))
                ));
                batchItemBuilder.add(tenantIdentifier, items, currentAccountWithdrawalRelativeUrl, withdrawAmountTransactionBody, false);
            }

            // STEP 2c - batch: withdraw fee, if any
            if (!BigDecimal.ZERO.equals(transactionFeeAmount)) {
                log.info("Withdrawing fee {} from conversion account {}", transactionFeeAmount, conversionAccountAmsId);
                String withdrawFeeOperation = "bookOnConversionAccountInAms.ConversionAccount.WithdrawTransactionFee";
                String withdrawFeeConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawFeeOperation);
                Integer withdrawFeePaymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawFeeConfigOperationKey);
                String withdrawFeePaymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawFeeConfigOperationKey);
                transactionDetails.setAdditionalTransactionInformation(withdrawFeePaymentTypeCode);
                transactionDetails.setSupplementaryData(new ArrayList<>());
                camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001.getDocument(), transactionDetails, transactionFeeCategoryPurposeCode);
                camt053Mapper.refillOtherIdentification(pain001.getDocument(), transactionDetails);

                if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                    String withdrawFeeBodyItem = painMapper.writeValueAsString(new TransactionBody(transactionDate, transactionFeeAmount, withdrawFeePaymentTypeId, "", FORMAT, locale));
                    batchItemBuilder.add(tenantIdentifier, items, currentAccountWithdrawalRelativeUrl, withdrawFeeBodyItem, false);
                }

                String withdrawFeeCamt053Entry = serializationHelper.writeCamt053AsString(accountProductType, convertedCamt053Entry);

                if (accountProductType.equalsIgnoreCase("SAVINGS")) {
                    String withdrawFeeDetailsCamt053Body = painMapper.writeValueAsString(new DtSavingsTransactionDetails(transactionFeeInternalCorrelationId, withdrawFeeCamt053Entry, debtorIban, withdrawFeePaymentTypeCode, transactionGroupId, creditorName, creditorIban, null, creditorId, unstructured, transactionFeeCategoryPurposeCode, paymentScheme, conversionAccountAmsId, null, endToEndId));
                    batchItemBuilder.add(tenantIdentifier, items, withdrawDetailsCamt053RelativeUrl, withdrawFeeDetailsCamt053Body, true);
                } else {  // CURRENT account executes withdrawal and details in one step
                    String withdrawFeeTransactionBody = painMapper.writeValueAsString(new CurrentAccountTransactionBody(transactionFeeAmount, FORMAT, locale, withdrawFeePaymentTypeId, currency, List.of(
                            new CurrentAccountTransactionBody.DataTable(List.of(new CurrentAccountTransactionBody.Entry(
                                    debtorIban,
                                    withdrawFeeCamt053Entry,
                                    transactionFeeInternalCorrelationId,
                                    creditorName,
                                    creditorIban,
                                    transactionGroupId,
                                    endToEndId,
                                    transactionFeeCategoryPurposeCode,
                                    paymentScheme,
                                    unstructured,
                                    conversionAccountAmsId,
                                    null,
                                    partnerAccountSecondaryIdentifier,
                                    null,
                                    valueDated
                            )), "dt_current_transaction_details"))
                    ));
                    batchItemBuilder.add(tenantIdentifier, items, currentAccountWithdrawalRelativeUrl, withdrawFeeTransactionBody, false);
                }
            }

            doBatch(items, tenantIdentifier, "-1", conversionAccountAmsId, internalCorrelationId, "bookOnConversionAccountInAms");

            log.info("Book debit on conversion account has finished  successfully");

            MDC.remove("internalCorrelationId");
        } catch (JsonProcessingException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }

        return null;
    }

    @JobWorker
    @TraceZeebeArguments
    @LogInternalCorrelationId
    public void withdrawTheAmountFromConversionAccountInAms(JobClient client,
                                                            ActivatedJob activatedJob,
                                                            @Variable BigDecimal amount,
                                                            @Variable String conversionAccountAmsId,
                                                            @Variable String tenantIdentifier,
                                                            @Variable String paymentScheme,
                                                            @Variable String transactionCategoryPurposeCode,
                                                            @Variable String camt056,
                                                            @Variable String debtorIban,
                                                            @Variable String generatedPacs004,
                                                            @Variable String pacs002,
                                                            @Variable String transactionDate,
                                                            @Variable String internalCorrelationId,
                                                            @Variable String accountProductType) {
        log.info("withdrawTheAmountFromConversionAccountInAms");
        eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob,
                        "withdrawTheAmountFromConversionAccountInAms",
                        null,
                        null,
                        eventBuilder),
                eventBuilder -> withdrawTheAmountFromConversionAccountInAms(amount,
                        conversionAccountAmsId,
                        tenantIdentifier,
                        paymentScheme,
                        transactionCategoryPurposeCode,
                        camt056,
                        debtorIban,
                        generatedPacs004,
                        pacs002,
                        transactionDate,
                        internalCorrelationId,
                        accountProductType));
    }

    private Void withdrawTheAmountFromConversionAccountInAms(BigDecimal amount,
                                                             String conversionAccountAmsId,
                                                             String tenantIdentifier,
                                                             String paymentScheme,
                                                             String transactionCategoryPurposeCode,
                                                             String camt056,
                                                             String debtorIban,
                                                             String originalPacs004,
                                                             String originalPacs002,
                                                             String transactionDate,
                                                             String internalCorrelationId,
                                                             String accountProductType) {
        try {
            log.info("Withdrawing amount {} from conversion account {} of tenant {}", amount, conversionAccountAmsId, tenantIdentifier);

            if (transactionDate == null) {
                transactionDate = LocalDate.now().format(DateTimeFormatter.ofPattern(FORMAT));
            }

            iso.std.iso._20022.tech.xsd.pacs_004_001.Document pacs004 = jaxbUtils.unmarshalPacs004(originalPacs004);

            iso.std.iso._20022.tech.xsd.pacs_002_001.Document pacs002 = jaxbUtils.unmarshalPacs002(originalPacs002);

            String conversionAccountWithdrawalRelativeUrl = String.format("%s%s/transactions?command=%s", incomingMoneyApi.substring(1), conversionAccountAmsId, "withdrawal");

            Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
            String withdrawAmountOperation = "withdrawTheAmountFromConversionAccountInAms.ConversionAccount.WithdrawTransactionAmount";
            String configOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
            Integer paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(configOperationKey);
            String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(configOperationKey);

            TransactionBody body = new TransactionBody(
                    transactionDate,
                    amount,
                    paymentTypeId,
                    "",
                    FORMAT,
                    locale);

            String bodyItem = painMapper.writeValueAsString(body);

            List<TransactionItem> items = new ArrayList<>();

            batchItemBuilder.add(tenantIdentifier, items, conversionAccountWithdrawalRelativeUrl, bodyItem, false);

            Pacs004ToCamt053Converter converter = new Pacs004ToCamt053Converter();
            ReportEntry10 camt053Entry = converter.convert(pacs004,
                    new BankToCustomerStatementV08()
                            .withStatement(List.of(new AccountStatement9()
                                    .withEntry(List.of(new ReportEntry10()
                                            .withEntryDetails(List.of(new EntryDetails9()
                                                    .withTransactionDetails(List.of(new EntryTransaction10()))))))))).getStatement().get(0).getEntry().get(0);
            camt053Entry.setStatus(new EntryStatus1Choice().withAdditionalProperty("Proprietary", "BOOKED"));
            camt053Entry.setCreditDebitIndicator(CreditDebitCode.DBIT);

            PaymentTransactionInformation27 paymentTransactionInformation = pacs004.getPmtRtr().getTxInf().get(0);

            XMLGregorianCalendar orgnlCreDtTm = pacs002.getFIToFIPmtStsRpt().getOrgnlGrpInfAndSts().getOrgnlCreDtTm();
            if (orgnlCreDtTm == null) {
                String hyphenatedDate = transactionDate.substring(0, 4) + "-" + transactionDate.substring(4, 6) + "-" + transactionDate.substring(6);
                camt053Entry.getValueDate().setAdditionalProperty("Date", hyphenatedDate);
            } else {
                ZoneId zi = TimeZone.getTimeZone("Europe/Budapest").toZoneId();
                ZonedDateTime zdt = orgnlCreDtTm.toGregorianCalendar().toZonedDateTime().withZoneSameInstant(zi);
                var copy = DatatypeFactory.newDefaultInstance().newXMLGregorianCalendar(GregorianCalendar.from(zdt));
                String date = copy.toGregorianCalendar().toZonedDateTime().format(DateTimeFormatter.ISO_LOCAL_DATE);
                camt053Entry.getValueDate().setAdditionalProperty("Date", date);
            }

            EntryTransaction10 transactionDetails = camt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0);
            transactionDetails.setAdditionalTransactionInformation(paymentTypeCode);

            String camt053 = serializationHelper.writeCamt053AsString(accountProductType, camt053Entry);

            String camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";

            String remittanceInformationUnstructured = Optional.ofNullable(paymentTransactionInformation.getOrgnlTxRef().getRmtInf())
                    .map(RemittanceInformation5::getUstrd).map(List::toString)
                    .orElse("");

            DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
                    internalCorrelationId,
                    camt053,
                    paymentTransactionInformation.getOrgnlTxRef().getCdtrAcct().getId().getIBAN(),
                    paymentTypeCode,
                    internalCorrelationId,
                    paymentTransactionInformation.getOrgnlTxRef().getDbtr().getNm(),
                    paymentTransactionInformation.getOrgnlTxRef().getDbtrAcct().getId().getIBAN(),
                    null,
                    contactDetailsUtil.getId(paymentTransactionInformation.getOrgnlTxRef().getDbtr().getCtctDtls()),
                    remittanceInformationUnstructured,
                    transactionCategoryPurposeCode,
                    paymentScheme,
                    conversionAccountAmsId,
                    null,
                    paymentTransactionInformation.getOrgnlEndToEndId());

            String camt053Body = painMapper.writeValueAsString(td);

            batchItemBuilder.add(tenantIdentifier, items, camt053RelativeUrl, camt053Body, true);

            doBatch(items,
                    tenantIdentifier,
                    "-1",
                    conversionAccountAmsId,
                    internalCorrelationId,
                    "withdrawTheAmountFromConversionAccountInAms");
        } catch (JAXBException | JsonProcessingException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
        return null;
    }
}