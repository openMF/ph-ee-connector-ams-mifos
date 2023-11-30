package org.mifos.connector.ams.zeebe.workers.bookamount;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.mifos.connector.ams.fineract.Config;
import org.mifos.connector.ams.fineract.ConfigFactory;
import org.mifos.connector.ams.log.EventLogUtil;
import org.mifos.connector.ams.log.LogInternalCorrelationId;
import org.mifos.connector.ams.log.TraceZeebeArguments;
import org.mifos.connector.ams.mapstruct.Pain001Camt053Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.AuthTokenHelper;
import org.mifos.connector.ams.zeebe.workers.utils.BatchItemBuilder;
import org.mifos.connector.ams.zeebe.workers.utils.ContactDetailsUtil;
import org.mifos.connector.ams.zeebe.workers.utils.DtSavingsTransactionDetails;
import org.mifos.connector.ams.zeebe.workers.utils.HoldAmountBody;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionBody;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionItem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import com.baasflow.commons.events.Event;
import com.baasflow.commons.events.EventService;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import hu.dpc.rt.utils.converter.AccountSchemeName1Choice;
import hu.dpc.rt.utils.converter.GenericAccountIdentification1;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import iso.std.iso._20022.tech.json.camt_053_001.ActiveOrHistoricCurrencyAndAmount;
import iso.std.iso._20022.tech.json.camt_053_001.AmountAndCurrencyExchange3;
import iso.std.iso._20022.tech.json.camt_053_001.AmountAndCurrencyExchangeDetails3;
import iso.std.iso._20022.tech.json.camt_053_001.BankToCustomerStatementV08;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import iso.std.iso._20022.tech.json.camt_053_001.ReportEntry10;
import iso.std.iso._20022.tech.json.camt_053_001.ActiveOrHistoricCurrencyAndAmountRange2.CreditDebitCode;
import iso.std.iso._20022.tech.json.pain_001_001.CustomerCreditTransferInitiationV10;
import iso.std.iso._20022.tech.json.pain_001_001.Pain00100110CustomerCreditTransferInitiationV10MessageSchema;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class OnUsTransferWorker extends AbstractMoneyInOutWorker {

    @Autowired
    private Pain001Camt053Mapper camt053Mapper;

    @Value("${fineract.incoming-money-api}")
    protected String incomingMoneyApi;

    @Autowired
    private ConfigFactory paymentTypeConfigFactory;

    @Autowired
    private BatchItemBuilder batchItemBuilder;
    
    @Autowired
    private ContactDetailsUtil contactDetailsUtil;
    
    @Autowired
    private AuthTokenHelper authTokenHelper;

    @Autowired
    private EventService eventService;
    
    @Autowired
    private ObjectMapper objectMapper;

    private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);

    @JobWorker
    @LogInternalCorrelationId
    @TraceZeebeArguments
    public Map<String, Object> transferTheAmountBetweenDisposalAccounts(JobClient jobClient,
                                                                        ActivatedJob activatedJob,
                                                                        @Variable String internalCorrelationId,
                                                                        @Variable String paymentScheme,
                                                                        @Variable String originalPain001,
                                                                        @Variable BigDecimal amount,
                                                                        @Variable Integer creditorDisposalAccountAmsId,
                                                                        @Variable Integer debtorDisposalAccountAmsId,
                                                                        @Variable Integer debtorConversionAccountAmsId,
                                                                        @Variable BigDecimal transactionFeeAmount,
                                                                        @Variable String tenantIdentifier,
                                                                        @Variable String transactionGroupId,
                                                                        @Variable String transactionCategoryPurposeCode,
                                                                        @Variable String transactionFeeCategoryPurposeCode,
                                                                        @Variable String transactionFeeInternalCorrelationId,
                                                                        @Variable String creditorIban,
                                                                        @Variable String debtorIban,
                                                                        @Variable String debtorInternalAccountId,
                                                                        @Variable String creditorInternalAccountId) {
        log.info("transferTheAmountBetweenDisposalAccounts");
        return eventService.auditedEvent(
                eventBuilder -> EventLogUtil.initZeebeJob(activatedJob, "transferTheAmountBetweenDisposalAccounts", internalCorrelationId, transactionGroupId, eventBuilder),
                eventBuilder -> transferTheAmountBetweenDisposalAccounts(internalCorrelationId,
                        paymentScheme,
                        originalPain001,
                        amount,
                        creditorDisposalAccountAmsId,
                        debtorDisposalAccountAmsId,
                        debtorConversionAccountAmsId,
                        transactionFeeAmount,
                        tenantIdentifier,
                        transactionGroupId,
                        transactionCategoryPurposeCode,
                        transactionFeeCategoryPurposeCode,
                        transactionFeeInternalCorrelationId,
                        creditorIban,
                        debtorIban,
                        debtorInternalAccountId,
                        creditorInternalAccountId,
                        eventBuilder));
    }

    @SuppressWarnings("unchecked")
	private Map<String, Object> transferTheAmountBetweenDisposalAccounts(String internalCorrelationId,
                                                                         String paymentScheme,
                                                                         String originalPain001,
                                                                         BigDecimal amount,
                                                                         Integer creditorDisposalAccountAmsId,
                                                                         Integer debtorDisposalAccountAmsId,
                                                                         Integer debtorConversionAccountAmsId,
                                                                         BigDecimal transactionFeeAmount,
                                                                         String tenantIdentifier,
                                                                         String transactionGroupId,
                                                                         String transactionCategoryPurposeCode,
                                                                         String transactionFeeCategoryPurposeCode,
                                                                         String transactionFeeInternalCorrelationId,
                                                                         String creditorIban,
                                                                         String debtorIban,
                                                                         String debtorInternalAccountId,
                                                                         String creditorInternalAccountId,
                                                                         Event.Builder eventBuilder) {
try {
			
			log.debug("Incoming pain.001: {}", originalPain001);
			
			Config paymentTypeConfig = paymentTypeConfigFactory.getConfig(tenantIdentifier);
			objectMapper.setSerializationInclusion(Include.NON_NULL);
			Pain00100110CustomerCreditTransferInitiationV10MessageSchema pain001 = objectMapper.readValue(originalPain001, Pain00100110CustomerCreditTransferInitiationV10MessageSchema.class);
			
			BankToCustomerStatementV08 convertedStatement = camt053Mapper.toCamt053Entry(pain001.getDocument());
			ReportEntry10 convertedcamt053Entry = convertedStatement.getStatement().get(0).getEntry().get(0);
			AmountAndCurrencyExchangeDetails3 withAmountAndCurrency = new AmountAndCurrencyExchangeDetails3()
					.withAmount(new ActiveOrHistoricCurrencyAndAmount()
							.withAmount(amount.add(transactionFeeAmount))
							.withCurrency(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getAmount().getInstructedAmount().getCurrency()));
			convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).withAmountDetails(
					new AmountAndCurrencyExchange3()
							.withTransactionAmount(withAmountAndCurrency)
							.withInstructedAmount(withAmountAndCurrency));
			
			GenericAccountIdentification1 debtorAccountIdOther = (GenericAccountIdentification1) convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getRelatedParties().getDebtorAccount().getIdentification().getAdditionalProperties().getOrDefault("Other", GenericAccountIdentification1.builder().build());
			debtorAccountIdOther.setId(debtorInternalAccountId);
			debtorAccountIdOther.setSchemeName(AccountSchemeName1Choice.builder().code("IAID").build());
			convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getRelatedParties().getDebtorAccount().getIdentification().setAdditionalProperty("Other", debtorAccountIdOther);
			GenericAccountIdentification1 creditorAccountIdOther = (GenericAccountIdentification1) convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getRelatedParties().getCreditorAccount().getIdentification().getAdditionalProperties().getOrDefault("Other", GenericAccountIdentification1.builder().build());
			creditorAccountIdOther.setId(creditorInternalAccountId);
			creditorAccountIdOther.setSchemeName(AccountSchemeName1Choice.builder().code("IAID").build());
			convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getRelatedParties().getCreditorAccount().getIdentification().setAdditionalProperty("Other", creditorAccountIdOther);
			
			String interbankSettlementDate = LocalDate.now().format(PATTERN);
			
			boolean hasFee = !BigDecimal.ZERO.equals(transactionFeeAmount);
			
            batchItemBuilder.tenantId(tenantIdentifier);
            List<TransactionItem> items = new ArrayList<>();
            
            String holdTransactionUrl = String.format("%s%d/transactions?command=holdAmount", incomingMoneyApi.substring(1), debtorDisposalAccountAmsId);

			Integer outHoldReasonId = paymentTypeConfig.findPaymentTypeIdByOperation(String.format("%s.%s", paymentScheme, "outHoldReasonId"));
			HoldAmountBody body = new HoldAmountBody(
					interbankSettlementDate,
					hasFee ? amount.add(transactionFeeAmount) : amount,
	                outHoldReasonId,
	                locale,
	                FORMAT
	        );
			
			objectMapper.setSerializationInclusion(Include.NON_NULL);
    		String bodyItem = objectMapper.writeValueAsString(body);
    		
    		batchItemBuilder.add(items, holdTransactionUrl, bodyItem, false);
    		
			String camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";
			
			String partnerName = pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getName();
			String partnerAccountIban = pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditorAccount().getIdentification().getIban();
			String partnerAccountSecondaryIdentifier = contactDetailsUtil.getId(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getContactDetails());
			String unstructured = Optional.ofNullable(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getRemittanceInformation())
					.map(iso.std.iso._20022.tech.json.pain_001_001.RemittanceInformation16::getUnstructured).map(List::toString).orElse("");
    		
    		addDetails(internalCorrelationId, transactionCategoryPurposeCode, internalCorrelationId, 
					objectMapper, batchItemBuilder, items, pain001.getDocument(), convertedcamt053Entry, camt053RelativeUrl, debtorIban, 
					paymentTypeConfig, paymentScheme, null, partnerName, partnerAccountIban, 
					partnerAccountSecondaryIdentifier, unstructured, null, null);
    		
			Long lastHoldTransactionId = holdBatch(items, tenantIdentifier, debtorDisposalAccountAmsId, creditorDisposalAccountAmsId, internalCorrelationId, "transferToConversionAccountInAms");
			
			HttpHeaders httpHeaders = new HttpHeaders();
			httpHeaders.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
			httpHeaders.set("Authorization", authTokenHelper.generateAuthToken());
			httpHeaders.set("Fineract-Platform-TenantId", tenantIdentifier);
			LinkedHashMap<String, Object> accountDetails = restTemplate.exchange(
					String.format("%s/%s%d", fineractApiUrl, incomingMoneyApi.substring(1), debtorDisposalAccountAmsId), 
					HttpMethod.GET, 
					new HttpEntity<>(httpHeaders), 
					LinkedHashMap.class)
				.getBody();
			LinkedHashMap<String, Object> summary = (LinkedHashMap<String, Object>) accountDetails.get("summary");
			BigDecimal availableBalance = new BigDecimal(summary.get("availableBalance").toString());
			if (availableBalance.signum() < 0) {
				restTemplate.exchange(
					String.format("%s/%ssavingsaccounts/%d/transactions/%d?command=releaseAmount", fineractApiUrl, incomingMoneyApi.substring(1), debtorDisposalAccountAmsId, lastHoldTransactionId),
					HttpMethod.POST,
					new HttpEntity<>(httpHeaders),
					Object.class
				);
				throw new ZeebeBpmnError("Error_InsufficientFunds", "Insufficient funds");
			}
			
			items.clear();
			
			String releaseTransactionUrl = String.format("%s%d/transactions/%d?command=releaseAmount", incomingMoneyApi.substring(1), debtorDisposalAccountAmsId, lastHoldTransactionId);
			batchItemBuilder.add(items, releaseTransactionUrl, null, false);
			String releaseAmountOperation = "transferTheAmountBetweenDisposalAccounts.Debtor.DisposalAccount.ReleaseTransactionAmount";
			addDetails(internalCorrelationId, transactionCategoryPurposeCode, internalCorrelationId, 
					objectMapper, batchItemBuilder, items, pain001.getDocument(), convertedcamt053Entry, camt053RelativeUrl, debtorIban, 
					paymentTypeConfig, paymentScheme, releaseAmountOperation, partnerName, partnerAccountIban, 
					partnerAccountSecondaryIdentifier, unstructured, null, null);
			
    		String debtorDisposalWithdrawalRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), debtorDisposalAccountAmsId, "withdrawal");
    		
    		String withdrawAmountOperation = "transferTheAmountBetweenDisposalAccounts.Debtor.DisposalAccount.WithdrawTransactionAmount";
			String withdrawAmountConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawAmountOperation);
			Integer paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawAmountConfigOperationKey);
			String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawAmountConfigOperationKey);
    		
    		TransactionBody transactionBody = new TransactionBody(
    				interbankSettlementDate,
    				amount,
    				paymentTypeId,
    				"",
    				FORMAT,
    				locale);
    		
    		bodyItem = objectMapper.writeValueAsString(transactionBody);
    		
    		
    		batchItemBuilder.add(items, debtorDisposalWithdrawalRelativeUrl, bodyItem, false);
    		
    		convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getAmountDetails().getInstructedAmount().getAmount().setAmount(amount);
    		convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getAmountDetails().getTransactionAmount().getAmount().setAmount(amount);
    		convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
    		convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.DBIT);
    		String camt053Entry = objectMapper.writeValueAsString(convertedcamt053Entry);
    	
    		camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";
    		
    		DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
    				internalCorrelationId,
    				camt053Entry,
    				pain001.getDocument().getPaymentInformation().get(0).getDebtorAccount().getIdentification().getIban(),
    				paymentTypeCode,
    				transactionGroupId,
    				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getName(),
    				partnerAccountIban,
    				partnerAccountIban.substring(partnerAccountIban.length() - 8),
    				contactDetailsUtil.getId(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getContactDetails()),
    				Optional.ofNullable(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getRemittanceInformation())
							.map(iso.std.iso._20022.tech.json.pain_001_001.RemittanceInformation16::getUnstructured).map(List::toString).orElse(""),
    				transactionCategoryPurposeCode,
    				paymentScheme,
    				debtorDisposalAccountAmsId,
    				creditorDisposalAccountAmsId);
    		
    		String camt053Body = objectMapper.writeValueAsString(td);

    		batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
    		
			if (hasFee) {
				String withdrawFeeDisposalOperation = "transferTheAmountBetweenDisposalAccounts.Debtor.DisposalAccount.WithdrawTransactionFee";
				String withdrawFeeDisposalConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawFeeDisposalOperation);
				paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawFeeDisposalConfigOperationKey);
				paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawFeeDisposalConfigOperationKey);
	    		
				transactionBody = new TransactionBody(
	    				interbankSettlementDate,
	    				transactionFeeAmount,
	    				paymentTypeId,
	    				"",
	    				FORMAT,
	    				locale);
	    		
	    		bodyItem = objectMapper.writeValueAsString(transactionBody);
	    		
	    		batchItemBuilder.add(items, debtorDisposalWithdrawalRelativeUrl, bodyItem, false);
	    	
	    		convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getSupplementaryData().get(0).getEnvelope().setAdditionalProperty("InternalCorrelationId", transactionFeeInternalCorrelationId);
	    		convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getAmountDetails().getInstructedAmount().getAmount().setAmount(transactionFeeAmount);
	    		convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getAmountDetails().getTransactionAmount().getAmount().setAmount(transactionFeeAmount);
	    		convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
	    		convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getSupplementaryData().clear();
	    		camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001.getDocument(), convertedcamt053Entry, transactionFeeCategoryPurposeCode);
	    		camt053Mapper.moveOtherIdentificationToSupplementaryData(convertedcamt053Entry, pain001.getDocument());
	    		camt053Entry = objectMapper.writeValueAsString(convertedcamt053Entry);
	    		
	    		td = new DtSavingsTransactionDetails(
	    				transactionFeeInternalCorrelationId,
	    				camt053Entry,
	    				pain001.getDocument().getPaymentInformation().get(0).getDebtorAccount().getIdentification().getIban(),
	    				paymentTypeCode,
	    				transactionGroupId,
	    				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getName(),
	    				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditorAccount().getIdentification().getIban(),
	    				creditorIban.substring(creditorIban.length() - 8),
	    				contactDetailsUtil.getId(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getContactDetails()),
	    				Optional.ofNullable(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getRemittanceInformation())
								.map(iso.std.iso._20022.tech.json.pain_001_001.RemittanceInformation16::getUnstructured).map(List::toString).orElse(""),
	    				transactionFeeCategoryPurposeCode,
	    				paymentScheme,
	    				debtorDisposalAccountAmsId,
	    				debtorConversionAccountAmsId);
	    		
	    		camt053Body = objectMapper.writeValueAsString(td);
	    		batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);

	    		
	    		
				String depositFeeOperation = "transferTheAmountBetweenDisposalAccounts.Debtor.ConversionAccount.DepositTransactionFee";
				String depositFeeConfigOperationKey = String.format("%s.%s", paymentScheme, depositFeeOperation);
				paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(depositFeeConfigOperationKey);
				paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(depositFeeConfigOperationKey);
				convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
				convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getSupplementaryData().clear();
				convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.CRDT);
				camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001.getDocument(), convertedcamt053Entry, transactionFeeCategoryPurposeCode);
				camt053Mapper.moveOtherIdentificationToSupplementaryData(convertedcamt053Entry, pain001.getDocument());
				camt053Entry = objectMapper.writeValueAsString(convertedcamt053Entry);
	    		
				transactionBody = new TransactionBody(
	    				interbankSettlementDate,
	    				transactionFeeAmount,
	    				paymentTypeId,
	    				"",
	    				FORMAT,
	    				locale);
			    		
	    		bodyItem = objectMapper.writeValueAsString(transactionBody);
	    		
	    		String debtorConversionDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), debtorConversionAccountAmsId, "deposit");
		    		
	    		batchItemBuilder.add(items, debtorConversionDepositRelativeUrl, bodyItem, false);
		    	
	    		td = new DtSavingsTransactionDetails(
	    				transactionFeeInternalCorrelationId,
	    				camt053Entry,
	    				pain001.getDocument().getPaymentInformation().get(0).getDebtorAccount().getIdentification().getIban(),
	    				paymentTypeCode,
	    				transactionGroupId,
	    				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getName(),
	    				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditorAccount().getIdentification().getIban(),
	    				partnerAccountIban.substring(partnerAccountIban.length() - 8),
	    				contactDetailsUtil.getId(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getContactDetails()),
	    				Optional.ofNullable(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getRemittanceInformation())
								.map(iso.std.iso._20022.tech.json.pain_001_001.RemittanceInformation16::getUnstructured).map(List::toString).orElse(""),
	    				transactionFeeCategoryPurposeCode,
	    				paymentScheme,
	    				debtorDisposalAccountAmsId,
	    				debtorConversionAccountAmsId);
	    		
	    		camt053Body = objectMapper.writeValueAsString(td);
	    		batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			}
			
			String depositAmountOperation = "transferTheAmountBetweenDisposalAccounts.Creditor.DisposalAccount.DepositTransactionAmount";
			String depositAmountConfigOperationKey = String.format("%s.%s", paymentScheme, depositAmountOperation);
			paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(depositAmountConfigOperationKey);
			paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(depositAmountConfigOperationKey);
			convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
			convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getSupplementaryData().clear();
			camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001.getDocument(), convertedcamt053Entry, transactionCategoryPurposeCode);
			camt053Mapper.moveOtherIdentificationToSupplementaryData(convertedcamt053Entry, pain001.getDocument());
    		
			transactionBody = new TransactionBody(
    				interbankSettlementDate,
    				amount,
    				paymentTypeId,
    				"",
    				FORMAT,
    				locale);
		    		
    		bodyItem = objectMapper.writeValueAsString(transactionBody);
    		
    		String creditorDisposalDepositRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), creditorDisposalAccountAmsId, "deposit");
	    		
    		batchItemBuilder.add(items, creditorDisposalDepositRelativeUrl, bodyItem, false);
	    	
    		convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getSupplementaryData().get(0).getEnvelope().setAdditionalProperty("InternalCorrelationId", internalCorrelationId);
    		convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getAmountDetails().getInstructedAmount().getAmount().setAmount(amount);
    		convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getAmountDetails().getTransactionAmount().getAmount().setAmount(amount);
    		convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.CRDT);
    		camt053Entry = objectMapper.writeValueAsString(convertedcamt053Entry);
			
    		td = new DtSavingsTransactionDetails(
    				transactionFeeInternalCorrelationId,
    				camt053Entry,
    				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditorAccount().getIdentification().getIban(),
    				paymentTypeCode,
    				transactionGroupId,
    				pain001.getDocument().getPaymentInformation().get(0).getDebtor().getName(),
    				pain001.getDocument().getPaymentInformation().get(0).getDebtorAccount().getIdentification().getIban(),
    				debtorIban.substring(debtorIban.length() - 8),
    				contactDetailsUtil.getId(pain001.getDocument().getPaymentInformation().get(0).getDebtor().getContactDetails()),
    				Optional.ofNullable(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getRemittanceInformation())
							.map(iso.std.iso._20022.tech.json.pain_001_001.RemittanceInformation16::getUnstructured).map(List::toString).orElse(""),
    				transactionCategoryPurposeCode,
    				paymentScheme,
    				debtorDisposalAccountAmsId,
    				creditorDisposalAccountAmsId);
    		
    		camt053Body = objectMapper.writeValueAsString(td);
    		batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			
	    		
			if (hasFee) {
	    		String withdrawFeeConversionOperation = "transferTheAmountBetweenDisposalAccounts.Debtor.ConversionAccount.WithdrawTransactionFee";
				String withdrawFeeConversionConfigOperationKey = String.format("%s.%s", paymentScheme, withdrawFeeConversionOperation);
				paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(withdrawFeeConversionConfigOperationKey);
				paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(withdrawFeeConversionConfigOperationKey);
				convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
				convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getSupplementaryData().clear();
				convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setCreditDebitIndicator(CreditDebitCode.DBIT);
				camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001.getDocument(), convertedcamt053Entry, transactionFeeCategoryPurposeCode);
				camt053Mapper.moveOtherIdentificationToSupplementaryData(convertedcamt053Entry, pain001.getDocument());
	    		
				transactionBody = new TransactionBody(
	    				interbankSettlementDate,
	    				transactionFeeAmount,
	    				paymentTypeId,
	    				"",
	    				FORMAT,
	    				locale);
			    		
	    		bodyItem = objectMapper.writeValueAsString(transactionBody);
	    		
	    		String debtorConversionWithdrawRelativeUrl = String.format("%s%d/transactions?command=%s", incomingMoneyApi.substring(1), debtorConversionAccountAmsId, "withdrawal");
		    		
	    		batchItemBuilder.add(items, debtorConversionWithdrawRelativeUrl, bodyItem, false);
		    	
	    		convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getSupplementaryData().get(0).getEnvelope().setAdditionalProperty("InternalCorrelationId", transactionFeeInternalCorrelationId);
	    		convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getAmountDetails().getInstructedAmount().getAmount().setAmount(transactionFeeAmount);
	    		convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getAmountDetails().getTransactionAmount().getAmount().setAmount(transactionFeeAmount);
	    		convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getSupplementaryData().clear();
	    		camt053Mapper.fillAdditionalPropertiesByPurposeCode(pain001.getDocument(), convertedcamt053Entry, transactionFeeCategoryPurposeCode);
	    		camt053Mapper.moveOtherIdentificationToSupplementaryData(convertedcamt053Entry, pain001.getDocument());
	    		camt053Entry = objectMapper.writeValueAsString(convertedcamt053Entry);
				
				td = new DtSavingsTransactionDetails(
	    				transactionFeeInternalCorrelationId,
	    				camt053Entry,
	    				pain001.getDocument().getPaymentInformation().get(0).getDebtorAccount().getIdentification().getIban(),
	    				paymentTypeCode,
	    				transactionGroupId,
	    				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getName(),
	    				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditorAccount().getIdentification().getIban(),
	    				null,
	    				contactDetailsUtil.getId(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditor().getContactDetails()),
	    				Optional.ofNullable(pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getRemittanceInformation())
								.map(iso.std.iso._20022.tech.json.pain_001_001.RemittanceInformation16::getUnstructured).map(List::toString).orElse(""),
	    				transactionFeeCategoryPurposeCode,
	    				paymentScheme,
	    				debtorConversionAccountAmsId,
	    				null);
	    		
	    		camt053Body = objectMapper.writeValueAsString(td);
			    		
	    		batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
			}
			
			doBatchOnUs(items,
                    tenantIdentifier,
                    debtorDisposalAccountAmsId,
                    debtorConversionAccountAmsId,
                    creditorDisposalAccountAmsId,
                    internalCorrelationId);
			
			return Map.of("transactionDate", interbankSettlementDate);
		} catch (JsonProcessingException e) {
			log.error(e.getMessage(), e);
			throw new RuntimeException("failed to create camt.053", e);
		} catch (ZeebeBpmnError e) {
			throw e;
		} catch (RuntimeException re) {
			throw re;
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
    }
    
    private void addDetails(String transactionGroupId, 
			String transactionFeeCategoryPurposeCode,
			String internalCorrelationId, 
			ObjectMapper om, 
			BatchItemBuilder batchItemBuilder, 
			List<TransactionItem> items,
			CustomerCreditTransferInitiationV10 pain001,
			ReportEntry10 convertedcamt053Entry, 
			String camt053RelativeUrl,
			String accountIban,
			Config paymentTypeConfig,
			String paymentScheme,
			String paymentTypeOperation,
			String partnerName,
			String partnerAccountIban,
			String partnerAccountSecondaryIdentifier,
			String unstructured,
			Integer sourceAmsAccountId,
			Integer targetAmsAccountId) throws JsonProcessingException {
    	String paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(String.format("%s.%s", paymentScheme, paymentTypeOperation));
    	if (paymentTypeCode == null) {
    		paymentTypeCode = "";
    	}
    	convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).setAdditionalTransactionInformation(paymentTypeCode);
    	camt053Mapper.moveOtherIdentificationToSupplementaryData(convertedcamt053Entry, pain001);
    	String camt053 = objectMapper.writeValueAsString(convertedcamt053Entry);
		DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
				internalCorrelationId,
				camt053,
				accountIban,
				paymentTypeCode,
				transactionGroupId,
				partnerName,
				partnerAccountIban,
				partnerAccountIban.substring(partnerAccountIban.length() - 8),
				partnerAccountSecondaryIdentifier,
				unstructured,
				transactionFeeCategoryPurposeCode,
				paymentScheme,
				sourceAmsAccountId,
				targetAmsAccountId);
		
		String camt053Body = om.writeValueAsString(td);
		batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);
    }
}