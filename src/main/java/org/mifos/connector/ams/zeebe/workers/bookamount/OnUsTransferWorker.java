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
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import iso.std.iso._20022.tech.json.camt_053_001.ReportEntry10;
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
			
			ReportEntry10 convertedcamt053Entry = camt053Mapper.toCamt053Entry(pain001.getDocument());
			GenericAccountIdentification1 debtorAccountIdOther = (GenericAccountIdentification1) convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getRelatedParties().getDebtorAccount().getIdentification().getAdditionalProperties().getOrDefault("Other", GenericAccountIdentification1.builder().build());
			debtorAccountIdOther.id(debtorInternalAccountId);
			debtorAccountIdOther.schemeName(AccountSchemeName1Choice.builder().code("IAID").build());
			GenericAccountIdentification1 creditorAccountIdOther = (GenericAccountIdentification1) convertedcamt053Entry.getEntryDetails().get(0).getTransactionDetails().get(0).getRelatedParties().getCreditorAccount().getIdentification().getAdditionalProperties().getOrDefault("Other", GenericAccountIdentification1.builder().build());
			creditorAccountIdOther.id(creditorInternalAccountId);
			creditorAccountIdOther.schemeName(AccountSchemeName1Choice.builder().code("IAID").build());
			String camt053Entry = objectMapper.writeValueAsString(convertedcamt053Entry);
			
			String interbankSettlementDate = LocalDate.now().format(PATTERN);
			
			boolean hasFee = !BigDecimal.ZERO.equals(transactionFeeAmount);
			
            batchItemBuilder.tenantId(tenantIdentifier);
            
            List<TransactionItem> items = new ArrayList<>();
            
            String holdTransactionUrl = String.format("%s%d/transactions/command=holdAmount", incomingMoneyApi.substring(1), debtorDisposalAccountAmsId);

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
    		
			String holdAmountOperation = "transferTheAmountBetweenDisposalAccounts.Debtor.DisposalAccount.HoldTransactionAmount";
    		addDetails(internalCorrelationId, transactionCategoryPurposeCode, internalCorrelationId, 
					objectMapper, batchItemBuilder, items, camt053Entry, camt053RelativeUrl, debtorIban, 
					paymentTypeConfig, paymentScheme, holdAmountOperation, partnerName, partnerAccountIban, 
					partnerAccountSecondaryIdentifier, unstructured, debtorDisposalAccountAmsId, creditorDisposalAccountAmsId);
    		
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
					objectMapper, batchItemBuilder, items, camt053Entry, camt053RelativeUrl, debtorIban, 
					paymentTypeConfig, paymentScheme, releaseAmountOperation, partnerName, partnerAccountIban, 
					partnerAccountSecondaryIdentifier, unstructured, debtorDisposalAccountAmsId, creditorDisposalAccountAmsId);
    		
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
    	
    		camt053RelativeUrl = "datatables/dt_savings_transaction_details/$.resourceId";
    		
    		DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
    				internalCorrelationId,
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
	    				debtorDisposalAccountAmsId,
	    				debtorConversionAccountAmsId);
	    		
	    		camt053Body = objectMapper.writeValueAsString(td);
	    		batchItemBuilder.add(items, camt053RelativeUrl, camt053Body, true);

	    		
	    		
				String depositFeeOperation = "transferTheAmountBetweenDisposalAccounts.Debtor.ConversionAccount.DepositTransactionFee";
				String depositFeeConfigOperationKey = String.format("%s.%s", paymentScheme, depositFeeOperation);
				paymentTypeId = paymentTypeConfig.findPaymentTypeIdByOperation(depositFeeConfigOperationKey);
				paymentTypeCode = paymentTypeConfig.findPaymentTypeCodeByOperation(depositFeeConfigOperationKey);
	    		
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
	    				null,
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
			camt053Entry = objectMapper.writeValueAsString(convertedcamt053Entry);
			
    		td = new DtSavingsTransactionDetails(
    				transactionFeeInternalCorrelationId,
    				camt053Entry,
    				pain001.getDocument().getPaymentInformation().get(0).getCreditTransferTransactionInformation().get(0).getCreditorAccount().getIdentification().getIban(),
    				paymentTypeCode,
    				transactionGroupId,
    				pain001.getDocument().getPaymentInformation().get(0).getDebtor().getName(),
    				pain001.getDocument().getPaymentInformation().get(0).getDebtorAccount().getIdentification().getIban(),
    				null,
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
		}
    }
    
    private void addDetails(String transactionGroupId, 
			String transactionFeeCategoryPurposeCode,
			String internalCorrelationId, 
			ObjectMapper om, 
			BatchItemBuilder batchItemBuilder, 
			List<TransactionItem> items,
			String camt053, 
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
		DtSavingsTransactionDetails td = new DtSavingsTransactionDetails(
				internalCorrelationId,
				camt053,
				accountIban,
				paymentTypeCode,
				transactionGroupId,
				partnerName,
				partnerAccountIban,
				null,
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