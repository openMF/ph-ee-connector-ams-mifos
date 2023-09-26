package org.mifos.connector.ams.zeebe;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;
import org.mifos.connector.common.ams.dto.LoanRepaymentDTO;
import org.mifos.connector.common.channel.dto.TransactionChannelRequestDTO;
import org.mifos.connector.common.gsma.dto.CustomData;
import org.mifos.connector.common.gsma.dto.GsmaTransfer;
import org.mifos.connector.common.mojaloop.dto.*;
import org.mifos.connector.common.mojaloop.type.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ROLE;
import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSFER_ACTION;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.ACCOUNT_IDENTIFIER;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.ACCOUNT_NUMBER;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.CHANNEL_REQUEST;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.EXTERNAL_ACCOUNT_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.PARTY_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.PARTY_ID_TYPE;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.REQUESTED_DATE;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TENANT_ID;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.TRANSACTION_ID;
import static org.mifos.connector.common.ams.dto.TransferActionType.CREATE;

@Component
public class ZeebeUtil {


    private static Logger logger = LoggerFactory.getLogger(ZeebeUtil.class);
    private static ObjectMapper objectMapper = new ObjectMapper();
    @Value("#{'${accountPrefixes}'.split(',')}")
    public List<String> accountPrefixes;

    public static void zeebeVariablesToCamelProperties(Map<String, Object> variables, Exchange exchange, String... names) {
        exchange.setProperty("zeebeVariables", variables);

        for (String name : names) {
            Object value = variables.get(name);
            if (value == null) {
                logger.error("failed to find Zeebe variable name {}", name);
            } else {
                exchange.setProperty(name, value);
            }
        }
    }

    public static Map<String, Object> zeebeVariablesFrom(Exchange exchange) {
        return exchange.getProperty("zeebeVariables", Map.class);
    }

    public static <T> T zeebeVariable(Exchange exchange, String name, Class<T> clazz) throws Exception {
        Object content = zeebeVariablesFrom(exchange).get(name);
        if (content instanceof Map) {
            return objectMapper.readValue(objectMapper.writeValueAsString(content), clazz);
        }
        return (T) content;
    }

    public static String getCurrentDate(String requestedDate,String dateFormat) {
        String dateFormatGiven="yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
        LocalDateTime datetime = LocalDateTime.parse(requestedDate, DateTimeFormatter.ofPattern(dateFormatGiven));
        String newDate= datetime.format(DateTimeFormatter.ofPattern(dateFormat));
        return newDate;
    }

    public static LoanRepaymentDTO setLoanRepaymentBody(Exchange exchange) {
        String dateFormat = "dd MMMM yyyy";
        LoanRepaymentDTO loanRepaymentDTO = new LoanRepaymentDTO();
        loanRepaymentDTO.setDateFormat(dateFormat);
        loanRepaymentDTO.setLocale("en");
        loanRepaymentDTO.setTransactionAmount(exchange.getProperty("amount").toString());
        loanRepaymentDTO.setTransactionDate(exchange.getProperty("requestedDate").toString());
        return loanRepaymentDTO;
    }

    public static void setZeebeVariables(Exchange e, Map<String, Object> variables, String requestDate, String accountHoldingInstitutionId, String transactionChannelRequestDTO) {
        String dateFormat = "dd MMMM yyyy";
        String currentDate=getCurrentDate(requestDate,dateFormat);

        variables.put(ACCOUNT_IDENTIFIER,e.getProperty(ACCOUNT_IDENTIFIER));
        variables.put(ACCOUNT_NUMBER,e.getProperty(ACCOUNT_NUMBER));
//        variables.put(TRANSACTION_ID, UUID.randomUUID().toString());
        variables.put(TENANT_ID,accountHoldingInstitutionId);
        variables.put(TRANSFER_ACTION,CREATE.name());
        variables.put(CHANNEL_REQUEST,transactionChannelRequestDTO);
        variables.put(REQUESTED_DATE,currentDate);
        variables.put(EXTERNAL_ACCOUNT_ID,e.getProperty(ACCOUNT_NUMBER));
        variables.put("payeeTenantId", accountHoldingInstitutionId);
    }

    public static void setZeebeVariablesLoanWorker(Map<String, Object> variables, TransactionChannelRequestDTO transactionRequest) {
        TransactionType transactionType = new TransactionType();
        transactionType.setInitiator(TransactionRole.PAYEE);
        transactionType.setInitiatorType(InitiatorType.CONSUMER);
        transactionType.setScenario(Scenario.PAYMENT);
        transactionRequest.setTransactionType(transactionType);
        variables.put("initiator", transactionType.getInitiator().name());
        variables.put("initiatorType", transactionType.getInitiatorType().name());
        variables.put("scenario", transactionType.getScenario().name());
        variables.put("amount", new FspMoneyData(transactionRequest.getAmount().getAmountDecimal(),
                transactionRequest.getAmount().getCurrency()));
        variables.put("processType", "api");
    }

    public static void setExchangePropertyLoan(Exchange ex, String partyId, String partyIdType, TransactionChannelRequestDTO transactionRequest, Map<String, Object> existingVariables) throws JsonProcessingException {
        ex.setProperty(PARTY_ID_TYPE, partyIdType);
        ex.setProperty(PARTY_ID, partyId);

        ex.setProperty(CHANNEL_REQUEST, objectMapper.writeValueAsString(transactionRequest));
        ex.setProperty(TRANSACTION_ROLE, TransactionRole.PAYEE.name());
        ex.setProperty("amount",transactionRequest.getAmount().getAmountDecimal());
        ex.setProperty(ACCOUNT_NUMBER,existingVariables.get(ACCOUNT_NUMBER));
        ex.setProperty(EXTERNAL_ACCOUNT_ID,"L");
        ex.setProperty(REQUESTED_DATE,existingVariables.get("requestedDate"));
    }

    public Exchange setAccountTypeAndNumber(Exchange e, String accountNo) {
        String accountTypeIdentifier = "";
        String accountNumber = "";
        int accountNoLength = accountNo.length();
        // Separating account id and prefix
        for (String accountPrefix : accountPrefixes) {
            if (accountNo.startsWith(accountPrefix)) {
                accountNumber = accountNo.substring(accountPrefix.length(), accountNoLength);
                accountTypeIdentifier = accountNo.substring(0, accountPrefix.length());
                break;
            }
        }
        logger.debug("Accout number:{}, Identifier:{}", accountNumber, accountTypeIdentifier);
        e.setProperty(ACCOUNT_NUMBER, accountNumber);
        e.setProperty(ACCOUNT_IDENTIFIER, accountTypeIdentifier);
        return e;
    }

    public static String getValueofKey(List<CustomData> customData, String key) {
        for (CustomData obj : customData) {
            String keyString = obj.getKey();
            if (keyString.equalsIgnoreCase(key)) {
                return obj.getValue().toString();
            }
        }
        return null;
    }

    public static String convertGsmaTransfertoTransactionChannel(GsmaTransfer gsmaTransfer, Object property) throws JsonProcessingException {
        TransactionChannelRequestDTO transactionChannelRequestDTO = new TransactionChannelRequestDTO();
        String msisdn = gsmaTransfer.getPayer().get(0).getPartyIdIdentifier();
        String accountId = property.toString();

        String amount = gsmaTransfer.getAmount().trim();
        List<CustomData> customData = gsmaTransfer.getCustomData();
        String currency = gsmaTransfer.getCurrency();

        PartyIdInfo partyIdInfopayee = new PartyIdInfo(IdentifierType.ACCOUNT_ID, accountId);
        PartyIdInfo partyIdInfopayer = new PartyIdInfo(IdentifierType.MSISDN, msisdn);
        Party partyPayee = new Party(partyIdInfopayee);
        Party partyPayer = new Party(partyIdInfopayer);

        MoneyData moneyData = new MoneyData(amount, currency);

        transactionChannelRequestDTO.setAmount(moneyData);
        transactionChannelRequestDTO.setPayee(partyPayee);
        transactionChannelRequestDTO.setPayer(partyPayer);

        return objectMapper.writeValueAsString(transactionChannelRequestDTO);
    }
}
