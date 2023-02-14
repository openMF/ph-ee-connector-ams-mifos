package org.mifos.connector.ams.zeebe.workers.bookamount;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.nets.realtime247.ri_2015_10.ObjectFactory;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import iso.std.iso._20022.tech.xsd.pacs_002_001.Document;
import iso.std.iso20022plus.tech.json.camt_052_001.BankToCustomerAccountReportV08;
import org.jboss.logging.MDC;
import org.mifos.connector.ams.mapstruct.Pacs008Camt052Mapper;
import org.mifos.connector.ams.zeebe.workers.utils.TransactionDetails;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.util.UriComponentsBuilder;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.datatype.XMLGregorianCalendar;
import java.io.StringReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

@Component
public class BookCreditedAmountToConversionAccountWorker extends AbstractMoneyInOutWorker {

    @Autowired
    private Pacs008Camt052Mapper camt052Mapper;

    private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern(FORMAT);

    @Override
    @SuppressWarnings("unchecked")
    public void handle(JobClient jobClient, ActivatedJob activatedJob) throws Exception {
        try {
            Map<String, Object> variables = activatedJob.getVariablesAsMap();

            logger.info("Incoming money worker started with variables");
            variables.keySet().forEach(logger::info);

            String originalPacs008 = (String) variables.get("originalPacs008");
            JAXBContext jc = JAXBContext.newInstance(ObjectFactory.class,
                    iso.std.iso._20022.tech.xsd.pacs_008_001.ObjectFactory.class,
                    iso.std.iso._20022.tech.xsd.pacs_002_001.ObjectFactory.class);
            JAXBElement<iso.std.iso._20022.tech.xsd.pacs_008_001.Document> object = (JAXBElement<iso.std.iso._20022.tech.xsd.pacs_008_001.Document>) jc.createUnmarshaller().unmarshal(new StringReader(originalPacs008));
            iso.std.iso._20022.tech.xsd.pacs_008_001.Document pacs008 = object.getValue();

            String internalCorrelationId = (String) variables.get("internalCorrelationId");
            MDC.put("internalCorrelationId", internalCorrelationId);
            String transactionDate;
            String tenantId = (String) variables.get("tenantIdentifier");

            if (isIG2(pacs008)) {

                logger.info("Worker to book incoming IG2 money in AMS has started with incoming pacs.008 {} for tenant {}", originalPacs008, tenantId);

                XMLGregorianCalendar acceptanceDate = pacs008.getFIToFICstmrCdtTrf().getGrpHdr().getCreDtTm();
                transactionDate = acceptanceDate.toGregorianCalendar().toZonedDateTime().toLocalDate().format(PATTERN);

            } else if (isHctInts(pacs008)) {

                String originalPacs002 = (String) variables.get("originalPacs002");
                logger.info("Worker to book incoming hct-inst money in AMS has started with incoming pacs.002 {} for tenant {}", originalPacs002, tenantId);

                JAXBElement<Document> jaxbObject = (JAXBElement<Document>) jc.createUnmarshaller().unmarshal(new StringReader(originalPacs002));
                Document pacs_002 = jaxbObject.getValue();

                XMLGregorianCalendar acceptanceDate = pacs_002.getFIToFIPmtStsRpt().getTxInfAndSts().get(0).getAccptncDtTm();
                transactionDate = acceptanceDate.toGregorianCalendar().toZonedDateTime().toLocalDate().format(PATTERN);

            } else {
                jobClient.newThrowErrorCommand(activatedJob.getKey()).errorCode("Error_InvalidCreditTransferType").send();
                return;
            }


            Object amount = variables.get("amount");

            Integer conversionAccountAmsId = (Integer) variables.get("conversionAccountAmsId");

            ResponseEntity<Object> responseObject = deposit(transactionDate, amount, conversionAccountAmsId, 1, tenantId);
            
            
            if (HttpStatus.OK.equals(responseObject.getStatusCode())) {
                logger.info("Worker to book incoming money in AMS has finished successfully");
                
                ObjectMapper om = new ObjectMapper();
                BankToCustomerAccountReportV08 convertedCamt052 = camt052Mapper.toCamt052(pacs008);
                String camt052 = om.writeValueAsString(convertedCamt052);
                
                postCamt052(tenantId, camt052, internalCorrelationId, responseObject);
                
                jobClient.newCompleteCommand(activatedJob.getKey()).variables(variables).send();
            } else {
                logger.error("Worker to book incoming money in AMS has failed, dispatching user task to handle fiat deposit");
                jobClient.newThrowErrorCommand(activatedJob.getKey()).errorCode("Error_BookToConversionToBeHandledManually").send();
            }
        } catch (Exception e) {
            logger.error("Worker to book incoming money in AMS has failed, dispatching user task to handle fiat deposit", e);
            jobClient.newThrowErrorCommand(activatedJob.getKey()).errorCode("Error_BookToConversionToBeHandledManually").send();
        } finally {
            MDC.remove("internalCorrelationId");
        }
    }

    private boolean isIG2(iso.std.iso._20022.tech.xsd.pacs_008_001.Document creditTransfer) {
        Optional<String> sttlmMtd = nullSafe(() -> creditTransfer.getFIToFICstmrCdtTrf().getGrpHdr().getSttlmInf().getSttlmMtd().value());
        Optional<String> clrSys = nullSafe(() -> creditTransfer.getFIToFICstmrCdtTrf().getGrpHdr().getSttlmInf().getClrSys().getPrtry());
        if (sttlmMtd.isEmpty() || clrSys.isEmpty()) {
            return false;
        }
        return sttlmMtd.get().equals("CLRG") && clrSys.get().equals("IG2");
    }

    private boolean isHctInts(iso.std.iso._20022.tech.xsd.pacs_008_001.Document creditTransfer) {
        Optional<String> svcLvl = nullSafe(() -> creditTransfer.getFIToFICstmrCdtTrf().getGrpHdr().getPmtTpInf().getSvcLvl().getCd());
        Optional<String> lclInstrm = nullSafe(() -> creditTransfer.getFIToFICstmrCdtTrf().getGrpHdr().getPmtTpInf().getLclInstrm().getCd());
        if (svcLvl.isEmpty() || lclInstrm.isEmpty()) {
            return false;
        }
        return svcLvl.get().equals("SEPA") && lclInstrm.get().equals("INST");
    }

    public static <T> Optional<T> nullSafe(Supplier<T> resolver) {
        try {
            T result = resolver.get();
            return Optional.ofNullable(result);
        } catch (NullPointerException | IndexOutOfBoundsException e) {
            return Optional.empty();
        }
    }
}