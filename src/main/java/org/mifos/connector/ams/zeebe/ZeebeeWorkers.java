package org.mifos.connector.ams.zeebe;

import io.zeebe.client.ZeebeClient;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.support.DefaultExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.List;

import static org.mifos.connector.ams.camel.config.CamelProperties.TRANSACTION_ID;

@Component
public class ZeebeeWorkers {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public static List<String> DFSPIDS = Arrays.asList("DFSPID");    // TODO load from config

    @Autowired
    private ZeebeClient zeebeClient;

    @Autowired
    private ProducerTemplate producerTemplate;

    @Autowired
    private CamelContext camelContext;

    @Value("${ams.local.quote-enabled:false}")
    private boolean isLocalQuoteEnabled;

    @PostConstruct
    public void setupWorkers() {
        zeebeClient.newWorker()
                .jobType("local-quote")
                .handler((client, job) -> {
                    logger.info("Job '{}' started from process '{}' with key {}", job.getType(), job.getBpmnProcessId(), job.getKey());
                    if (isLocalQuoteEnabled) {
                        Exchange ex = new DefaultExchange(camelContext);
                        ex.setProperty(TRANSACTION_ID, job.getVariablesAsMap().get(TRANSACTION_ID));
                        producerTemplate.send("direct:send-local-quote", ex);
                    }
                    client.newCompleteCommand(job.getKey()).send();
                })
                .maxJobsActive(100_000)
                .open();

        zeebeClient.newWorker()
                .jobType("block-funds")
                .handler((client, job) -> {
                    logger.info("block funds task done");
                    client.newCompleteCommand(job.getKey()).send();
                }).open();

        zeebeClient.newWorker()
                .jobType("release-block")
                .handler((client, job) -> {
                    logger.info("release block task done");
                    client.newCompleteCommand(job.getKey()).send();
                }).open();

        zeebeClient.newWorker()
                .jobType("book-funds")
                .handler((client, job) -> {
                    logger.info("book funds task done");
                    client.newCompleteCommand(job.getKey()).send();
                }).open();

        zeebeClient.newWorker()
                .jobType("send-error-to-channel")
                .handler((client, job) -> {
                    logger.info("send-error-to-channel task done");
                    client.newCompleteCommand(job.getKey()).send();
                }).open();

        zeebeClient.newWorker()
                .jobType("send-success-to-channel")
                .handler((client, job) -> {
                    logger.info("send-success-to-channel task done");
                    client.newCompleteCommand(job.getKey()).send();
                }).open();

        zeebeClient.newWorker()
                .jobType("send-unknown-to-channel")
                .handler((client, job) -> {
                    logger.info("send-unknown-to-channel task done");
                    client.newCompleteCommand(job.getKey()).send();
                }).open();

        zeebeClient.newWorker()
                .jobType("send-to-operator")
                .handler((client, job) -> {
                    logger.info("send-to-operator task done");
                    client.newCompleteCommand(job.getKey()).send();
                }).open();
    }
}