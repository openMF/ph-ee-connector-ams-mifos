package org.mifos.connector.ams.fineract;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.sts.StsClient;

@Configuration
public class PaymentTypeConfiguration {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${TENANT_CONFIGS}")
    private String tenantConfigsJson;

    @Value("${ams.tenants-config.s3-bucket}")
    private String s3Bucket;

    @Value("${ams.tenants-config.s3-key}")
    private String s3Key;


    @Autowired
    private ObjectMapper objectMapper;


    @Bean
    public TenantConfigs tenantConfigs() throws JsonProcessingException {
        System.setProperty("software.amazon.awssdk.http.service.impl", "software.amazon.awssdk.http.apache.ApacheSdkHttpService");

        logger.debug("loading tenant configs from S3 bucket {} and key {}", s3Bucket, s3Key);
        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        logger.debug("## assumed role: {}", StsClient.builder().credentialsProvider(credentialsProvider).build().getCallerIdentity());

        try (S3Client s3Client = S3Client.builder().credentialsProvider(credentialsProvider).build()) {
            String content = s3Client.getObjectAsBytes(GetObjectRequest.builder()
                    .bucket(s3Bucket)
                    .key(s3Key)
                    .build()
            ).asUtf8String();

            logger.debug("loaded tenant config from S3: {}", content);
            tenantConfigsJson = content;
        }

        TenantConfigs tenantConfigs = objectMapper.readValue(tenantConfigsJson, TenantConfigs.class);
        logger.info("Using tenant configs from S3: {}", tenantConfigs);
        return tenantConfigs;
    }
}
