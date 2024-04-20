package com.gp.aws.sts.athena.config;

import com.gp.aws.sts.athena.model.AthenaQueryDef;
import com.gp.aws.sts.athena.auth.CredentialProviderFactory;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;

import java.io.*;

@Data
@ConfigurationProperties("athena-source")
@Configuration
@Slf4j
public class AthenaSourceConfig {

    private AthenaQueryDef athenaTargetDef;
    private String region = Region.US_EAST_1.id();
    private String account;
    private String env="non-local";

    @Bean(name = "demo-athena-client")
    public AthenaClient amazonAthenaClient() throws IOException {
        var def = athenaTargetDef;
        var athenaClient = getAmazonAthenaClient(def);
        if(null == athenaClient){
             log.error("demo-athena-client is null ,def {} ",def);
        }
        return athenaClient;
    }

    public AthenaClient getAmazonAthenaClient(AthenaQueryDef def) throws IOException {
        AwsCredentialsProvider awsCredentialsProvider = CredentialProviderFactory.getProvider(def,env);
        if(null != awsCredentialsProvider){
            AthenaClient athenaClient = AthenaClient
                    .builder()
                    .region(Region.of(region))
                    .credentialsProvider(awsCredentialsProvider)
                    .build();
            log.info("Created athenaClient {} ",athenaClient);
            return athenaClient;
        }

        log.error("Can not create AthenaClient using the assumeRoleARN {} ",def.getRoleArn());
        return null;
    }


}
