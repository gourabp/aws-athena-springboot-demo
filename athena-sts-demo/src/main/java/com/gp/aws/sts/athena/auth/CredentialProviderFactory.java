package com.gp.aws.sts.athena.auth;

import com.gp.aws.sts.athena.model.AthenaQueryDef;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsAsyncClient;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;
import java.io.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Slf4j
public class CredentialProviderFactory {

    private static final String ENV_LOCAL = "local";

    public static final AwsCredentialsProvider getProvider(AthenaQueryDef cfg, String env) throws IOException {
        if(ENV_LOCAL.equalsIgnoreCase(env)){
            return getCredProviderByProfile(cfg);
        }
        return getCredProvider(cfg);
    }

    private static AwsCredentialsProvider getCredProvider(AthenaQueryDef cfg) {
        try{
            DefaultCredentialsProvider defaultCredentialsProvider = DefaultCredentialsProvider.create();

            StsClient stsClient = StsClient.builder()
                    .credentialsProvider(defaultCredentialsProvider)
                    .region(Region.of(cfg.getRegion())).build();

            var assumeRoleRequest = AssumeRoleRequest.builder()
                    .roleArn(cfg.getRoleArn())
                    .roleSessionName(cfg.getRoleSessionName())
                    .durationSeconds(cfg.getSessionDurationSeconds())
                    .build();

            var assumeRoleResponse = stsClient.assumeRole(assumeRoleRequest);

            if(null != assumeRoleResponse){
                Credentials credentials = assumeRoleResponse.credentials();
                AwsSessionCredentials sessionCredentials =
                        AwsSessionCredentials.create(credentials.accessKeyId(),
                                credentials.secretAccessKey(),
                                credentials.sessionToken());

                var credentialsProviderChain = AwsCredentialsProviderChain.builder()
                        .credentialsProviders(StaticCredentialsProvider.create(sessionCredentials))
                        .build();

                return credentialsProviderChain;
            }else{
                log.info("Issue with assuming role");
            }
        }catch(Exception ex){
             log.error("Not-able to assumeRole message {} cause {}",ex.getMessage(),ex.getCause());
        }
        return null;
    }

    private static AwsCredentialsProvider getCredProviderByProfile(AthenaQueryDef cfg) throws IOException {
        ProfileCredentialsProvider defaultProfileCredProvider = ProfileCredentialsProvider
                .builder()
                .profileName(cfg.getProfile())
                .build();

        StsAsyncClient stsAsyncClient = StsAsyncClient.builder()
                .credentialsProvider(defaultProfileCredProvider)
                .region(Region.of(cfg.getRegion()))
                .build();

        AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
                .durationSeconds(cfg.getSessionDurationSeconds())
                .roleArn(cfg.getRoleArn())
                .roleSessionName(cfg.getRoleSessionName())
                .build();

        // log.info("Send request to assume role {} ",assumeRoleRequest);
        Future<AssumeRoleResponse> responseFuture = stsAsyncClient.assumeRole(assumeRoleRequest);
        AssumeRoleResponse response = null;
        try {
            response = responseFuture.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        if(null != response && response.assumedRoleUser().arn().contains(cfg.getRoleName())){
            Credentials creds = response.credentials();
            //log.info("Credentials are {} ",creds);
            AwsSessionCredentials sessionCredentials = AwsSessionCredentials.create(creds.accessKeyId(), creds.secretAccessKey(), creds.sessionToken());
            return AwsCredentialsProviderChain.builder()
                    .credentialsProviders(StaticCredentialsProvider.create(sessionCredentials))
                    .build();
        }

        return null;
    }
}