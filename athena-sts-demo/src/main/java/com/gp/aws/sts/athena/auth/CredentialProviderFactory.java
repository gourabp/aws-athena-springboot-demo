package com.gp.aws.sts.athena.auth;

import com.gp.aws.sts.athena.model.AthenaQueryDef;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsAsyncClient;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class CredentialProviderFactory {

    private static final String ENV_LOCAL = "local";

    public static final AwsCredentialsProvider getProvider(AthenaQueryDef cfg, String env){
        if(ENV_LOCAL.equalsIgnoreCase(env)){
            return getCredProviderByProfile(cfg);
        }
        return getCredProvider(cfg);
    }

    private static AwsCredentialsProvider getCredProvider(AthenaQueryDef cfg) {
        //log.info("@@@@@@@@@@@@@@@@@@@@@@assumeRole -1");
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
                // log.info("I don't want to see this message on my log, Not-able to assumeRole :((((");
            }
        }catch(Exception ex){
            // log.error("Not-able to assumeRole message {} cause {}",ex.getMessage(),ex.getCause());
            //log.error("stacktrace :{}", Arrays.toString(ex.getStackTrace()));
        }
        return null;
    }


    private static AwsCredentialsProvider getCredProviderByProfile(AthenaQueryDef cfg) {
        //log.info("@@@@@@@@@@@@@@@@@@@@@@assumeRole -2");
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
            //log.error("InterruptedException request to assume role failed {} error message {} ",assumeRoleRequest,e.getMessage());
        } catch (ExecutionException e) {
            e.printStackTrace();
            // log.error("ExecutionException request to assume role failed {} error message {} ",assumeRoleRequest,e.getMessage());
        }

        if(null != response && response.assumedRoleUser().arn().contains(cfg.getRoleName())){
            Credentials creds = response.credentials();
            //log.info("Credentials are {} ",creds);
            AwsSessionCredentials sessionCredentials = AwsSessionCredentials.create(creds.accessKeyId(), creds.secretAccessKey(), creds.sessionToken());
            //log.info("@@@@@@@@@@@@@@@@@@@@@@assumeRole -2.1");
            return AwsCredentialsProviderChain.builder()
                    .credentialsProviders(StaticCredentialsProvider.create(sessionCredentials))
                    .build();
        }

        return null;
    }
}