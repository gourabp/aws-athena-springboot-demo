package com.gp.aws.sts.athena;

import com.gp.aws.sts.athena.config.AthenaSourceConfig;
import com.gp.aws.sts.athena.model.AthenaQueryDef;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;

@SpringBootApplication
@Slf4j
public class Application implements CommandLineRunner {

    @Autowired
    AthenaSourceConfig athenaSourceConfig;
    @Autowired
    private AthenaClient athenaClient;
    @Autowired
    private ResultProcessor resultProcessor;

    @Override
    public void run(String... args) throws Exception {
        var queryDef = athenaSourceConfig.getCamelAthenaTargetDef();
        var queryExecId = submitAthenaQuery(queryDef);
        log.info("Query submitted: " + System.currentTimeMillis());
        waitForQueryToComplete(queryExecId,queryDef);
        log.info("Query finished: " + System.currentTimeMillis());
        var getQueryResultsRequest = GetQueryResultsRequest.builder()
                .queryExecutionId(queryExecId).build();
        var response = athenaClient.getQueryResults(getQueryResultsRequest);

        var queryResultsResultItr = athenaClient.getQueryResultsPaginator(getQueryResultsRequest);
        resultProcessor.processResult(response);
        log.info("Query finished: " + System.currentTimeMillis());
    }

    private String submitAthenaQuery(AthenaQueryDef queryDef) {
        var queryExecContext = QueryExecutionContext.builder()
                .database(queryDef.getDb()).build();

        var resultConfig = ResultConfiguration.builder()
                .outputLocation(queryDef.getOutputS3()).build();

        var startQueryExecReq = StartQueryExecutionRequest.builder()
                .queryString(queryDef.getQuery())
                .queryExecutionContext(queryExecContext)
                .resultConfiguration(resultConfig).build();

        var startQueryExecutionResponse = athenaClient.startQueryExecution(startQueryExecReq);

        return startQueryExecutionResponse.queryExecutionId();
    }

    private void waitForQueryToComplete(String queryExecId,AthenaQueryDef queryDef) throws InterruptedException {

        var queryExecRequest = GetQueryExecutionRequest.builder().queryExecutionId(queryExecId).build();
        boolean shouldTry = true;
        int retryCounter = 1;
        do {
            var queryResponse = athenaClient.getQueryExecution(queryExecRequest);
            var queryStatus = queryResponse.queryExecution().status();

            switch (queryStatus.state()) {
                case FAILED:
                    log.error("Query Failed to run with reason : {} ",queryStatus.stateChangeReason());
                case CANCELLED:
                    log.error("Query was cancelled.");
                case SUCCEEDED:
                    shouldTry = false;
                    break;
                default:
                   if(retryCounter <= queryDef.getRepeatCount()){
                       retryCounter++;
                     Thread.sleep(queryDef.getDelay());
                   }else{
                      shouldTry = false;
                   }
                   break;
            }

            log.info("Current Status is: {} " , queryStatus.state());

        } while (shouldTry);
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}