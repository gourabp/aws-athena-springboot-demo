package com.gp.aws.sts.athena;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.services.athena.model.*;
import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class ResultProcessor {

    public final void processResult(GetQueryResultsResponse response){
        List<String> resultsInString = new ArrayList<>();
        var rs = response.resultSet();
        if(rs.hasRows()){
            var rsmd = rs.resultSetMetadata();
            if(null != rsmd){
                log.info("Columns: {}",rsmd.columnInfo());
            }
            var rowItr = rs.rows().iterator();
            int count = 0;
            while(rowItr.hasNext()){
                var row = rowItr.next();
                String lineItem;

                if(count == 0){ //header record
                    lineItem = getLineItemAsCsvString(row);
                    log.debug("#######Header record skipping Decimal parsing {} count {} ",lineItem,count);
                }else {
                    lineItem = getFormattedLineItem(row);
                    log.info("-------->>>>"+lineItem);
                }
                if(StringUtils.hasLength(lineItem)){
                    resultsInString.add(lineItem);
                }
                count++;
            }
        }
    }

    private String getLineItemAsCsvString(Row row){
        String line = row.data()
                .stream()
                .map(Datum::varCharValue)
                .collect(Collectors.joining(","));
        line = line+System.lineSeparator();
        return line;
    }

    //Unpack the result from Athena and format
    private String getFormattedLineItem(Row row){
        StringBuilder line = new StringBuilder();
        var data = row.data();
        for(int i=0; i< data.size();i++){
            var val = data.get(i).varCharValue();
            if(null == val || "null".equals(val)){
                val = ""; //as per on-prem report
            }
            line.append(val).append(",");
        }
        return line.deleteCharAt(line.lastIndexOf(",")).append(System.lineSeparator()).toString();
    }

    //================
    private void processResultApproach2(GetQueryResultsIterable queryResultsResultItr) {
        queryResultsResultItr.forEach(result -> {
            List<ColumnInfo> columnInfoList = result.resultSet().resultSetMetadata().columnInfo();
            int resultSize = result.resultSet().rows().size();
            log.info("Result size: {} ",resultSize);
            List<Row> results = result.resultSet().rows();
            processRow(results, columnInfoList);
        });
    }

    private static void processRow(List<Row> rowList, List<ColumnInfo> columnInfoList) {
        List<String> columns = new ArrayList<>();
        for (ColumnInfo columnInfo : columnInfoList) {
            columns.add(columnInfo.name());
        }
        for (Row row: rowList) {
            int index = 0;
            for (Datum datum : row.data()) {
                log.info(columns.get(index) + ": " + datum.varCharValue());
            }
            index++;
        }
        log.info("===================================");
    }

}
