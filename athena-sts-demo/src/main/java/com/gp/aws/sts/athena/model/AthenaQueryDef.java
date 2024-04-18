package com.gp.aws.sts.athena.model;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class AthenaQueryDef {
    private String region;
    private String profile;
    private String account;
    private String roleName;
    private String roleSessionName;
    private String roleArn;
    private String outputS3;
    private String workgroup;
    private String db;
    private String query;
    private long delay;
    private int repeatCount;
    private int sessionDurationSeconds;
    private String athenaClientId;
}
