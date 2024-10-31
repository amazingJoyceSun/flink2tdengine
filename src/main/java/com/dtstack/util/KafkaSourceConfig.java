package com.dtstack.util;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.io.Serializable;

//依赖此类将properties中的配置读取到程序中
public class KafkaSourceConfig implements Serializable {
    /*
    joyce,
    2024/10/31
    */
    private static final long serialVersionUID = -6016931785029927048L;
    private ParameterTool parameterTool;

    private String bootstrap = "";
    private String groupId = "";

    private String autoCommit = "true";
    private String commitInterval = "10000";
    private String clientidPrefix = "";
    private String consumeTopic = "";
    public KafkaSourceConfig(ParameterTool parameterTool) throws IOException {
        bootstrap = parameterTool.get("kafka.bootstrap.servers");
        groupId = parameterTool.get("kafka.group.id", "test-group");
        autoCommit = parameterTool.get("kafka.enable.auto.commit", "true");
        commitInterval = parameterTool.get("kafka.auto.commit.interval.ms", "10000");
        consumeTopic = parameterTool.get("kafka.consume.topic", "test-topic");
        clientidPrefix = parameterTool.get("kafka.client.prefix", "testxxx");

    }

    public ParameterTool getParameterTool() {
        return parameterTool;
    }

    public String getBootstrap() {
        return bootstrap;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getAutoCommit() {
        return autoCommit;
    }

    public String getCommitInterval() {
        return commitInterval;
    }

    public String getClientidPrefix() {
        return clientidPrefix;
    }

    public String getConsumeTopic() {
        return consumeTopic;
    }
}