package com.raysurf.client.consumer;

import com.raysurf.client.util.AppUtils;

import java.io.Serializable;

public class ConsumerConfig implements Serializable, Cloneable {
    private static final long serialVersionUID = -9074677266297035128L;
    private int kafkaPartition = 0;
    private String kafkaTopic;
    private String consumerGroup = "default";
    private boolean isDefaultGroup = true;
    private int connTimeout = 2000;
    private int readTimeout = 5000;
    private String bridgeServer;
    private int maxProcessRetries = 10;
    private boolean startFromLatest = false;
    private boolean upgradeMode = false;
    private String redisServers;
    private String redisSchema = "";

    public ConsumerConfig() {
    }

    public boolean isUpgradeMode() {
        return this.upgradeMode;
    }

    public String getRedisServers() {
        return this.redisServers;
    }

    public String getRedisSchema() {
        return this.redisSchema;
    }

    public String getBridgeServer() {
        return this.bridgeServer;
    }

    public void setRedisServers(String redisServers) {
        this.redisServers = redisServers;
    }

    public void setRedisSchema(String redisSchema) {
        this.redisSchema = redisSchema;
    }

    public String getKafkaTopic() {
        return this.kafkaTopic;
    }

    public void setKafkaTopic(String kafkaTopic) {
        if (kafkaTopic != null && !kafkaTopic.isEmpty()) {
            if (kafkaTopic.matches("^(\\w|[_])(\\w|[._-])+\\w$")) {
                this.kafkaTopic = kafkaTopic;
            } else {
                throw new IllegalArgumentException("topic name: \"" + kafkaTopic + "\" illegal!");
            }
        } else {
            throw new IllegalArgumentException("kafka topic cannot be empty.");
        }
    }

    public int getKafkaPartition() {
        return this.kafkaPartition;
    }

    public void setKafkaPartition(int kafkaPartition) {
        this.kafkaPartition = kafkaPartition;
    }

    public int getMaxProcessRetries() {
        return this.maxProcessRetries;
    }

    public void setMaxProcessRetries(int maxProcessRetries) {
        this.maxProcessRetries = maxProcessRetries;
    }

    public int getConnTimeout() {
        return this.connTimeout;
    }

    public void setConnTimeout(int connTimeout) {
        this.connTimeout = connTimeout;
    }

    public int getReadTimeout() {
        return this.readTimeout;
    }

    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    public String getConsumerGroup() {
        return this.consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        if (consumerGroup != null && !consumerGroup.isEmpty()) {
            if (!consumerGroup.matches("^\\w(\\w|[._-])+\\w$")) {
                throw new IllegalArgumentException("group name: \"" + consumerGroup + "\" illegal!");
            }

            this.consumerGroup = consumerGroup;
            this.isDefaultGroup = false;
        } else {
            this.consumerGroup = "default";
            this.isDefaultGroup = true;
        }

    }

    public boolean isDefaultGroup() {
        return this.isDefaultGroup;
    }

    public void setBridgeServer(String bridgeServer) {
        this.bridgeServer = bridgeServer;
    }

    public void setUpgradeMode(boolean upgradeMode) {
        this.upgradeMode = upgradeMode;
    }

    public boolean isStartFromLatest() {
        return this.startFromLatest;
    }

    public void setStartFromLatest(boolean startFromLatest) {
        this.startFromLatest = startFromLatest;
    }

    public ConsumerConfig copy() {
        try {
            return (ConsumerConfig)this.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    static {
        String ver = System.getProperty("kafka-http-client-version");
        if (ver == null || ver.length() == 0) {
            ver = AppUtils.parseJarVersion(HttpConsumer.class, "kafka-http-client-");
            if (ver != null && ver.length() != 0) {
                System.setProperty("kafka-http-client-version", ver);
            }
        }

    }
}

