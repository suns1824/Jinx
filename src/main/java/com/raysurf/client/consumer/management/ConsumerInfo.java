package com.raysurf.client.consumer.management;

public class ConsumerInfo implements ConsumerInfoMBean {
    public ConsumerInfo() {
    }

    public String getVersion() {
        String ver = System.getProperty("kafka-http-client-version");
        return ver == null ? "unknown" : ver;
    }
}