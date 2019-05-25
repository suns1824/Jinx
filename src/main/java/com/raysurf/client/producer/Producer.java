package com.raysurf.client.producer;

public interface Producer {
    void produce(String topicName, Object msg) throws Exception;
}
