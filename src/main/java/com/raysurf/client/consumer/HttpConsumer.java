package com.raysurf.client.consumer;

import com.raysurf.client.consumer.Exception.ProcessingException;
import com.raysurf.client.consumer.response.*;

public interface HttpConsumer {
    void onMessageReceived(long offset, String msgKey, byte[] payload);

    void onProcessingException(KafkaMessage msg, ProcessingException e);

    void onThreadException(Throwable cause);

    void process(KafkaMessage km) throws ProcessingException;

    String getKafkaTopic();

    int getKafkaPartition();

    String getConsumerId();

    String getConsumerGroup();

    String getCurrentThreadId();

    String getMode();

    long getSessionTimeout();

    RegisterResponse register();

    UnregisterResponse unregister();

    long findOldOffset();

    SyncResponse syncOffset(long offset);

    FetchResponse fetch();

    AckResponse ack(long offset);

    HeartbeatResponse sendHeartbeat(long offset);

    void startUpgrade();

    void start();

    void resume();

    void stop();

    ConsumerState getConsumerState();

    long incrCount();

    long getCount();

}
