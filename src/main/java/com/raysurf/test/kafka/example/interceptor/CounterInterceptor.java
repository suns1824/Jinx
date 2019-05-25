package com.raysurf.test.kafka.example.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CounterInterceptor implements ProducerInterceptor<String, String>{

    private volatile static int successCount = 0;
    private volatile static int failCount = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            successCount ++;
        } else {
            failCount ++;
        }
    }

    @Override
    public void close() {
        System.out.println("success count: " + successCount);
        System.out.println("fail count: " + failCount);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
