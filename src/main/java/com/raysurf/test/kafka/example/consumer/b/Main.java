package com.raysurf.test.kafka.example.consumer.b;

import com.raysurf.test.kafka.example.KafkaProperties;

public class Main {
    public static void main(String[] args) {
        final ConsumerThreadHandler<Integer, String> handler = new ConsumerThreadHandler<>(KafkaProperties.SERVER, "DemoConsumer3", KafkaProperties.TOPIC3);
        new Thread(new Runnable() {
            @Override
            public void run() {
                handler.consume(Runtime.getRuntime().availableProcessors());
            }
        }).start();
        try {
            Thread.sleep(20000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //20s后关闭
        handler.close();
    }
}
