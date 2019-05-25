package com.raysurf.test.kafka.example.consumer.a;

import com.raysurf.test.kafka.example.KafkaProperties;

public class ConsumerMain {
    public static void main(String[] args) {
        ConsumerGroup group = new ConsumerGroup(3, KafkaProperties.GROUP_ID, KafkaProperties.TOPIC2, KafkaProperties.SERVER);
        group.execute();
    }
}
