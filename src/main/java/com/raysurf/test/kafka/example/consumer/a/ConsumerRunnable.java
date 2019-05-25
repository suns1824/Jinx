package com.raysurf.test.kafka.example.consumer.a;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class ConsumerRunnable implements Runnable {

    private KafkaConsumer<Integer, String> consumer;

    public ConsumerRunnable(String brokerList, String groupId, String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.70.133:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer2");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        this.consumer = new KafkaConsumer<Integer, String>(props);
        this.consumer.subscribe(Collections.singletonList(topic));
    }
    @Override
    public void run() {
        while(true) {
            ConsumerRecords<Integer, String> records = consumer.poll(200);
            for(ConsumerRecord<Integer, String> record: records) {
                System.out.println(Thread.currentThread().getName() + "consumed" + record.partition() + "the message with offset: " + record.offset());
            }
        }
    }
}
