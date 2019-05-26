package com.raysurf.test.kafka.example.consumer.b;

import org.apache.ibatis.ognl.CollectionElementsAccessor;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ConsumerThreadHandler<K, V> {
    private final KafkaConsumer<K, V> consumer;
    private ExecutorService excutors;
    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    public ConsumerThreadHandler(String brokerList, String groupId, String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.70.133:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer3");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<K, V>(props);
        consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                consumer.commitSync(offsets);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                offsets.clear();
            }
        });
    }

    public void consume(int threadNumber) {
        excutors = new ThreadPoolExecutor(threadNumber, threadNumber, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1000),
                new ThreadPoolExecutor.CallerRunsPolicy());
        try {
            ConsumerRecords<K, V> records = this.consumer.poll(1000L);
            if (!records.isEmpty()) {
                excutors.submit(new ConsumerWorker<>(records, offsets));
            }
            commitOffsets();
        } catch (WakeupException e) {

        } finally {
            commitOffsets();
            consumer.close();
        }

    }

    private void commitOffsets() {
        Map<TopicPartition, OffsetAndMetadata> unmodifiedMap;
        synchronized (offsets) {
            if (offsets.isEmpty()) {
                return;
            }
            //返回指定映射的不可修改视图
            unmodifiedMap = Collections.unmodifiableMap(new HashMap<>(offsets));
            offsets.clear();
        }
        consumer.commitSync(unmodifiedMap);
    }

    public void close() {
        consumer.close();
        excutors.shutdown();
    }
}
