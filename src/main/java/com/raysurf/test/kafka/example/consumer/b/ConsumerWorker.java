package com.raysurf.test.kafka.example.consumer.b;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

public class ConsumerWorker<K, V> implements Runnable {
    private final ConsumerRecords<K, V> records;
    private final Map<TopicPartition, OffsetAndMetadata> offsets;

    public ConsumerWorker(ConsumerRecords<K, V> records, Map<TopicPartition, OffsetAndMetadata> offsets) {
        this.records = records;
        this.offsets = offsets;
    }

    @Override
    public void run() {
        for (TopicPartition partition: records.partitions()) {
            List<ConsumerRecord<K, V>> partitionRecords = records.records(partition);
            for(ConsumerRecord<K, V> record: partitionRecords) {
                System.out.println(record.topic() + " " + record.partition() + " " + record.offset());
            }
            long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
            synchronized (offsets) {
                if (!offsets.containsKey(partition)) {
                    offsets.put(partition, new OffsetAndMetadata(lastOffset));
                } else {
                    long curr = offsets.get(partition).offset();
                    if (curr <= lastOffset + 1) {
                        offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
                    }
                }
            }
        }
    }
}
