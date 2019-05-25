package com.raysurf.test.kafka.example.consumer.a;

import java.util.ArrayList;
import java.util.List;

public class ConsumerGroup {
    private List<ConsumerRunnable> consumers;

    public ConsumerGroup(int consumerNum, String groupId, String topic, String brokerList) {
        consumers = new ArrayList<>(consumerNum);
        for(int i = 0; i < consumerNum; i++) {
            ConsumerRunnable costumeThread = new ConsumerRunnable(brokerList, groupId, topic);
            consumers.add(costumeThread);
        }
    }
    public void execute() {
        for(ConsumerRunnable runnable: consumers) {
            new Thread(runnable).start();
        }
    }
}
