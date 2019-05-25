package com.raysurf.client.consumer.management.events;

public class MessageEvent {
    public final String topic;
    public final int partition;
    public final long offset;
    public final String digest;
    public final long ackTime;

    public MessageEvent(String topic, int partition, long offset, String digest, long ackTime) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.digest = digest;
        this.ackTime = ackTime;
    }

    public String toString() {
        return "MessageEvent [topic=" + this.topic + ", partition=" + this.partition + ", offset=" + this.offset + ", digest=" + this.digest + ", ackTime=" + this.ackTime + "]";
    }
}
