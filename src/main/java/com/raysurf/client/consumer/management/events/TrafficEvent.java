package com.raysurf.client.consumer.management.events;

import java.io.Serializable;

public class TrafficEvent implements Serializable {
    private static final long serialVersionUID = -3415009239094652012L;
    public final String topic;
    public final int partition;
    public final long size;

    public TrafficEvent(String topic, int partition, long traffic) {
        this.topic = topic;
        this.partition = partition;
        this.size = traffic;
    }

    public String toString() {
        return "TrafficEvent [topic=" + this.topic + ", partition=" + this.partition + ", size=" + this.size + "]";
    }
}
