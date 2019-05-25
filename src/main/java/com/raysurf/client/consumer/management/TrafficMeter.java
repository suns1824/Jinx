package com.raysurf.client.consumer.management;

import java.io.Serializable;

public class TrafficMeter implements Cloneable, Serializable {
    private static final long serialVersionUID = 6017403890707806273L;
    public volatile long inbound;
    public volatile long lastResetTime;


    public TrafficMeter() {
    }

    public void reset() {
        this.inbound = 0L;
        this.lastResetTime = System.currentTimeMillis();
    }

    public Object clone() {
        try {
            return (TrafficMeter)super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
    public String toString() {
        return "TrafficMeter [inbound=" + this.inbound + ", lastResetTime=" + this.lastResetTime + "]";
    }
}
