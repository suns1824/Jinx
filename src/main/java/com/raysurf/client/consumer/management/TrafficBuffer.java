package com.raysurf.client.consumer.management;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;

public class TrafficBuffer implements Iterable<TrafficMeter>, Serializable {
    private static final long serialVersionUID = -3115863063976237360L;
    private final LinkedList<TrafficMeter> queue = new LinkedList<TrafficMeter>();
    private final int maxLen = 59;

    public TrafficBuffer() {

    }

    public Iterator<TrafficMeter> iterator() {
        return this.queue.iterator();
    }

    public TrafficMeter add(TrafficMeter newEle) {
        if (newEle == null) {
            return null;
        } else {
            int len = this.queue.size();
            if (len < maxLen) {
                this.queue.add(newEle);
                return null;
            } else {
                TrafficMeter earliest = this.queue.removeFirst();
                this.queue.add(newEle);
                return earliest;
            }
        }
    }

    public long getTotalInbound() {
        long total = 0L;
        TrafficMeter t;
        for(Iterator itr = this.queue.iterator(); itr.hasNext(); total += t.inbound) {
            t = (TrafficMeter)itr.next();
        }
        return total;
    }

    public int size() {
        return this.queue.size();
    }

    public TrafficMeter getLatest() {
        return (TrafficMeter)this.queue.getLast();
    }
}
