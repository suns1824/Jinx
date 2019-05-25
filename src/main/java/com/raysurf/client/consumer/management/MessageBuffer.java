package com.raysurf.client.consumer.management;

import com.raysurf.client.consumer.management.events.MessageEvent;
import com.raysurf.client.util.CircularFifoQueue;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

public class MessageBuffer  implements Iterable<MessageEvent>, Serializable {

    private static final long serialVersionUID = -467696564722560524L;
    private final int maxLen;
    private final CircularFifoQueue<MessageEvent> queue;

    public MessageBuffer(int maxLen) {
        this.maxLen = maxLen;
        this.queue = new CircularFifoQueue(maxLen);
    }

    public void add(MessageEvent e) {
        this.queue.add(e);
    }

    public int size() {
        return this.queue.size();
    }

    public int getMaxLen() {
        return this.maxLen;
    }

    public Iterator<MessageEvent> iterator() {
        return this.queue.iterator();
    }

    public String dump() {
        StringBuilder sb = new StringBuilder();
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
        Iterator it = this.queue.iterator();

        while(it.hasNext()) {
            MessageEvent e = (MessageEvent)it.next();
            sb.append(fmt.format(new Date(e.ackTime))).append(" ");
            sb.append("offset=").append(e.offset).append(" ");
            sb.append(e.digest).append("\r\n");
        }

        return sb.toString();
    }
}
