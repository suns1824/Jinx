package com.raysurf.client.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class MyThreadFactory implements ThreadFactory {
    private final ThreadGroup group;
    private final AtomicInteger threadNum = new AtomicInteger(1);
    private final String prefix;

    public MyThreadFactory(String prefix) {
        this.prefix = prefix;
        SecurityManager sm = System.getSecurityManager();
        this.group = sm != null ? sm.getThreadGroup() : Thread.currentThread().getThreadGroup();
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(this.group, r, this.prefix + "-thread-" + this.threadNum.getAndIncrement(), 0L);
        if (t.getPriority() != 5) {
            t.setPriority(5);
        }

        return t;
    }
}
