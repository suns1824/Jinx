package com.raysurf.client.consumer.management;


import com.raysurf.client.consumer.management.events.CommandEvent;
import com.raysurf.client.consumer.management.events.MessageEvent;
import com.raysurf.client.consumer.management.events.TrafficEvent;
import com.raysurf.client.util.MyThreadFactory;
import com.raysurf.client.util.TableBuilder;

import javax.management.InstanceAlreadyExistsException;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class StatisticsService {
    private static final String DEFAULT_DATE_FROMAT = "yyyy-MM-dd HH:mm:ss";
    private static final StatisticsService instance = new StatisticsService();
    private ExecutorService statExecutor = Executors.newSingleThreadExecutor(new MyThreadFactory("kafka-statistics-submitter"));
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, new MyThreadFactory("kafka-statistics-handler"));
    private ConcurrentHashMap<String, StatisticsService.TopicStatRecord> statMap = new ConcurrentHashMap();
    private ConcurrentHashMap<String, MessageBuffer> msgMap = new ConcurrentHashMap();
    private volatile boolean started;
    private final Thread shutdownHook = new Thread() {
        public void run() {
            StatisticsService.this.stop();
        }
    };
    private static final long jvmStartTime = ManagementFactory.getRuntimeMXBean().getStartTime();

    private StatisticsService() {
    }

    public static StatisticsService getInstance() {
        return instance;
    }

    public synchronized void startScheduler() {
        if (!this.started) {
            Runnable task = new Runnable() {
                public void run() {
                    Iterator itr = StatisticsService.this.statMap.entrySet().iterator();
                    while (itr.hasNext()) {
                        Map.Entry<String, TopicStatRecord> e = (Map.Entry)itr.next();
                        StatisticsService.TopicStatRecord record = (StatisticsService.TopicStatRecord)e.getValue();
                        record.traffic.shift();
                    }
                }
            };
            this.scheduler.scheduleAtFixedRate(task, 0L, 1L, TimeUnit.SECONDS);
            this.started = true;
            this.registerShutdownHook();
            this.registerMXBean();
        }
    }

    private ObjectName registerMXBean() {
        String name = "com.raysurf:00=kafka-http-consumers,name=version";
        ConsumerInfo mbean = new ConsumerInfo();
        try {
            ObjectName objectName = new ObjectName(name);
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            try {
                mBeanServer.registerMBean(mbean, objectName);
            } catch (InstanceAlreadyExistsException e0) {
                mBeanServer.unregisterMBean(objectName);
            }
            return objectName;
        } catch (JMException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public synchronized void stop() {
        if (this.started) {
            if (this.scheduler != null) {
                this.scheduler.shutdownNow();
            }
            if (this.statExecutor != null) {
                this.statExecutor.shutdownNow();
            }

            this.started = false;
            this.unreigsterShutdownHook();
        }
    }

    private void registerShutdownHook() {
        try {
            Runtime.getRuntime().addShutdownHook(this.shutdownHook);
        } catch (IllegalArgumentException e) {
            ;
        }

    }

    private void unreigsterShutdownHook() {
        Runtime.getRuntime().removeShutdownHook(this.shutdownHook);
    }

    public void submitCommandEvent(final CommandEvent e) {
        if (this.started && this.statExecutor != null) {
            try {
                this.statExecutor.submit(new Runnable() {
                    public void run() {
                        if (StatisticsService.this.started) {
                            String topicAndPartition = e.topic + ":" + e.partition;
                            StatisticsService.TopicStatRecord r = (StatisticsService.TopicStatRecord)StatisticsService.this.statMap.get(topicAndPartition);
                            if (r == null) {
                                r = StatisticsService.this.new TopicStatRecord();
                                StatisticsService.this.statMap.put(topicAndPartition, r);
                            }
                            r.addElapsedTimeItem(e.command, e.elapsed, e.isFail);
                        }
                    }
                });
            } catch (RejectedExecutionException e1) {
                ;
            }

        }
    }

    public void submitMessageEvent(final MessageEvent e) {
        if (this.started && this.statExecutor != null) {
            try {
                this.statExecutor.submit(new Runnable() {
                    public void run() {
                        if (StatisticsService.this.started) {
                            String topicAndPartition = e.topic + ":" + e.partition;
                            MessageBuffer buf = (MessageBuffer)StatisticsService.this.msgMap.get(topicAndPartition);
                            if (buf == null) {
                                buf = new MessageBuffer(10);
                                StatisticsService.this.msgMap.put(topicAndPartition, buf);
                            }

                            buf.add(e);
                        }
                    }
                });
            } catch (RejectedExecutionException e1) {
                ;
            }

        }
    }

    public void submitTrafficEvent(final TrafficEvent e) {
        if (this.started && this.statExecutor != null && this.scheduler != null) {
            try {
                this.statExecutor.submit(new Runnable() {
                    public void run() {
                        if (StatisticsService.this.started) {
                            String topicAndPartition = e.topic + ":" + e.partition;
                            StatisticsService.TopicStatRecord r = (StatisticsService.TopicStatRecord)StatisticsService.this.statMap.get(topicAndPartition);
                            if (r == null) {
                                r = StatisticsService.this.new TopicStatRecord();
                                StatisticsService.this.statMap.put(topicAndPartition, r);
                            }

                            r.addCurrentInbound(e.size);
                        }
                    }
                });
            } catch (RejectedExecutionException e1) {
                ;
            }

        }
    }

    String getDiagnoseInfo() {
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        StringBuilder sb = new StringBuilder();
        sb.append("===========================");
        sb.append("kafka consumer statistics @" + fmt.format(new Date()));
        sb.append("===========================\n");
        String currentTime = fmt.format(new Date());
        Iterator itr = this.statMap.entrySet().iterator();

        while(itr.hasNext()) {
            Map.Entry<String, TopicStatRecord> e = (Map.Entry)itr.next();
            String topicAndPartition = (String)e.getKey();
            StatisticsService.TopicStatRecord record = (StatisticsService.TopicStatRecord)e.getValue();
            sb.append("Kafka topic: ").append(topicAndPartition).append("\n");
            String accTime = fmt.format(new Date(record.start));
            sb.append("Consumer start time: ").append(accTime).append(", current: ").append(currentTime).append("\n");
            sb.append("Command elapsed and network latency(ms): ").append("\n");
            sb.append(record.formatElapsedItemTable()).append("\n\n");
            sb.append("Traffic: ").append("\n");
            sb.append(record.formatTrafficItemTable()).append("\n");
        }

        return sb.toString();
    }

    String getHistory() {
        StringBuilder sb = new StringBuilder();
        Iterator itr = this.msgMap.entrySet().iterator();

        while(itr.hasNext()) {
            Map.Entry<String, MessageBuffer> e = (Map.Entry)itr.next();
            String topicAndPartition = (String)e.getKey();
            sb.append("===========================");
            sb.append("topic: ").append(topicAndPartition);
            sb.append("===========================\n");
            MessageBuffer buffer = (MessageBuffer)e.getValue();
            sb.append(buffer.dump());
        }

        return sb.toString();
    }

    private class TrafficItem {
        private TrafficMeter last1s;
        private TrafficBuffer last59s;
        private TrafficMeter total;

        private TrafficItem() {
            this.last1s = new TrafficMeter();
            this.last59s = new TrafficBuffer();
            this.total = new TrafficMeter();
        }

        public String getCurrentInbound() {
            if (this.last1s.inbound > 0L) {
                return this.last1s.inbound + " B/s";
            } else {
                return System.currentTimeMillis() - this.last1s.lastResetTime < 100L ? this.last59s.getLatest().inbound + " B/s" : this.last1s.inbound + " B/s";
            }
        }

        public String getTotalInbound() {
            long in = this.total.inbound + this.last59s.getTotalInbound() + this.last1s.inbound;
            long seconds = (System.currentTimeMillis() - StatisticsService.jvmStartTime) / 1000L;
            DecimalFormat df = new DecimalFormat();
            df.setMaximumFractionDigits(2);
            String speed = df.format((double)in / (double)seconds) + " B/s";
            String size = df.format((double)((float)in / 1000.0F)) + "KB";
            return speed + " (" + size + ")";
        }

        public synchronized void addCurrentInbound(long in) {
            this.last1s.inbound += in;
        }

        public String getLast1mInbound() {
            long in = this.last59s.getTotalInbound() + this.last1s.inbound;
            DecimalFormat df = new DecimalFormat();
            df.setMaximumFractionDigits(2);
            String speed = df.format((double)in / (double)(this.last59s.size() + 1)) + " B/s";
            String size = df.format((double)((float)in / 1000.0F)) + "KB";
            return speed + " (" + size + ")";
        }

        public synchronized void shift() {
            TrafficMeter latest = (TrafficMeter)this.last1s.clone();
            TrafficMeter earliest = this.last59s.add(latest);
            if (earliest != null) {
                this.total.inbound += earliest.inbound;
            }

            this.last1s.reset();
        }
    }
    private class ElapsedTimeItem {
        public final AtomicInteger successCount = new AtomicInteger(0);
        public final AtomicInteger failCount = new AtomicInteger(0);
        public volatile long maxElapsed;
        public volatile long minElapsed;
        public volatile long totalElapsed;

        public ElapsedTimeItem(long elapsed, boolean isFail) {
            this.maxElapsed = elapsed;
            this.minElapsed = elapsed;
            this.totalElapsed = elapsed;
            if (isFail) {
                this.failCount.incrementAndGet();
            } else {
                this.successCount.incrementAndGet();
            }

        }

        public void update(long elapsed, boolean isFail) {
            this.totalElapsed += elapsed;
            if (elapsed > this.maxElapsed) {
                this.maxElapsed = elapsed;
            } else if (elapsed < this.minElapsed) {
                this.minElapsed = elapsed;
            }

            if (isFail) {
                this.failCount.incrementAndGet();
            } else {
                this.successCount.incrementAndGet();
            }

        }
    }
    private class TopicStatRecord {
        private final long start = System.currentTimeMillis();
        private final ConcurrentHashMap<String, StatisticsService.ElapsedTimeItem> elapsedMap = new ConcurrentHashMap();
        private final StatisticsService.TrafficItem traffic = StatisticsService.this.new TrafficItem();

        public TopicStatRecord() {
        }

        public void addElapsedTimeItem(String command, long elapsed, boolean isFail) {
            StatisticsService.ElapsedTimeItem item = (StatisticsService.ElapsedTimeItem)this.elapsedMap.get(command);
            if (item == null) {
                this.elapsedMap.put(command, StatisticsService.this.new ElapsedTimeItem(elapsed, isFail));
            } else {
                item.update(elapsed, isFail);
            }

        }

        public void addCurrentInbound(long in) {
            this.traffic.addCurrentInbound(in);
        }

        public String formatElapsedItemTable() {
            DecimalFormat df = new DecimalFormat();
            df.setMaximumFractionDigits(2);
            TableBuilder tb = new TableBuilder(6);
            String[] header = new String[]{"command", "times", "failed", "maxElapsed", "minElapsed", "meanElapsed"};
            tb.setHeader(header);
            Iterator itr = this.elapsedMap.entrySet().iterator();

            while(itr.hasNext()) {
                Map.Entry<String, ElapsedTimeItem> e = (Map.Entry)itr.next();
                String command = e.getKey();
                StatisticsService.ElapsedTimeItem it = e.getValue();
                int fail = it.failCount.get();
                int total = it.successCount.get() + fail;
                tb.addRow(new String[]{command, String.valueOf(total), String.valueOf(fail), df.format(it.maxElapsed / 1000L), df.format(it.minElapsed / 1000L), df.format(it.totalElapsed / (long)total / 1000L)});
            }

            return tb.toString();
        }

        public String formatTrafficItemTable() {
            TableBuilder tb = new TableBuilder(2);
            String[] header = new String[]{"interval", "inbound"};
            tb.setHeader(header);
            synchronized(this.traffic) {
                tb.addRow(new String[]{"last second", this.traffic.getCurrentInbound()});
                tb.addRow(new String[]{"last minute", this.traffic.getLast1mInbound()});
                tb.addRow(new String[]{"total", this.traffic.getTotalInbound()});
            }
            return tb.toString();
        }
    }
}
