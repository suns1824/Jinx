package com.raysurf.client.consumer;

import com.codahale.metrics.*;
import com.raysurf.client.consumer.Exception.ProcessingException;
import com.raysurf.client.consumer.command.*;
import com.raysurf.client.consumer.management.Controller;
import com.raysurf.client.consumer.management.StatisticsService;
import com.raysurf.client.consumer.management.events.CommandEvent;
import com.raysurf.client.consumer.management.events.MessageEvent;
import com.raysurf.client.consumer.management.events.TrafficEvent;
import com.raysurf.client.consumer.response.*;
import com.wacai.common.redis.RedisException;
import com.wacai.common.redis.provider.SimpleRedisCli;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.net.*;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractHttpConsumer implements HttpConsumer{
    protected static final Log log = LogFactory.getLog(HttpConsumer.class);
    private static final AtomicInteger serialNo = new AtomicInteger(43690);
    private static final MetricRegistry metricRegistry = new MetricRegistry();
    protected final StatisticsService statSvc = StatisticsService.getInstance();
    protected final String consumerId = this.generateId();
    protected final Meter fetchRequests;
    protected final Meter ackRequests;
    protected final Counter success;
    protected final Counter processFailed;
    protected final Histogram trafficHistogram;
    protected final Timer processTimer;
    protected volatile ConsumerState consumerState;
    protected long sessionTimeout;
    protected ConsumerConfig config;
    private final int maxRetries;

    public AbstractHttpConsumer(ConsumerConfig config) {
        this.fetchRequests = metricRegistry.meter(this.consumerId + "-fetchRequests");
        this.ackRequests = metricRegistry.meter(this.consumerId + "-ackRequests");
        this.success = metricRegistry.counter(MetricRegistry.name(this.consumerId + "-ackRequests", new String[]{"success"}));
        this.processFailed = metricRegistry.counter(this.consumerId + "-process-failed");
        this.trafficHistogram = metricRegistry.histogram(this.consumerId + "-traffic-incoming");
        this.processTimer = metricRegistry.timer(this.consumerId + "-processTimer");
        this.consumerState = ConsumerState.INITIALIZING;
        this.maxRetries = 5;
        String bridgeServer = config.getBridgeServer();
        if (bridgeServer != null && bridgeServer.length() != 0) {
            this.config = config;
            this.registerMetrics();
            Controller.getInstance().addConsumer(this);
        } else {
            throw new IllegalArgumentException("Error: kafka bridge server cannot be null or empty");
        }
    }

    private void registerMetrics() {
        String consumerName = "\"" + this.getConsumerName() + "\"";
        AbstractHttpConsumer.MyObjectNameFactory onFactory = new MyObjectNameFactory();
        String mbean = "com.raysurf:00=kafka-http-consumers,01=" + consumerName + ",name=ackRequests";
        JmxReporter.forRegistry(metricRegistry).createsObjectNamesWith(onFactory).inDomain(mbean).filter(new MetricFilter() {
            public boolean matches(String name, Metric metric) {
                return name.equals(AbstractHttpConsumer.this.consumerId + "-ackRequests");
            }
        }).build().start();
        mbean = "com.raysurf:00=kafka-http-consumers,01=" + consumerName + ",name=ackSuccess";
        JmxReporter.forRegistry(metricRegistry).createsObjectNamesWith(onFactory).inDomain(mbean).filter(new MetricFilter() {
            public boolean matches(String name, Metric metric) {
                return name.equals(AbstractHttpConsumer.this.consumerId + "-ackRequests.success");
            }
        }).build().start();
        mbean = "com.raysurf:00=kafka-http-consumers,01=" + consumerName + ",name=fetchRequests";
        JmxReporter.forRegistry(metricRegistry).createsObjectNamesWith(onFactory).inDomain(mbean).filter(new MetricFilter() {
            public boolean matches(String name, Metric metric) {
                return name.equals(AbstractHttpConsumer.this.consumerId + "-fetchRequests");
            }
        }).build().start();
    }

    public String getConsumerName() {
        return this.getKafkaTopic() + ":" + this.getKafkaPartition() + ":" + this.getConsumerGroup() + ":" + this.getMode() + ":" + this.getConsumerId();
    }

    public String getUserAgent() {
        StringBuilder sb = new StringBuilder("kafka-http-client");
        String version = System.getProperty("kafka-http-client-version");
        if (version != null && version.length() != 0) {
            sb.append("-").append(version);
        }

        sb.append("/java").append(System.getProperty("java.version"));
        return sb.toString();
    }

    public void submitCommandEvent(String command, long elapsed) {
        boolean isFailed = false;
        this.submitCommandEvent(command, elapsed, isFailed);
    }

    public void submitCommandEvent(String command, long elapsed, boolean isFailed) {
        CommandEvent e = new CommandEvent(this.getKafkaTopic(), this.getKafkaPartition(), command, elapsed, isFailed);
        this.statSvc.submitCommandEvent(e);
        if ("fetch".equals(command)) {
            this.fetchRequests.mark();
        } else if ("process".equals(command)) {
            this.processTimer.update(elapsed, TimeUnit.MILLISECONDS);
        } else if ("ack".equals(command)) {
            this.ackRequests.mark();
        }
    }

    public void submitMessageEvent(long offset, String digest, long ackTime) {
        MessageEvent e = new MessageEvent(this.getKafkaTopic(), this.getKafkaPartition(), offset, digest, ackTime);
        this.statSvc.submitMessageEvent(e);
    }

    private void submitTrafficEvent(long traffic) {
        TrafficEvent e = new TrafficEvent(this.getKafkaTopic(), this.getKafkaPartition(), traffic);
        this.statSvc.submitTrafficEvent(e);
        this.trafficHistogram.update(traffic);
    }

    static class MyObjectNameFactory implements ObjectNameFactory {
        MyObjectNameFactory() {
        }

        public ObjectName createName(String type, String domain, String name) {
            try {
                ObjectName objectName = new ObjectName(domain);
                return objectName;
            } catch (MalformedObjectNameException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private long findOldOffset(String host, int port) {
        SimpleRedisCli redisCli = new SimpleRedisCli(host, port);
        String grp = this.config.getConsumerGroup();
        if (this.config.isDefaultGroup()) {
            grp = "";
        }

        String topic = this.getKafkaTopic();
        String offsetKey = grp + "kafka-topic-offset:" + topic;

        try {
            String prefix = this.config.getRedisSchema();
            if (prefix != null) {
                offsetKey = prefix + offsetKey;
            }

            String s = redisCli.get(offsetKey);
            if (s != null && s.length() != 0) {
                long offset = Long.parseLong(s);
                log.info("find topic " + topic + " old consumer's offset=" + offset + " in redis " + host + ":" + port);
                return offset;
            }
        } catch (RedisException e) {
            log.error(e.getMessage(), e);
        } finally {
            redisCli.close();
        }

        return -1L;
    }

    public int getMaxRetries() {
        return 5;
    }

    public String getBridgeServer() {
        return this.config.getBridgeServer();
    }

    public int getConnTimeout() {
        return this.config.getConnTimeout();
    }

    public int getReadTimeout() {
        return this.config.getReadTimeout();
    }

    protected String generateId() {
        StringBuilder sb = new StringBuilder();

        try {
            Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
            Iterator itr = Collections.list(nets).iterator();

            label34:
            while(itr.hasNext()) {
                NetworkInterface netint = (NetworkInterface)itr.next();
                Iterator inetAddressIterator = Collections.list(netint.getInetAddresses()).iterator();

                while(inetAddressIterator.hasNext()) {
                    InetAddress addr = (InetAddress)inetAddressIterator.next();
                    if (!addr.isLoopbackAddress() && addr instanceof Inet4Address) {
                        sb.append(addr.getHostAddress());
                        break label34;
                    }
                }
            }

            if (sb.length() == 0) {
                sb.append(InetAddress.getLocalHost().getHostAddress());
            }
        } catch (SocketException socketE) {
            ;
        } catch (UnknownHostException unkownhostE) {
            ;
        }

        sb.append("-");
        String pidStr = ManagementFactory.getRuntimeMXBean().getName();
        sb.append(pidStr.substring(0, pidStr.indexOf(64)));
        sb.append("-");
        sb.append(String.format("%04X", serialNo.getAndIncrement()));
        return sb.toString();
    }

    public Histogram getTrafficHistogram() {
        return this.trafficHistogram;
    }

    public Timer getProcessTimer() {
        return this.processTimer;
    }

    public Meter getAckRequests() {
        return this.ackRequests;
    }

    public long getFailedCount() {
        return this.processFailed.getCount();
    }

    public boolean isStartFromLatest() {
        return this.config.isStartFromLatest();
    }

    public boolean isUpgradeMode() {
        return this.config.isUpgradeMode();
    }



    /*
    重写HttpConsumer中部分方法
     */

    public void onProcessingException(KafkaMessage msg, ProcessingException ex) {
        throw ex;
    }
    public void onThreadException(Throwable exception) {
    }
    public void process(KafkaMessage msg) throws ProcessingException {
        Throwable cause = null;
        long offset = msg.getOffset();
        String msgKey = msg.getKey();
        byte[] payload = msg.getPayload();
        int i = 0;

        while(i < this.config.getMaxProcessRetries()) {
            try {
                this.onMessageReceived(offset, msgKey, payload);
                cause = null;
                break;
            } catch (Throwable throwable) {
                cause = throwable;
                ++i;
            }
        }

        this.submitTrafficEvent((long)payload.length);
        if (cause != null) {
            throw new ProcessingException(msg, cause);
        }
    }
    public String getKafkaTopic() {
        return this.config.getKafkaTopic();
    }
    public int getKafkaPartition() {
        return this.config == null ? -1 : this.config.getKafkaPartition();
    }
    public String getConsumerId() {
        return this.consumerId;
    }
    public String getConsumerGroup() {
        return this.config.getConsumerGroup();
    }
    public long getSessionTimeout() {
        return this.sessionTimeout;
    }
    public RegisterResponse register() {
        return (new RegisterCommand(this)).execute();
    }
    public UnregisterResponse unregister() {
        return (new UnregisterCommand(this)).execute();
    }
    public long findOldOffset() {
        String servers = this.config.getRedisServers();
        if (servers != null && servers.length() != 0) {
            long oldOffset = -1L;
            String[] serverArr = servers.split("[,]");
            int len = serverArr.length;

            for(int i = 0; i < len; ++i) {
                String nodeStr = serverArr[i];
                String[] pair = nodeStr.split("[:]");
                long offset = this.findOldOffset(pair[0], Integer.parseInt(pair[1]));
                if (offset > oldOffset) {
                    oldOffset = offset;
                }
            }

            if (oldOffset == -1L) {
                String topic = this.getKafkaTopic();
                log.warn("cannot found topic " + topic + " old consumer's offset from redis " + servers);
            }

            return oldOffset;
        } else {
            log.warn("redis servers is null, cannot found old offset.");
            return -1L;
        }
    }
    public SyncResponse syncOffset(long offset) {
        return (new SyncCommand(this, offset)).execute();
    }
    public FetchResponse fetch() {
        return (new FetchCommand(this)).execute();
    }
    public AckResponse ack(long offset) {
        return (new AckCommand(this, offset)).execute();
    }
    public HeartbeatResponse sendHeartbeat(long offset) {
        return (new HeartbeatCommand(this, offset)).execute();
    }
    public ConsumerState getConsumerState() {
        return this.consumerState;
    }
    public long incrCount() {
        this.success.inc();
        return this.success.getCount();
    }
    public long getCount() {
        return this.success.getCount();
    }
}
