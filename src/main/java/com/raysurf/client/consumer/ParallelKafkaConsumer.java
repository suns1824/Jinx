package com.raysurf.client.consumer;

import com.raysurf.client.consumer.Exception.CommunicationException;
import com.raysurf.client.consumer.management.BridgeChecker;
import com.raysurf.client.consumer.management.Controller;
import com.raysurf.client.consumer.response.RegisterResponse;
import com.raysurf.client.consumer.response.SyncResponse;
import com.raysurf.client.consumer.response.UnregisterResponse;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ParallelKafkaConsumer extends AbstractHttpConsumer {

    private volatile ParallelKafkaConsumer.PLooper[] loopers;
    private volatile ParallelKafkaConsumer.HeartbeatThread heartbeatThread;
    private volatile int looperThreadNum = 1;
    private final Map<ParallelKafkaConsumer.PLooper, LooperState> deadLoopers = new HashMap();
    private final Thread shutdownHook = new Thread(() -> {
        ParallelKafkaConsumer.this.consumerState = ConsumerState.EXITING;
        ParallelKafkaConsumer.this.stopHeartbeat();
        ParallelKafkaConsumer.this.stopWork();
        ParallelKafkaConsumer.this.consumerState = ConsumerState.EXITED;
    });

    public ParallelKafkaConsumer(ConsumerConfig config) {
        super(config);
        this.consumerState = ConsumerState.INITIALIZED;
    }


    private synchronized void stopWork() {
        if (this.loopers != null && this.loopers.length > 0) {
            this.gracefulStop();
            this.unregisterShutdownHook();
        }

    }

    private void unregisterShutdownHook() {
        try {
            Runtime.getRuntime().removeShutdownHook(this.shutdownHook);
        } catch (IllegalStateException e) {
            ;
        }

    }

    private synchronized void stopHeartbeat() {
        if (this.heartbeatThread != null) {
            this.heartbeatThread.stopHeartbeat = true;
        }

    }

    private void gracefulStop() {
        long t1 = System.currentTimeMillis();

        while(this.isWorking() && (System.currentTimeMillis() - t1) / 1000L < 10L) {
            log.warn("waiting for kafka client thread terminated.");

            try {
                Thread.sleep(500L);
            } catch (InterruptedException e) {
                ;
            }
        }

        if (this.isWorking()) {
            log.warn("consumer threads still working after 10 seconds.");
            StringBuilder st = new StringBuilder("--kafka client thread stack trace--\n");
            ParallelKafkaConsumer.PLooper[] pLoopers = this.loopers;
            int len = pLoopers.length;

            for(int i = 0; i < len; ++i) {
                ParallelKafkaConsumer.PLooper looper = pLoopers[i];
                if (looper.isAlive()) {
                    StackTraceElement[] stackTraces = looper.getStackTrace();
                    int stackTraceLen = stackTraces.length;

                    for(int j = 0; j < stackTraceLen; ++j) {
                        StackTraceElement e = stackTraces[j];
                        st.append(e.toString()).append("\n");
                    }
                }
            }

            log.warn(st.toString());
        } else {
            log.warn("consumer " + this.getConsumerName() + " all threads terminated. total " + this.getCount() + " messages processed.");
        }

        try {
            UnregisterResponse resp = this.unregister();
            if (resp.isOK()) {
                log.warn("consumer " + this.getConsumerName() + " unregister ok.");
            } else {
                log.error("consumer " + this.getConsumerName() + " unregister failed: " + resp.getErrMsg());
            }
        } catch (CommunicationException e) {
            log.error(e.getMessage(), e);
        }

    }

    public int getDeadThreadNum() {
        return this.deadLoopers.size();
    }

    public int getLooperThreadNum() {
        return this.looperThreadNum;
    }

    private boolean isWorking() {
        if (this.loopers != null && this.loopers.length != 0) {
            ParallelKafkaConsumer.PLooper[] loopArr = this.loopers;
            int len = loopArr.length;

            for(int i = 0; i < len; ++i) {
                ParallelKafkaConsumer.PLooper looper = loopArr[i];
                if (looper != null && looper.isAlive()) {
                    return true;
                }
            }
            return false;
        } else {
            return false;
        }
    }

    protected boolean isStarted() {
        return this.consumerState == ConsumerState.STARTED;
    }

    private void registerAndStartWork() {
        RegisterResponse resp = this.register();
        if (resp.isOK()) {
            log.info("consumer " + this.getConsumerName() + " register ok.");
            this.sessionTimeout = resp.getSessionTimeout();
            if (resp.isNeedSyncOldOffset()) {
                this.syncOldOffset();
            }
            this.startWork();
        } else {
            log.error("register failed: " + resp.getErrMsg());
            throw new RuntimeException(resp.getErrMsg());
        }
    }

    private void startWork() {
        this.statSvc.startScheduler();
        this.startLoopers();
        this.startHeartbeat();
        this.registerShutdownHook();
        this.consumerState = ConsumerState.STARTED;
        this.startChecker();
    }

    private void registerShutdownHook() {
        try {
            Runtime.getRuntime().addShutdownHook(this.shutdownHook);
        } catch (IllegalArgumentException e) {
            ;
        }
    }

    private synchronized void startLoopers() {
        if (!this.deadLoopers.isEmpty()) {
            this.deadLoopers.clear();
        }

        int i;
        if (this.loopers == null) {
            this.loopers = new ParallelKafkaConsumer.PLooper[this.looperThreadNum];

            for(i = 0; i < this.looperThreadNum; ++i) {
                this.loopers[i] = new ParallelKafkaConsumer.PLooper(i);
                this.loopers[i].start();
            }
        } else {
            for(i = 0; i < this.loopers.length; ++i) {
                if (this.loopers[i] == null || !this.loopers[i].isAlive()) {
                    this.loopers[i] = new ParallelKafkaConsumer.PLooper(i);
                    this.loopers[i].start();
                }
            }
        }

        log.info("Consumer " + this.getConsumerName() + " loop threads started.");
    }

    private synchronized void startHeartbeat() {
        if (this.heartbeatThread == null || this.heartbeatThread.stopHeartbeat || !this.heartbeatThread.isAlive()) {
            this.heartbeatThread = new ParallelKafkaConsumer.HeartbeatThread("kafka-consumer-heartbeat-" + this.getKafkaTopic());
            this.heartbeatThread.start();
        }
    }

    private void syncOldOffset() {
        long oldOffset = this.findOldOffset();
        if (oldOffset >= 0L) {
            SyncResponse syncResp = this.syncOffset(oldOffset);
            SyncResponse.Status st = syncResp.getStatus();
            if (st != SyncResponse.Status.SERVER_ERROR && st != SyncResponse.Status.REQUEST_PARAM_ILLEGAL) {
                log.warn("sync old consumer offset " + oldOffset + ", response: " + syncResp.getStatus());
            } else {
                log.error("sync old consumer offset error: " + st);
                if (this.isUpgradeMode()) {
                    throw new RuntimeException("sync offset error: " + st);
                }
            }
        } else if (this.isUpgradeMode()) {
            String key = this.getOldOffsetKey();
            throw new RuntimeException("cannot find old offset by key " + key + " check redis server: " + this.config.getRedisServers());
        }
    }

    private String getOldOffsetKey() {
        String offsetKey = this.config.getConsumerGroup() + "kafka-topic-offset:" + this.getKafkaTopic();
        String prefix = this.config.getRedisSchema();
        if (prefix != null) {
            offsetKey = prefix + offsetKey;
        }

        return offsetKey;
    }

    private void startChecker() {
        BridgeChecker checker = BridgeChecker.getInstance(this.getBridgeServer());
        if (checker.start()) {
            Controller.getInstance().addBridgeChecker(checker);
        }

    }

    /*
    实现HttpConsumer接口方法
     */
    public void onMessageReceived(long offset, String msgKey, byte[] payload) {

    }
    public String getCurrentThreadId() {
        Thread t = Thread.currentThread();
        return t instanceof ParallelKafkaConsumer.PLooper ? "pt" + ((ParallelKafkaConsumer.PLooper)t).threadNo : t.getName();
    }
    public String getMode() {
        return "parallel";
    }
    public synchronized void startUpgrade() {
        if (this.isStarted()) {
            log.warn("consumer " + this.getConsumerName() + " already started.");
        } else {
            this.registerAndStartWork();
        }
    }
    public synchronized void start() {
        if (this.isStarted()) {
            log.warn("consumer " + this.getConsumerName() + " already started.");
        } else {
            this.consumerState = ConsumerState.STARTING;
            if (!this.isUpgradeMode()) {
                this.registerAndStartWork();
            } else {
                RegisterResponse resp = this.register();
                if (resp != null && resp.isOK()) {
                    if (resp.isNeedSyncOldOffset()) {
                        this.unregister();
                        log.warn("consumer " + this.getConsumerName() + " will not start in upgrade mode.");
                        return;
                    }

                    this.startWork();
                }

            }
        }
    }
    public synchronized void resume() {
        if (this.consumerState != ConsumerState.SUSPENDED && this.consumerState != ConsumerState.ABORTED && this.consumerState != ConsumerState.STOPPED) {
            log.warn("consumer " + this.getConsumerName() + " current state is " + this.consumerState);
        } else {
            this.consumerState = ConsumerState.STARTING;
            this.startWork();
        }
    }
    public synchronized void stop() {
        if (this.consumerState == ConsumerState.STARTED || this.consumerState == ConsumerState.STARTING || this.consumerState == ConsumerState.SUSPENDED) {
            this.consumerState = ConsumerState.STOPPING;
            this.stopHeartbeat();
            this.stopWork();
            this.consumerState = ConsumerState.STOPPED;
        }
    }

    private class PLooper extends Looper {
        int threadNo;

        PLooper(int threadNo) {
            super(ParallelKafkaConsumer.this);
            this.threadNo = threadNo;
            this.setName("p-kafka-http-consumer-t" + threadNo + "-for-" + ParallelKafkaConsumer.this.getKafkaTopic());
            this.setDaemon(true);
            this.addListener(this.buildListener());
        }

        private LooperStateChangeListener buildListener() {
            return new LooperStateChangeListener() {
                public void stateChange(LooperStateChangeEvent e) {
                    LooperState newState = e.getNewState();
                    if (newState != null) {
                        if (newState == LooperState.ABORTED || newState == LooperState.SUSPENDED) {
                            ParallelKafkaConsumer.this.deadLoopers.put(PLooper.this, newState);
                            if (ParallelKafkaConsumer.this.deadLoopers.size() == ParallelKafkaConsumer.this.loopers.length) {
                                this.updateConsumerState();
                            }
                        }

                    }
                }

                private void updateConsumerState() {
                    Iterator itr = ParallelKafkaConsumer.this.deadLoopers.entrySet().iterator();

                    Map.Entry entry;
                    do {
                        if (!itr.hasNext()) {
                            ParallelKafkaConsumer.this.consumerState = ConsumerState.ABORTED;
                            return;
                        }

                        entry = (Map.Entry)itr.next();
                    } while(entry.getValue() != LooperState.SUSPENDED);

                    ParallelKafkaConsumer.this.consumerState = ConsumerState.SUSPENDED;
                }
            };
        }
    }

    private final class HeartbeatThread extends Thread {
        private long lastSentTime = 0L;
        private volatile boolean stopHeartbeat = false;

        public HeartbeatThread(String name) {
            super(name);
        }

        private void checkLoopers() {
            if (!this.stopHeartbeat && ParallelKafkaConsumer.this.loopers != null) {
                ParallelKafkaConsumer.PLooper[] pLoopers = ParallelKafkaConsumer.this.loopers;
                int len = pLoopers.length;

                for(int i = 0; i < len; ++i) {
                    Looper looper = pLoopers[i];
                    if (this.stopHeartbeat) {
                        return;
                    }

                    if (looper.isAlive() && looper.isSessionTimeout()) {
                        try {
                            ParallelKafkaConsumer.this.sendHeartbeat(looper.getCurrentOffset());
                        } catch (CommunicationException e) {
                            AbstractHttpConsumer.log.error(e.getMessage(), e);
                        }
                    }
                }

                if (!this.stopHeartbeat && ParallelKafkaConsumer.this.isWorking()) {
                    long ts = System.currentTimeMillis();
                    if (ts - this.lastSentTime > 3000L) {
                        try {
                            ParallelKafkaConsumer.this.sendHeartbeat(-1L);
                        } catch (CommunicationException e) {
                            AbstractHttpConsumer.log.error(e.getMessage(), e);
                        }

                        this.lastSentTime = System.currentTimeMillis();
                    }
                }

                if (ParallelKafkaConsumer.this.loopers != null && ParallelKafkaConsumer.this.deadLoopers.size() == ParallelKafkaConsumer.this.loopers.length) {
                    AbstractHttpConsumer.log.warn("Consumer " + ParallelKafkaConsumer.this.getConsumerName() + " loop threads aborted.");
                }

            }
        }

        private void sleep0() {
            if (!this.stopHeartbeat) {
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    ;
                }
            }

        }

        public void run() {
            while(true) {
                if (!this.stopHeartbeat) {
                    ConsumerState cs = ParallelKafkaConsumer.this.getConsumerState();
                    if (cs == ConsumerState.STARTING) {
                        this.sleep0();
                        continue;
                    }

                    if (cs == ConsumerState.STARTED) {
                        this.checkLoopers();
                        this.sleep0();
                        continue;
                    }

                    AbstractHttpConsumer.log.warn("Consumer " + ParallelKafkaConsumer.this.getConsumerName() + " state is " + cs + ", heartbeat thread will be terminated.");
                }

                AbstractHttpConsumer.log.warn("Consumer " + ParallelKafkaConsumer.this.getConsumerName() + " heartbeat thread terminated.");
                return;
            }
        }
    }

}
