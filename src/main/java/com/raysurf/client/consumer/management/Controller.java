package com.raysurf.client.consumer.management;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Snapshot;
import com.raysurf.client.consumer.AbstractHttpConsumer;
import com.raysurf.client.consumer.ConsumerState;
import com.raysurf.client.consumer.ParallelKafkaConsumer;
import com.wacai.common.middleware.endpoint.Action;
import com.wacai.common.middleware.endpoint.HttpEndpoint;
import com.wacai.common.middleware.httpserver.HttpExchange;
import com.wacai.common.middleware.util.IllegalParameterException;
import com.wacai.common.middleware.util.TableBuilder;

import java.net.InetSocketAddress;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Controller {
    private static final Controller INSTANCE = new Controller();
    private final Map<String, AbstractHttpConsumer> consumers = new HashMap<>();
    private final Map<String, BridgeChecker> bridgeCheckers = new HashMap<>();

    public static Controller getInstance() {
        return INSTANCE;
    }

    private Controller() {
        HttpEndpoint endpoint = HttpEndpoint.getInstance();
        endpoint.registerAction(new Controller.VersionAction());
        endpoint.registerAction(new Controller.StatusAction());
        endpoint.registerAction(new Controller.StartAction());
        endpoint.registerAction(new Controller.StopAction());
        endpoint.registerAction(new Controller.DiagnoseAction());
        endpoint.registerAction(new Controller.HistoryAction());
        endpoint.registerAction(new Controller.StatisticsAction());
        endpoint.registerAction(new Controller.TrafficAction());
        endpoint.registerAction(new Controller.ProcessTimerAction());
    }

    public synchronized void addConsumer(AbstractHttpConsumer consumer) {
        this.consumers.put(consumer.getConsumerId(), consumer);
    }

    public synchronized void removeConsumer(AbstractHttpConsumer consumer) {
        this.consumers.remove(consumer.getConsumerId());
    }

    public Collection<AbstractHttpConsumer> getAllConsumers() {
        return Collections.unmodifiableCollection(this.consumers.values());
    }

    public synchronized void addBridgeChecker(BridgeChecker checker) {
        if (checker != null) {
            String server = checker.getBridgeServer();
            if (!this.bridgeCheckers.containsKey(server)) {
                this.bridgeCheckers.put(server, checker);
            }
        }

    }

    private static String now() {
        return (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date());
    }

    class TrafficAction extends Action {
        public TrafficAction() {
            super("kafka-consumer", "/kafka-consumer/traffic");
        }

        public String execute(Map<String, String> params) throws IllegalParameterException {
            return this.formatTrafficMetrics();
        }

        private String formatTrafficMetrics() {
            TableBuilder tb = new TableBuilder(10);
            String[] header = new String[]{"Traffic", "TotalCount", "SnapshotSize", "Min(Byte)", "Max", "Mean", "Median", "75%", "95%", "99.9%"};
            tb.setHeader(header);
            DecimalFormat format = new DecimalFormat("#.##");
            Iterator itr = Controller.this.consumers.values().iterator();

            while(itr.hasNext()) {
                AbstractHttpConsumer c = (AbstractHttpConsumer)itr.next();
                String[] ha = this.toArray(c.getTrafficHistogram(), format);
                tb.addRow(new String[]{"incoming", ha[0], ha[1], ha[2], ha[3], ha[4], ha[5], ha[6], ha[7], ha[8]});
            }

            return tb.toString();
        }

        private String[] toArray(Histogram h, DecimalFormat format) {
            Snapshot s = h.getSnapshot();
            String count = String.valueOf(h.getCount());
            String size = String.valueOf(s.size());
            String min = format.format(s.getMin());
            String max = format.format(s.getMax());
            String mean = format.format(s.getMean());
            String median = format.format(s.getMedian());
            String p75 = format.format(s.get75thPercentile());
            String p95 = format.format(s.get95thPercentile());
            String p999 = format.format(s.get999thPercentile());
            return new String[]{count, size, min, max, mean, median, p75, p95, p999};
        }
    }

    class ProcessTimerAction extends Action {
        public ProcessTimerAction() {
            super("kafka-consumer", "/kafka-consumer/timer");
        }

        public String execute(Map<String, String> params) throws IllegalParameterException {
            StringBuilder sb = new StringBuilder();
            DecimalFormat df = new DecimalFormat("#.##");
            TableBuilder tb = new TableBuilder(9);
            String[] header = new String[]{"Consumer", "SnapshotSize", "Min(ms)", "Max", "Mean", "Median", "75%", "95%", "99.9%"};
            tb.setHeader(header);
            Iterator itr = Controller.this.consumers.values().iterator();

            while(itr.hasNext()) {
                AbstractHttpConsumer c = (AbstractHttpConsumer)itr.next();
                Snapshot s = c.getProcessTimer().getSnapshot();
                String size = String.valueOf(s.size());
                String min = df.format((double)(s.getMin() / 1000L) / 1000.0D);
                String max = df.format((double)(s.getMax() / 1000L) / 1000.0D);
                String mean = df.format(s.getMean() / 1000.0D / 1000.0D);
                String median = df.format(s.getMedian() / 1000.0D / 1000.0D);
                String p75 = df.format(s.get75thPercentile() / 1000.0D / 1000.0D);
                String p95 = df.format(s.get95thPercentile() / 1000.0D / 1000.0D);
                String p999 = df.format(s.get999thPercentile() / 1000.0D / 1000.0D);
                tb.addRow(new String[]{c.getConsumerName(), size, min, max, mean, median, p75, p95, p999});
            }

            sb.append(tb.toString());
            return sb.toString();
        }
    }

    class StatisticsAction extends Action {
        public StatisticsAction() {
            super("kafka-consumer", "/kafka-consumer/stats");
        }

        public String execute(Map<String, String> params) throws IllegalParameterException {
            TableBuilder tb = new TableBuilder(7);
            String[] header = new String[]{"Consumer", "AckRequests", "AckSuccess", "1MinuteRate", "5MinuteRate", "15MinuteRate", "MeanRate"};
            tb.setHeader(header);
            DecimalFormat format = new DecimalFormat("#.##");
            Iterator itr = Controller.this.consumers.values().iterator();

            while(itr.hasNext()) {
                AbstractHttpConsumer c = (AbstractHttpConsumer)itr.next();
                Meter ackRequests = c.getAckRequests();
                String total = String.valueOf(ackRequests.getCount());
                String succ = String.valueOf(c.getCount());
                String last1m = format.format(ackRequests.getOneMinuteRate());
                String last5m = format.format(ackRequests.getFiveMinuteRate());
                String last15m = format.format(ackRequests.getFifteenMinuteRate());
                String mean = format.format(ackRequests.getMeanRate());
                tb.addRow(new String[]{c.getConsumerName(), total, succ, last1m, last5m, last15m, mean});
            }

            return tb.toString();
        }
    }

    class HistoryAction extends Action {
        public HistoryAction() {
            super("kafka-consumer", "/kafka-consumer/history");
        }

        public String execute(Map<String, String> params) throws IllegalParameterException {
            return StatisticsService.getInstance().getHistory();
        }
    }

    class StopAction extends Action {
        public StopAction() {
            super("kafka-consumer", "/kafka-consumer/stop");
        }

        public void handle(HttpExchange he) {
            if (!"POST".equalsIgnoreCase(he.getRequestMethod())) {
                this.response(he, "please use http post method.");
            } else {
                InetSocketAddress remoteAddr = he.getRemoteAddress();
                System.out.println(Controller.now() + " receive http request to stop kafka-consumer from " + remoteAddr);
                super.handle(he);
            }
        }

        public String execute(Map<String, String> params) throws IllegalParameterException {
            String consumerId = "all";
            if (params != null && !params.isEmpty()) {
                consumerId = (String)params.get("consumer");
                if (consumerId == null || consumerId.isEmpty()) {
                    consumerId = "all";
                }
            }

            return "all".equals(consumerId) ? this.stopAllConsumers() : this.stopSingleConsumer(consumerId);
        }

        private String stopSingleConsumer(String consumerId) throws IllegalParameterException {
            AbstractHttpConsumer consumer = (AbstractHttpConsumer)Controller.this.consumers.get(consumerId);
            if (consumer == null) {
                throw new IllegalParameterException("unknown consumer: " + consumerId);
            } else {
                System.out.println(Controller.now() + " stop kafka-consumer " + consumerId);
                String res = this.stopConsumer(consumer);
                String server = consumer.getBridgeServer();
                boolean hasOthers = false;
                Iterator itr = Controller.this.consumers.values().iterator();

                while(itr.hasNext()) {
                    AbstractHttpConsumer c = (AbstractHttpConsumer)itr.next();
                    if (c.getBridgeServer().equals(server) && c.getConsumerState() == ConsumerState.STARTED) {
                        hasOthers = true;
                        break;
                    }
                }

                if (!hasOthers) {
                    ((BridgeChecker)Controller.this.bridgeCheckers.get(server)).stop();
                }

                return res;
            }
        }

        private String stopAllConsumers() {
            Iterator itr = Controller.this.bridgeCheckers.values().iterator();

            while(itr.hasNext()) {
                BridgeChecker checker = (BridgeChecker)itr.next();
                checker.stop();
            }

            int liveNum = 0;
            Collection<AbstractHttpConsumer> consumerSet = Controller.this.consumers.values();
            Iterator consumerIterator = consumerSet.iterator();

            while(consumerIterator.hasNext()) {
                AbstractHttpConsumer consumer = (AbstractHttpConsumer)consumerIterator.next();
                ConsumerState cs = consumer.getConsumerState();
                if (cs == ConsumerState.STARTED) {
                    ++liveNum;
                }
            }

            if (liveNum == 0) {
                return "All kafka consumers already stopped.";
            } else {
                ExecutorService stopExecutor = Executors.newFixedThreadPool(liveNum);
                return this.parallelStopConsumers(consumerSet, stopExecutor);
            }
        }

        private String parallelStopConsumers(Collection<AbstractHttpConsumer> consumerSet, ExecutorService stopExecutor) {
            CompletableFuture<String>[] futures = new CompletableFuture[consumerSet.size()];
            int i = 0;
            for(Iterator itr = consumerSet.iterator(); itr.hasNext(); futures[i++] = CompletableFuture.supplyAsync(() -> {
                return this.stopConsumer((AbstractHttpConsumer)itr.next());
            }, stopExecutor)) {
                //
            }

            try {
                CompletableFuture.allOf(futures).join();
            } finally {
                stopExecutor.shutdownNow();
            }

            StringBuilder sb = new StringBuilder();
            CompletableFuture[] futureArr = futures;
            int len = futures.length;

            for(int j = 0; j < len; ++j) {
                CompletableFuture f = futureArr[j];

                try {
                    sb.append((String)f.get()).append(System.lineSeparator());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            return sb.toString();
        }

        private String stopConsumer(AbstractHttpConsumer consumer) {
            consumer.stop();
            return "consumer " + consumer.toString() + " stopped.";
        }
    }

    class StartAction extends Action {
        public StartAction() {
            super("kafka-consumer", "/kafka-consumer/start");
        }

        public void handle(HttpExchange he) {
            if (!"POST".equalsIgnoreCase(he.getRequestMethod())) {
                this.response(he, "please use http post method.");
            } else {
                super.handle(he);
            }
        }

        public String execute(Map<String, String> params) throws IllegalParameterException {
            String consumerId = "all";
            if (params != null && !params.isEmpty()) {
                consumerId = (String)params.get("consumer");
                if (consumerId == null || consumerId.isEmpty()) {
                    consumerId = "all";
                }
            }

            if (!"all".equals(consumerId)) {
                AbstractHttpConsumer consumer = (AbstractHttpConsumer)Controller.this.consumers.get(consumerId);
                if (consumer == null) {
                    throw new IllegalParameterException("unknown consumer: " + consumerId);
                } else {
                    return this.startConsumer(consumer);
                }
            } else {
                StringBuilder sb = new StringBuilder();
                Iterator itr = Controller.this.consumers.values().iterator();

                while(itr.hasNext()) {
                    AbstractHttpConsumer consumerx = (AbstractHttpConsumer)itr.next();
                    sb.append(this.startConsumer(consumerx)).append(System.lineSeparator());
                }

                return sb.toString();
            }
        }

        private synchronized String startConsumer(AbstractHttpConsumer consumer) {
            StringBuilder sb = new StringBuilder();
            sb.append("consumer ").append(consumer.getConsumerName());
            if (consumer.getConsumerState() == ConsumerState.STARTED) {
                sb.append(" already started.");
                return sb.toString();
            } else {
                boolean failed = false;
                Throwable err = null;

                try {
                    if (consumer.isUpgradeMode()) {
                        consumer.startUpgrade();
                    } else {
                        consumer.start();
                    }
                } catch (Throwable cause) {
                    failed = true;
                    err = cause;
                }

                if (failed) {
                    sb.append(" start failed. ");
                    if (err != null) {
                        sb.append(err.getMessage());
                    }
                } else {
                    sb.append(" started.");
                }

                String bridgeServer = consumer.getBridgeServer();
                BridgeChecker checker = (BridgeChecker)Controller.this.bridgeCheckers.get(bridgeServer);
                if (checker == null) {
                    checker = BridgeChecker.getInstance(bridgeServer);
                    Controller.this.bridgeCheckers.put(bridgeServer, checker);
                }

                checker.start();
                return sb.toString();
            }
        }
    }

    class DiagnoseAction extends Action {
        public DiagnoseAction() {
            super("kafka-consumer", "/kafka-consumer/diagnose");
        }

        public String execute(Map<String, String> params) throws IllegalParameterException {
            return StatisticsService.getInstance().getDiagnoseInfo();
        }
    }

    class StatusAction extends Action {
        public StatusAction() {
            super("kafka-consumer", "/kafka-consumer/status");
        }

        public String execute(Map<String, String> params) throws IllegalParameterException {
            TableBuilder tb = new TableBuilder(7);
            String[] header = new String[]{"Topic/Partition", "ConsumerGroup", "ConsumerId", "Mode", "Status", "Acked", "Failed"};
            tb.setHeader(header);
            Iterator itr = Controller.this.consumers.values().iterator();

            while(itr.hasNext()) {
                AbstractHttpConsumer consumer = (AbstractHttpConsumer)itr.next();
                String topic = consumer.getKafkaTopic() + ":" + consumer.getKafkaPartition();
                String consumerGrp = consumer.getConsumerGroup();
                String consumerId = consumer.getConsumerId();
                String mode = consumer.getMode();
                if (consumer instanceof ParallelKafkaConsumer) {
                    mode = mode + "(" + ((ParallelKafkaConsumer)consumer).getLooperThreadNum() + " threads)";
                }

                String state = "not running";
                ConsumerState cs = consumer.getConsumerState();
                if (cs == ConsumerState.STARTED) {
                    if (consumer instanceof ParallelKafkaConsumer) {
                        ParallelKafkaConsumer pc = (ParallelKafkaConsumer)consumer;
                        int totalThreads = pc.getLooperThreadNum();
                        int deadThreads = pc.getDeadThreadNum();
                        int livedThreads = totalThreads - deadThreads;
                        state = "running(" + livedThreads + "/" + totalThreads + ")";
                    } else {
                        state = "running";
                    }
                } else {
                    state = cs.toString().toLowerCase();
                }

                String acked = String.valueOf(consumer.getCount());
                String failed = String.valueOf(consumer.getFailedCount());
                tb.addRow(new String[]{topic, consumerGrp, consumerId, mode, state, acked, failed});
            }

            return tb.toString();
        }
    }

    class VersionAction extends Action {
        public VersionAction() {
            super("kafka-consumer", "/kafka-consumer/version");
        }

        public String execute(Map<String, String> params) throws IllegalParameterException {
            String ver = System.getProperty("kafka-http-client-version");
            return ver != null && !ver.isEmpty() ? ver : "unknown";
        }
    }
}

