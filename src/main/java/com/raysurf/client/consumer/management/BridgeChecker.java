package com.raysurf.client.consumer.management;

import com.raysurf.client.consumer.AbstractHttpConsumer;
import com.raysurf.client.consumer.ConsumerState;
import com.raysurf.client.util.HttpResult;
import com.raysurf.client.util.IOUtils;
import com.raysurf.client.util.MyThreadFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class BridgeChecker {
    private static final Log log = LogFactory.getLog(BridgeChecker.class);
    private static final Map<String, BridgeChecker> instances = new HashMap<>();
    private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(new MyThreadFactory("KafkaBridgeChecker"));
    private ScheduledFuture<?> scheduledTaskFuture;
    private final String bridgeServer;
    private final URL bridgeStatusUrl;
    private volatile boolean started;

    private volatile static BridgeChecker bridgeChecker;

    private BridgeChecker(String bridgeServer) {
        this.bridgeServer = bridgeServer;

        try {
            this.bridgeStatusUrl = new URL(bridgeServer + "/kafka-bridge/manager/status");
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }

        Controller.getInstance().addBridgeChecker(this);
    }

    public static BridgeChecker getInstance(String bridgeServer) {
        if (! instances.containsKey(bridgeServer)) {
            synchronized (BridgeChecker.class) {
                if (! instances.containsKey(bridgeServer)) {
                    bridgeChecker = new BridgeChecker(bridgeServer);
                    instances.put(bridgeServer, bridgeChecker);
                    return bridgeChecker;
                } else {
                    return instances.get(bridgeServer);
                }
            }
        } else {
            return instances.get(bridgeServer);
        }
    }

    public synchronized boolean start() {
        if (this.started) {
            log.info("Kafka bridge checker(" + this.bridgeServer + ") already started!");
            return false;
        } else {
            if (this.scheduledTaskFuture == null || this.scheduledTaskFuture.isCancelled()) {
                this.scheduledTaskFuture = scheduler.scheduleAtFixedRate(this.checkServerAvailable(), 5L, 5L, TimeUnit.SECONDS);
            }

            this.started = true;
            return true;
        }
    }

    public synchronized void stop() {
        if (!this.started) {
            log.warn("Kafka bridge checker(" + this.bridgeServer + ") not started!");
        } else {
            this.started = false;
            if (this.scheduledTaskFuture != null) {
                this.scheduledTaskFuture.cancel(false);
            }

        }
    }

    public String getBridgeServer() {
        return this.bridgeServer;
    }

    private Runnable checkServerAvailable() {
        return new Runnable() {
            public void run() {
                HttpResult httpResult = null;
                Throwable exception = null;
                InputStream is = null;

                HttpURLConnection errMsgx;
                try {
                    errMsgx = (HttpURLConnection)BridgeChecker.this.bridgeStatusUrl.openConnection();
                    errMsgx.setRequestProperty("User-Agent", BridgeChecker.this.getUserAgent());
                    int respCode = errMsgx.getResponseCode();
                    is = respCode == 200 ? errMsgx.getInputStream() : errMsgx.getErrorStream();
                    String respBody = IOUtils.parseString(is, "UTF-8");
                    httpResult = new HttpResult(respCode, respBody);
                } catch (Throwable cause) {
                    BridgeChecker.log.warn(cause.getMessage(), cause);
                    exception = cause;
                } finally {
                    IOUtils.close(is);
                }

                if (httpResult != null && this.isServerAvailable(httpResult)) {
                    Collection<AbstractHttpConsumer> consumers = Controller.getInstance().getAllConsumers();
                    Iterator itr = consumers.iterator();

                    while(itr.hasNext()) {
                        AbstractHttpConsumer consumer = (AbstractHttpConsumer)itr.next();
                        if (BridgeChecker.this.bridgeServer.equals(consumer.getBridgeServer()) && consumer.getConsumerState() == ConsumerState.SUSPENDED) {
                            consumer.resume();
                        }
                    }
                } else {
                    String errMsg;
                    if (httpResult == null) {
                        errMsg = exception == null ? "unknown" : exception.getMessage();
                    } else {
                        errMsg = httpResult.httpCode + ", " + httpResult.rawData;
                    }

                    BridgeChecker.log.warn("kafka bridge server unavailable, reason: " + errMsg);
                }

            }

            private boolean isServerAvailable(HttpResult r) {
                if (r == null) {
                    return false;
                } else if (r.httpCode != 200) {
                    return false;
                } else {
                    return r.rawData != null && r.rawData.indexOf("normal") != -1;
                }
            }
        };
    }

    private String getUserAgent() {
        StringBuilder sb = new StringBuilder("kafka-http-client");
        String version = System.getProperty("kafka-http-client-version");
        if (version != null && !version.isEmpty()) {
            sb.append("-").append(version);
        }

        sb.append("/java").append(System.getProperty("java.version"));
        return sb.toString();
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    BridgeChecker.scheduler.shutdownNow();
                } catch (Throwable cause) {
                    ;
                }

            }
        });
    }
}
