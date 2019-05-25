package com.raysurf.client.consumer;

import com.raysurf.client.consumer.Exception.CommunicationException;
import com.raysurf.client.consumer.Exception.ProcessingException;
import com.raysurf.client.consumer.response.AckResponse;
import com.raysurf.client.consumer.response.FetchResponse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.UnsupportedEncodingException;
import java.util.*;

public class Looper extends Thread {
    protected static final Log log = LogFactory.getLog(Looper.class);
    protected final AbstractHttpConsumer consumer;
    protected final Map<LooperState, Looper.StateAction> actions = new HashMap<LooperState, StateAction>();
    protected final Set<LooperStateChangeListener> listeners = new HashSet<LooperStateChangeListener>();
    private volatile LooperState currentState;
    private volatile KafkaMessage message;
    private volatile long processBeginTime;
    private volatile long processEndTime;

    public Looper(AbstractHttpConsumer consumer) {
        this.currentState = LooperState.WAITING;
        this.processBeginTime = 0L;
        this.processEndTime = 0L;
        this.consumer = consumer;
        this.initActions();
    }

    public void run() {
        this.currentState = LooperState.FETCHING;
        this.loop();
    }

    public long getCurrentOffset() {
        return this.message == null ? -1L : this.message.getOffset();
    }

    public boolean isSessionTimeout() {
        if (this.isAlive() && this.currentState == LooperState.PROCESSING) {
            long elapsed = this.processEndTime >= this.processBeginTime ? this.processEndTime - this.processBeginTime : System.nanoTime() - this.processBeginTime;
            long max = this.consumer.getSessionTimeout() - 2500L;
            return elapsed / 1000L / 1000L >= max;
        } else {
            return false;
        }
    }

    private void loop() {
        while (true) {
            try {
                LooperState oldState = this.currentState;
                LooperState newState = this.performAction();
                this.currentState = newState;
                this.fireStateChange(new LooperStateChangeEvent(this, oldState, newState));
            } catch (Throwable cause) {
                this.handleException(cause);
                break;
            }
            ConsumerState cs = this.consumer.getConsumerState();
            if (cs != ConsumerState.STARTED && cs != ConsumerState.STARTING && (this.currentState == LooperState.FINISHED || this.currentState == LooperState.WAITING)) {
                log.warn("Looper thread " + this.getName() + " terminated.");
                break;
            }
        }
    }

    private LooperState performAction() throws Exception {
        Looper.StateAction action = (Looper.StateAction)this.actions.get(this.currentState);
        return action.perform();
    }

    private void handleException(Throwable t) {
        log.error("thread " + this.getName() + " aborted by " + t.getMessage());
        Throwable cause = t.getCause();
        if (cause != null) {
            log.error(cause.getMessage(), cause);
        }

        this.consumer.processFailed.inc();
        LooperState oldState = this.currentState;
        LooperState newState;
        if (t instanceof CommunicationException) {
            newState = LooperState.SUSPENDED;
        } else {
            newState = LooperState.ABORTED;
        }

        this.currentState = newState;
        this.fireStateChange(new LooperStateChangeEvent(this, oldState, newState));
        this.consumer.onThreadException(t);
    }

    private void fireStateChange(LooperStateChangeEvent e) {
        Iterator itr = this.listeners.iterator();

        while(itr.hasNext()) {
            LooperStateChangeListener listener = (LooperStateChangeListener)itr.next();
            listener.stateChange(e);
        }

    }

    public LooperState getLooperState() {
        return this.currentState;
    }

    public void addListener(LooperStateChangeListener listener) {
        this.listeners.add(listener);
    }

    private void initActions() {
        this.actions.put(LooperState.WAITING, new StateAction() {
            public LooperState perform() throws Exception {
                try {
                    int sleepTime = 1100;
                    if (Looper.this.consumer instanceof ParallelKafkaConsumer) {
                        ParallelKafkaConsumer pc = (ParallelKafkaConsumer) Looper.this.consumer;
                        int alive = pc.getLooperThreadNum() - pc.getDeadThreadNum();
                        if (alive > 1 && alive < 5) {
                            sleepTime = 1500;
                        } else if (alive >= 5) {
                            sleepTime = 2000;
                        }
                    }
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    ;
                }
                return LooperState.FETCHING;
            }
        });
        this.actions.put(LooperState.FETCHING, new Looper.StateAction() {
            public LooperState perform() throws Exception {
                long t1 = System.currentTimeMillis();
                FetchResponse resp = Looper.this.consumer.fetch();
                long t2 = System.currentTimeMillis();
                Looper.this.message = resp.getMessage();
                FetchResponse.Status status = resp.getStatus();
                LooperState next = null;
                long elapsed = t2 - t1;
                if (Looper.log.isDebugEnabled()) {
                    StringBuilder sb = new StringBuilder();
                    if (status == FetchResponse.Status.OK && Looper.this.message != null) {
                        sb.append("Fetch OK, offset " + Looper.this.message.getOffset());
                    } else {
                        sb.append("Fetch " + status);
                    }

                    sb.append(", elapsed: ").append(elapsed);
                    Looper.log.debug(sb.toString());
                }

                if (Looper.this.message != null && elapsed > 3000L) {
                    Looper.this.traceFetchElapsed(Looper.this.message.getOffset(), resp.getInvokeElapsed());
                }

                switch(status) {
                    case OK:
                        next = LooperState.PROCESSING;
                        break;
                    case DEFER:
                    case SERVER_ERROR:
                    case LOCK_CONFLICT:
                        next = LooperState.WAITING;
                        break;
                    case REQUEST_PARAM_ILLEGAL:
                        throw new RuntimeException("REQUEST_PARAM_ILLEGAL");
                }

                return next;
            }
        });
        this.actions.put(LooperState.PROCESSING, new Looper.StateAction() {
            public LooperState perform() throws Exception {
                Looper.this.processBeginTime = System.nanoTime();

                try {
                    Looper.this.consumer.process(Looper.this.message);
                } catch (ProcessingException e) {
                    Looper.log.warn(e.getMessage(), e);
                    Looper.this.consumer.onProcessingException(e.getKafkaMessage(), e);
                }

                Looper.this.processEndTime = System.nanoTime();
                long ackTime = System.currentTimeMillis();
                long elapsed = (Looper.this.processEndTime - Looper.this.processBeginTime) / 1000L / 1000L;
                Looper.this.consumer.submitCommandEvent("process", elapsed);
                Looper.this.consumer.submitMessageEvent(Looper.this.message.getOffset(), Looper.this.getDigest(), ackTime);
                return LooperState.ACKING;
            }
        });
        this.actions.put(LooperState.ACKING, new Looper.StateAction() {
            public LooperState perform() throws Exception {
                long offset = Looper.this.message.getOffset();
                long t1 = System.currentTimeMillis();
                AckResponse ackResp = Looper.this.consumer.ack(offset);
                long elapsed = System.currentTimeMillis() - t1;
                if (elapsed > 3000L) {
                    Looper.this.traceAckElapsed(offset, ackResp.getInvokeElapsed());
                }

                AckResponse.Status status = ackResp.getStatus();
                LooperState next = null;
                switch(status) {
                    case OK:
                        Looper.this.consumer.incrCount();
                        Looper.this.logAck(offset, elapsed);
                        next = LooperState.FINISHED;
                        break;
                    case RECYCLED:
                    case ALREADY_ACKED:
                    case NOT_FOUND:
                    case DISALLOWED:
                        Looper.log.warn("Ack offset " + offset + " failed: " + status);
                        next = LooperState.FINISHED;
                        break;
                    case SERVER_BUSY:
                    case SERVER_ERROR:
                        next = LooperState.ACKING;
                        break;
                    case REQUEST_PARAM_ILLEGAL:
                        throw new RuntimeException("REQUEST_PARAM_ILLEGAL");
                }

                return next;
            }
        });
        this.actions.put(LooperState.FINISHED, new Looper.StateAction() {
            public LooperState perform() throws Exception {
                return LooperState.FETCHING;
            }
        });
    }

    private String getDigest() {
        StringBuilder sb = new StringBuilder();
        String key = this.message.getKey();
        if (key != null) {
            sb.append("key=");
            if (key.length() <= 10) {
                sb.append(key).append(" ");
            } else {
                sb.append(key.substring(0, 7)).append("... ");
            }
        }

        sb.append("msg=");
        KafkaMessageType mt = this.message.getMsgType();
        if (mt == KafkaMessageType.TEXT) {
            byte[] ba = this.message.getPayload();
            if (ba.length < 200) {
                try {
                    String txt = new String(ba, "UTF-8");
                    if (txt.length() <= 10) {
                        sb.append(txt);
                    } else {
                        sb.append(txt.substring(0, 7)).append("...");
                    }
                } catch (UnsupportedEncodingException e) {
                    sb.append("UnsupportedEncodingException");
                }
            } else {
                sb.append("...");
            }
        } else {
            sb.append("binary data...");
        }

        return sb.toString();
    }

    private void logAck(long offset, long elapsed) {
        if (log.isDebugEnabled()) {
            log.debug("Ack offset " + offset + " ok, elapsed: " + elapsed);
        } else if (log.isInfoEnabled()) {
            log.info("Ack offset " + offset + " ok.");
        }
    }

    private void traceAckElapsed(long offset, Map<Integer, Long> elapsedMap) {
        if (elapsedMap != null && !elapsedMap.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("trace ack " + offset + " elapsed in " + elapsedMap.size() + " invokes:");
            Iterator itr = elapsedMap.entrySet().iterator();

            while(itr.hasNext()) {
                Map.Entry<Integer, Long> e = (Map.Entry)itr.next();
                sb.append(System.lineSeparator() + "offset " + offset + " ack-" + e.getKey() + " elapsed: ").append(e.getValue());
            }

            log.warn(sb.toString());
        }
    }

    private void traceFetchElapsed(long offset, Map<Integer, Long> elapsedMap) {
        if (elapsedMap != null && !elapsedMap.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("trace fetch message " + offset + " elapsed in " + elapsedMap.size() + " invokes:");
            Iterator itr = elapsedMap.entrySet().iterator();

            while(itr.hasNext()) {
                Map.Entry<Integer, Long> e = (Map.Entry)itr.next();
                sb.append(System.lineSeparator() + "offset " + offset + " fetch-" + e.getKey() + " elapsed: ").append(e.getValue());
            }

            log.warn(sb.toString());
        }
    }

    protected interface StateAction {
        LooperState perform() throws Exception;
    }
}
