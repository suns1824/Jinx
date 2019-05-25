package com.raysurf.client.consumer.command;

import com.raysurf.client.consumer.AbstractHttpConsumer;
import com.raysurf.client.consumer.Exception.CommunicationException;
import com.raysurf.client.consumer.response.AckResponse;
import com.raysurf.client.consumer.response.AckResponse.Status;
import com.raysurf.client.util.HttpPost;
import com.raysurf.client.util.HttpResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.UnsupportedEncodingException;
import java.net.SocketTimeoutException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

public class AckCommand {
    private static final Log log = LogFactory.getLog(AckCommand.class);
    private final AbstractHttpConsumer consumer;
    private long offset;

    public AckCommand(AbstractHttpConsumer consumer, long offset) {
        this.consumer = consumer;
        this.offset = offset;
    }

    public AckResponse execute() {
        AckResponse resp = null;
        Throwable cause = null;
        int max = this.consumer.getMaxRetries();
        int times = 0;
        Map<Integer, Long> invokeElapsed = new HashMap();
        int i = 0;

        while(i < max) {
            ++times;
            long t1 = System.currentTimeMillis();
            long elapsed = 0L;
            boolean var18 = false;

            label164: {
                label172: {
                    boolean isFail;
                    label173: {
                        label174: {
                            try {
                                var18 = true;
                                HttpResult r = (new HttpPost(this.buildUrl())).invoke();
                                resp = this.parseResponse(r.httpCode, r.rawData);
                                elapsed = System.currentTimeMillis() - t1;
                                invokeElapsed.put(i, elapsed);
                                if (r.httpCode == 200) {
                                    cause = null;
                                    var18 = false;
                                    break label164;
                                }

                                log.error("server response: " + r.httpCode + ", " + r.rawData);
                                var18 = false;
                                break label173;
                            } catch (SocketTimeoutException var19) {
                                cause = var19;
                                if (i == max - 1) {
                                    log.warn("Ack offset " + this.offset + " " + var19.getMessage() + ", " + max + " times.");
                                    var18 = false;
                                } else {
                                    var18 = false;
                                }
                            } catch (Throwable var20) {
                                cause = var20;
                                String msg = "Ack offset " + this.offset + " error, invoke@" + i + " ";
                                log.error(msg + var20.getMessage(), var20);
                                var18 = false;
                                break label174;
                            } finally {
                                if (var18) {
                                    isFail = cause != null;
                                    this.consumer.submitCommandEvent("ack", elapsed, isFail);
                                }
                            }

                            isFail = cause != null;
                            this.consumer.submitCommandEvent("ack", elapsed, isFail);
                            break label172;
                        }

                        isFail = cause != null;
                        this.consumer.submitCommandEvent("ack", elapsed, isFail);
                        break label172;
                    }

                    isFail = cause != null;
                    this.consumer.submitCommandEvent("ack", elapsed, isFail);
                }

                ++i;
                continue;
            }

            boolean isFail = cause != null;
            this.consumer.submitCommandEvent("ack", elapsed, isFail);
            break;
        }

        if (cause != null) {
            throw new CommunicationException((Throwable)cause);
        } else {
            resp.setInvokeTimes(times);
            resp.setInvokeElapsed(invokeElapsed);
            return resp;
        }
    }

    private String buildUrl() {
        try {
            return this.consumer.getBridgeServer() + "/kafka-bridge/ack?mode=" + this.consumer.getMode() + "&topic=" + URLEncoder.encode(this.consumer.getKafkaTopic(), "UTF-8") + "&partition=" + this.consumer.getKafkaPartition() + "&consumerId=" + URLEncoder.encode(this.consumer.getConsumerId(), "UTF-8") + "&consumerGroup=" + URLEncoder.encode(this.consumer.getConsumerGroup(), "UTF-8") + "&threadId=" + this.consumer.getCurrentThreadId() + "&offset=" + this.offset;
        } catch (UnsupportedEncodingException var2) {
            throw new RuntimeException(var2);
        }
    }

    private AckResponse parseResponse(int httpCode, String str) {
        if (httpCode != 200) {
            return httpCode == 400 ? new AckResponse(Status.REQUEST_PARAM_ILLEGAL, str) : new AckResponse(Status.SERVER_ERROR, str);
        } else if (str != null && !str.isEmpty()) {
            if (str.indexOf(61) == -1) {
                return new AckResponse(Status.SERVER_ERROR, str);
            } else {
                String[] statusKvPair = str.split("=");
                return new AckResponse(this.parseStatus(statusKvPair[1]));
            }
        } else {
            return new AckResponse(Status.SERVER_ERROR, "EmptyResponse");
        }
    }

    private Status parseStatus(String str) {
        if ("ack_ok".equalsIgnoreCase(str)) {
            return Status.OK;
        } else if ("recycled".equalsIgnoreCase(str)) {
            return Status.RECYCLED;
        } else if ("server_busy".equalsIgnoreCase(str)) {
            return Status.SERVER_BUSY;
        } else if ("already_acked".equalsIgnoreCase(str)) {
            return Status.ALREADY_ACKED;
        } else if ("not_found".equalsIgnoreCase(str)) {
            return Status.NOT_FOUND;
        } else if ("disallowed".equalsIgnoreCase(str)) {
            return Status.DISALLOWED;
        } else {
            throw new IllegalArgumentException("Unknown status: " + str);
        }
    }
}
