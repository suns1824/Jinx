package com.raysurf.client.consumer.command;

import com.raysurf.client.consumer.AbstractHttpConsumer;
import com.raysurf.client.consumer.Exception.CommunicationException;
import com.raysurf.client.consumer.response.SyncResponse;
import com.raysurf.client.consumer.response.SyncResponse.Status;
import com.raysurf.client.util.HttpPost;
import com.raysurf.client.util.HttpResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public class SyncCommand {
    private static final Log log = LogFactory.getLog(SyncCommand.class);
    private final AbstractHttpConsumer consumer;
    private long offset;

    public SyncCommand(AbstractHttpConsumer consumer, long offset) {
        this.consumer = consumer;
        this.offset = offset;
    }

    public SyncResponse execute() {
        Throwable cause = null;
        SyncResponse resp = null;
        int i = 0;

        while(true) {
            if (i < this.consumer.getMaxRetries()) {
                long t1 = System.currentTimeMillis();
                long elapsed = 0L;
                boolean var15 = false;

                SyncResponse var9;
                label129: {
                    label138: {
                        boolean isFail;
                        label139: {
                            try {
                                var15 = true;
                                HttpResult r = (new HttpPost(this.buildUrl())).invoke();
                                resp = this.parseResponse(r.httpCode, r.rawData);
                                elapsed = System.currentTimeMillis() - t1;
                                if (r.httpCode == 200) {
                                    cause = null;
                                    var9 = resp;
                                    var15 = false;
                                    break label129;
                                }

                                log.error("server response: " + r.httpCode + ", " + r.rawData);
                                var15 = false;
                            } catch (Throwable var16) {
                                cause = var16;
                                var15 = false;
                                break label139;
                            } finally {
                                if (var15) {
                                    isFail = cause != null;
                                    this.consumer.submitCommandEvent("sync", elapsed, isFail);
                                }
                            }

                            isFail = cause != null;
                            this.consumer.submitCommandEvent("sync", elapsed, isFail);
                            break label138;
                        }

                        isFail = cause != null;
                        this.consumer.submitCommandEvent("sync", elapsed, isFail);
                    }

                    ++i;
                    continue;
                }

                boolean isFail = cause != null;
                this.consumer.submitCommandEvent("sync", elapsed, isFail);
                return var9;
            }

            if (cause != null) {
                throw new CommunicationException(cause);
            }

            return resp;
        }
    }

    private String buildUrl() {
        try {
            return this.consumer.getBridgeServer() + "/kafka-bridge/sync?mode=" + this.consumer.getMode() + "&topic=" + URLEncoder.encode(this.consumer.getKafkaTopic(), "UTF-8") + "&partition=" + this.consumer.getKafkaPartition() + "&consumerId=" + URLEncoder.encode(this.consumer.getConsumerId(), "UTF-8") + "&consumerGroup=" + URLEncoder.encode(this.consumer.getConsumerGroup(), "UTF-8") + "&offset=" + this.offset;
        } catch (UnsupportedEncodingException var2) {
            throw new RuntimeException(var2);
        }
    }

    private SyncResponse parseResponse(int httpCode, String str) {
        if (httpCode != 200) {
            return httpCode == 400 ? new SyncResponse(Status.REQUEST_PARAM_ILLEGAL, str) : new SyncResponse(Status.SERVER_ERROR, str);
        } else if (str != null && !str.isEmpty()) {
            if (str.indexOf(61) == -1) {
                return new SyncResponse(Status.SERVER_ERROR, str);
            } else {
                String[] statusKvPair = str.split("=");
                return new SyncResponse(this.parseStatus(statusKvPair[1]));
            }
        } else {
            return new SyncResponse(Status.SERVER_ERROR, "EmptyResponse");
        }
    }

    private Status parseStatus(String str) {
        if ("sync_ok".equalsIgnoreCase(str)) {
            return Status.OK;
        } else if ("already_exists".equalsIgnoreCase(str)) {
            return Status.ALREADY_EXISTS;
        } else {
            throw new IllegalArgumentException("Unknown status: " + str);
        }
    }
}

