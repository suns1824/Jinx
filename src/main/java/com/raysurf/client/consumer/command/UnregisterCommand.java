package com.raysurf.client.consumer.command;

import com.raysurf.client.consumer.AbstractHttpConsumer;
import com.raysurf.client.consumer.Exception.CommunicationException;
import com.raysurf.client.consumer.response.UnregisterResponse;
import com.raysurf.client.consumer.response.UnregisterResponse.Status;
import com.raysurf.client.util.HttpPost;
import com.raysurf.client.util.HttpResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public class UnregisterCommand {
    private static final Log log = LogFactory.getLog(UnregisterCommand.class);
    private final AbstractHttpConsumer consumer;

    public UnregisterCommand(AbstractHttpConsumer consumer) {
        this.consumer = consumer;
    }

    public UnregisterResponse execute() {
        Throwable cause = null;
        UnregisterResponse resp = null;
        int i = 0;

        while(true) {
            if (i < this.consumer.getMaxRetries()) {
                long t1 = System.currentTimeMillis();
                long elapsed = 0L;
                boolean var15 = false;

                UnregisterResponse var9;
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

                                log.error("server response code: " + r.httpCode + ", " + r.rawData);
                                var15 = false;
                            } catch (Throwable var16) {
                                cause = var16;
                                var15 = false;
                                break label139;
                            } finally {
                                if (var15) {
                                    isFail = cause != null;
                                    this.consumer.submitCommandEvent("unregister", elapsed, isFail);
                                }
                            }

                            isFail = cause != null;
                            this.consumer.submitCommandEvent("unregister", elapsed, isFail);
                            break label138;
                        }

                        isFail = cause != null;
                        this.consumer.submitCommandEvent("unregister", elapsed, isFail);
                    }

                    ++i;
                    continue;
                }

                boolean isFail = cause != null;
                this.consumer.submitCommandEvent("unregister", elapsed, isFail);
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
            return this.consumer.getBridgeServer() + "/kafka-bridge/session/unregister?mode=" + this.consumer.getMode() + "&topic=" + URLEncoder.encode(this.consumer.getKafkaTopic(), "UTF-8") + "&partition=" + this.consumer.getKafkaPartition() + "&consumerId=" + URLEncoder.encode(this.consumer.getConsumerId(), "UTF-8") + "&consumerGroup=" + URLEncoder.encode(this.consumer.getConsumerGroup(), "UTF-8");
        } catch (UnsupportedEncodingException var2) {
            throw new RuntimeException(var2);
        }
    }

    private UnregisterResponse parseResponse(int httpCode, String str) {
        if (httpCode != 200) {
            return httpCode == 400 ? new UnregisterResponse(Status.REQUEST_PARAM_ILLEGAL, str) : new UnregisterResponse(Status.SERVER_ERROR, str);
        } else if (str != null && str.length() != 0) {
            if (str.indexOf(61) == -1) {
                return new UnregisterResponse(Status.SERVER_ERROR, str);
            } else {
                String[] statusKvPair = str.split("=");
                return new UnregisterResponse(this.parseStatus(statusKvPair[1]));
            }
        } else {
            return new UnregisterResponse(Status.SERVER_ERROR, "EmptyResponse");
        }
    }

    private Status parseStatus(String str) {
        if ("unregister_ok".equalsIgnoreCase(str)) {
            return Status.OK;
        } else if ("server_busy".equalsIgnoreCase(str)) {
            return Status.SERVER_BUSY;
        } else {
            throw new IllegalArgumentException("Unknown status: " + str);
        }
    }
}

