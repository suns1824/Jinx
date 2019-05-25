package com.raysurf.client.consumer.command;

import com.raysurf.client.consumer.AbstractHttpConsumer;
import com.raysurf.client.consumer.Exception.CommunicationException;
import com.raysurf.client.consumer.KafkaMessage;
import com.raysurf.client.consumer.KafkaMessageType;
import com.raysurf.client.consumer.response.FetchResponse;
import com.raysurf.client.consumer.response.FetchResponse.Status;
import com.raysurf.client.util.MessageDecoder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

public class FetchCommand {
    private static final Log log = LogFactory.getLog(FetchCommand.class);
    private final AbstractHttpConsumer consumer;

    public FetchCommand(AbstractHttpConsumer consumer) {
        this.consumer = consumer;
    }

    public FetchResponse execute() {
        FetchResponse resp = null;
        Throwable cause = null;
        int max = this.consumer.getMaxRetries();
        int times = 0;
        Map<Integer, Long> invokeElapsed = new HashMap(max, 1.0F);

        for(int i = 0; i < max; ++i) {
            ++times;
            InputStream is = null;
            long t1 = System.currentTimeMillis();
            long elapsed = 0L;
            boolean var21 = false;

            boolean isFail;
            label184: {
                label185: {
                    label186: {
                        try {
                            var21 = true;
                            URL url = new URL(this.buildUrl());
                            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
                            conn.setConnectTimeout(this.consumer.getConnTimeout());
                            conn.setReadTimeout(this.consumer.getReadTimeout());
                            conn.setRequestProperty("User-Agent", this.consumer.getUserAgent());
                            int httpCode = conn.getResponseCode();
                            is = httpCode == 200 ? conn.getInputStream() : conn.getErrorStream();
                            resp = this.parseResponse(httpCode, is);
                            elapsed = System.currentTimeMillis() - t1;
                            invokeElapsed.put(i, elapsed);
                            if (httpCode != 200) {
                                var21 = false;
                                break label184;
                            }

                            cause = null;
                            var21 = false;
                            break label186;
                        } catch (SocketTimeoutException var22) {
                            StringBuilder sb = new StringBuilder();
                            sb.append(var22.getMessage()).append(" @fetch-invoke-").append(i);
                            if (i < max - 1) {
                                sb.append(", retrying.");
                            } else {
                                sb.append(", give up.");
                            }

                            log.warn(sb.toString());
                            cause = var22;
                            var21 = false;
                            break label185;
                        } catch (Exception var23) {
                            log.error(var23.getMessage(), var23);
                            cause = var23;
                            var21 = false;
                        } finally {
                            if (var21) {
                                this.closeStreams(is);
                                isFail = cause != null;
                                this.consumer.submitCommandEvent("fetch", elapsed, isFail);
                            }
                        }

                        this.closeStreams(is);
                        isFail = cause != null;
                        this.consumer.submitCommandEvent("fetch", elapsed, isFail);
                        continue;
                    }

                    this.closeStreams(is);
                    isFail = cause != null;
                    this.consumer.submitCommandEvent("fetch", elapsed, isFail);
                    break;
                }

                this.closeStreams(is);
                isFail = cause != null;
                this.consumer.submitCommandEvent("fetch", elapsed, isFail);
                continue;
            }

            this.closeStreams(is);
            isFail = cause != null;
            this.consumer.submitCommandEvent("fetch", elapsed, isFail);
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
            return this.consumer.getBridgeServer() + "/kafka-bridge/fetch?mode=" + this.consumer.getMode() + "&topic=" + URLEncoder.encode(this.consumer.getKafkaTopic(), "UTF-8") + "&partition=" + this.consumer.getKafkaPartition() + "&consumerId=" + URLEncoder.encode(this.consumer.getConsumerId(), "UTF-8") + "&consumerGroup=" + URLEncoder.encode(this.consumer.getConsumerGroup(), "UTF-8") + "&threadId=" + this.consumer.getCurrentThreadId();
        } catch (UnsupportedEncodingException var2) {
            throw new RuntimeException(var2);
        }
    }

    private String parseErrMsg(InputStream is) {
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        StringBuilder buf = new StringBuilder();
        String str = null;

        try {
            while((str = br.readLine()) != null) {
                buf.append("\n").append(str);
            }
        } catch (IOException var6) {
            ;
        }

        return buf.toString();
    }

    private FetchResponse parseResponse(int httpCode, InputStream is) throws IOException {
        if (httpCode != 200) {
            String errMsg = this.parseErrMsg(is);
            return httpCode == 400 ? new FetchResponse(Status.REQUEST_PARAM_ILLEGAL, errMsg) : new FetchResponse(Status.SERVER_ERROR, errMsg);
        } else {
            return this.parse200(is);
        }
    }

    private FetchResponse parse200(InputStream is) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        Status status = null;
        long offset = 0L;
        String msgKey = null;
        KafkaMessageType msgType = null;
        StringBuilder sb = new StringBuilder();
        int line = 1;
        String str = null;

        StringBuilder debugBuf;
        for(debugBuf = null; (str = br.readLine()) != null; ++line) {
            if (log.isDebugEnabled()) {
                if (debugBuf == null) {
                    debugBuf = new StringBuilder();
                }

                debugBuf.append(System.lineSeparator()).append(str);
            }

            String[] sa;
            if (line == 1) {
                sa = str.split("=");
                status = this.parseStatus(sa[1]);
            }

            if (status != Status.OK) {
                break;
            }

            if (line == 3) {
                sa = str.split("=");
                offset = Long.parseLong(sa[1]);
            } else if (line == 4) {
                sa = str.split("=");
                String key = sa[1];
                if (key != null && !key.equals("null")) {
                    msgKey = sa[1];
                }
            } else if (line == 5) {
                sa = str.split("=");
                msgType = KafkaMessageType.parse(sa[1]);
            }

            if (line > 6) {
                if (line == 7) {
                    sb.append(str);
                } else {
                    sb.append(System.lineSeparator()).append(str);
                }
            }
        }

        if (log.isDebugEnabled() && debugBuf != null) {
            log.debug(debugBuf);
        }

        if (status != Status.OK) {
            if (status == null) {
                log.error("server response empty!");
                status = Status.DEFER;
            }

            return new FetchResponse(status);
        } else {
            byte[] msgData = this.parseMsgData(msgType, sb);
            KafkaMessage kafkaMessage = new KafkaMessage(msgType, offset, msgKey, msgData);
            return new FetchResponse(kafkaMessage);
        }
    }

    private byte[] parseMsgData(KafkaMessageType msgType, StringBuilder buf) throws UnsupportedEncodingException {
        byte[] msgData = null;
        if (msgType == KafkaMessageType.TEXT) {
            msgData = buf.toString().getBytes("UTF-8");
        } else {
            msgData = MessageDecoder.decode(buf.toString());
        }

        return msgData;
    }

    private Status parseStatus(String str) {
        if ("fetch_ok".equalsIgnoreCase(str)) {
            return Status.OK;
        } else if ("fetch_defer".equalsIgnoreCase(str)) {
            return Status.DEFER;
        } else if ("lock_conflict".equalsIgnoreCase(str)) {
            return Status.LOCK_CONFLICT;
        } else {
            throw new IllegalArgumentException("Unknown status: " + str);
        }
    }

    private void closeStreams(Closeable... streams) {
        Closeable[] var2 = streams;
        int var3 = streams.length;

        for(int var4 = 0; var4 < var3; ++var4) {
            Closeable s = var2[var4];
            if (s != null) {
                try {
                    s.close();
                } catch (IOException var7) {
                    ;
                }
            }
        }

    }
}

