package com.raysurf.client.producer;

import com.raysurf.client.util.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

public class DefaultProducer implements Producer {
    private static final Log log = LogFactory.getLog(Producer.class);
    public static final String UTF_8 = "UTF-8";
    final URL url;
    final String clientId;
    final Encoder encoder;
    final int timoutMillis;
    final Partitioner partioner;
    final Identifier identifier;

    public DefaultProducer(URL url, String clientId, Encoder encoder, int timoutMillis, Partitioner partioner, Identifier identifier) {
        this.url = url;
        this.clientId = clientId;
        this.encoder = encoder;
        this.timoutMillis = timoutMillis;
        this.partioner = partioner;
        this.identifier = identifier;
    }

    public void produce(String topicName, Object msg) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) this.url.openConnection();
        OutputStream os = null;
        InputStream is = null;
        try {
            conn.setDoInput(true);
            conn.setRequestMethod("POST");
            conn.setReadTimeout(this.timoutMillis);
            conn.setRequestProperty("Client-Id", this.clientId);
            conn.setRequestProperty("Correlated-Id", this.identifier.apply(msg));
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8");
            os = conn.getOutputStream();
            if (msg instanceof KeyedMessage) {
                KeyedMessage km = (KeyedMessage) msg;
                this.write(os, topicName, km.getKey(), km.getMessage());
            } else {
                this.write(os, topicName, msg);
            }

            int code = conn.getResponseCode();
            is = code == 200 ? conn.getInputStream() : conn.getErrorStream();
            String ret;
            if (code != 200) {
                ret = this.parseErrMsg(is);
                log.error(ret);
                throw new IOException(code + ": " + ret);
            }
            ret = this.parse200(is);
            if (! "ok".equalsIgnoreCase(ret)) {
                throw new IOException(ret);
            }
        } finally {
            if (os != null) {
                os.close();
            }
            if (is != null) {
                is.close();
            }
        }
    }

    private void write(OutputStream out, String topic, String key, Object msg) throws IOException {
        String body = "topic=" + topic + "&message_key=" + URLEncoder.encode(key, "UTF-8") + "&message=" + URLEncoder.encode(this.encoder.encode(msg), "UTF-8");
        out.write(body.getBytes("UTF-8"));
    }

    private void write(OutputStream out, String topic, Object msg) throws IOException {
        String body = "topic=" + topic + "&message_key=" + this.partioner.key(msg) + "&message=" + URLEncoder.encode(this.encoder.encode(msg), "UTF-8");
        out.write(body.getBytes("UTF-8"));
    }

    private String parse200(InputStream is) {
        try {
            return IOUtils.parseString(is, "UTF-8");
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    private String parseErrMsg(InputStream is) {
        if (is == null) {
            return null;
        } else {
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            StringBuilder buf = new StringBuilder();
            String str = null;
            try {
                while ((str = br.readLine()) != null) {
                    buf.append(str);
                }
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
            return buf.toString();
        }
    }

}
