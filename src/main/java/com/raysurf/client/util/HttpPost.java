package com.raysurf.client.util;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class HttpPost {
    private int connTimeout;
    private int readTimeout;
    private URL url;
    private byte[] postData;

    public HttpPost(String urlStr) throws MalformedURLException {
        this(new URL(urlStr), (byte[])null);
    }

    public HttpPost(URL url, byte[] postData) {
        this.connTimeout = 2000;
        this.readTimeout = 3000;
        this.url = url;
        this.postData = postData;
    }

    public HttpPost(URL url, byte[] postData, int connTimeout, int readTimeout) {
        this(url, postData);
        this.connTimeout = connTimeout;
        this.readTimeout = readTimeout;
    }

    public HttpResult invoke() throws IOException {
        BufferedOutputStream bo = null;
        BufferedInputStream bi = null;
        ByteArrayOutputStream responseBuf;

        int httpCode;
        try {
            HttpURLConnection conn = (HttpURLConnection)this.url.openConnection();
            conn.setConnectTimeout(this.connTimeout);
            conn.setReadTimeout(this.readTimeout);
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setRequestProperty("User-Agent", this.buildUserAgent());
            byte[] buf = new byte[256];
            int len;
            if (this.postData != null && this.postData.length != 0) {
                bo = new BufferedOutputStream(conn.getOutputStream());
                ByteArrayInputStream dataInput = new ByteArrayInputStream(this.postData);

                while((len = dataInput.read(buf)) != -1) {
                    bo.write(buf, 0, len);
                }

                bo.flush();
                dataInput.close();
            }

            httpCode = conn.getResponseCode();
            InputStream is = httpCode == 200 ? conn.getInputStream() : conn.getErrorStream();
            responseBuf = new ByteArrayOutputStream();
            bi = new BufferedInputStream(is);

            while((len = bi.read(buf)) != -1) {
                responseBuf.write(buf, 0, len);
            }
        } finally {
            this.closeStreams(bi, bo);
        }

        String rawData = new String(responseBuf.toByteArray(), "UTF-8");
        return new HttpResult(httpCode, rawData);
    }

    protected String buildUserAgent() {
        StringBuilder sb = new StringBuilder("kafka-http-client");
        String version = System.getProperty("kafka-http-client-version");
        if (version != null && version.length() != 0) {
            sb.append("-").append(version);
        }

        sb.append("/java").append(System.getProperty("java.version"));
        return sb.toString();
    }

    private void closeStreams(Closeable... streams) {
        Closeable[] tmp = streams;
        int len = streams.length;

        for(int i = 0; i < len; ++i) {
            Closeable s = tmp[i];
            if (s != null) {
                try {
                    s.close();
                } catch (IOException e) {
                    ;
                }
            }
        }

    }
}
