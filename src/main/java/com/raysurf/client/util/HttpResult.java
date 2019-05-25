package com.raysurf.client.util;

import java.io.Serializable;

public class HttpResult implements Serializable {
    private static final long serialVersionUID = -2483429470635614707L;
    public final int httpCode;
    public final String rawData;

    public HttpResult(int httpCode, String rawData) {
        this.httpCode = httpCode;
        this.rawData = rawData;
    }

    public String toString() {
        return "HttpResult [httpCode=" + this.httpCode + ", rawData=" + this.rawData + "]";
    }
}

