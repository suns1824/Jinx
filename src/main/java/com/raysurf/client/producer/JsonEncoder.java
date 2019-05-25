package com.raysurf.client.producer;

import com.alibaba.fastjson.JSON;

import java.io.IOException;

public class JsonEncoder implements Encoder {

    public JsonEncoder() {

    }

    public String encode(Object msg) throws IOException {
        return JSON.toJSONString(msg);
    }
}
