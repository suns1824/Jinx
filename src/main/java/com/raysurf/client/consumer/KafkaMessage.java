package com.raysurf.client.consumer;

import java.io.Serializable;

public class KafkaMessage implements Serializable {
    private static final long serialVersionUID = -292264561612205832L;
    private KafkaMessageType msgType;
    private long offset;
    private String key;
    private byte[] payload;

    public KafkaMessage(KafkaMessageType msgType, long offset, String key, byte[] payload) {
        this.msgType = msgType;
        this.offset = offset;
        this.key = key;
        this.payload = payload;
    }

    public KafkaMessageType getMsgType() {
        return msgType;
    }

    public void setMsgType(KafkaMessageType msgType) {
        this.msgType = msgType;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public String toString() {
        return this.msgType == KafkaMessageType.TEXT ? "KafkaMessage [msgType=" + this.msgType + ", offset=" + this.offset + ", key=" + this.key + ", payload=" + new String(this.payload) + "]" : "KafkaMessage [msgType=" + this.msgType + ", offset=" + this.offset + ", key=" + this.key + ", msgByteSize=" + this.payload.length + "]";
    }
}
