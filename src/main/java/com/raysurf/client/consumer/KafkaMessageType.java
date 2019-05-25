package com.raysurf.client.consumer;

public enum KafkaMessageType {
    TEXT,
    BINARY;

    private KafkaMessageType() {

    }

    public static KafkaMessageType parse(String str) {
        if ("text".equalsIgnoreCase(str)) {
            return TEXT;
        } else if ("binary".equalsIgnoreCase(str)) {
            return BINARY;
        } else {
            throw new IllegalArgumentException("Unknown message type: " + str);
        }
    }

}
