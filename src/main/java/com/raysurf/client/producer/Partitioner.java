package com.raysurf.client.producer;

public interface Partitioner {
    String key(Object item);
}
