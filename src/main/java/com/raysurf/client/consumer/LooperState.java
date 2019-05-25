package com.raysurf.client.consumer;

public enum LooperState {
    WAITING,
    FETCHING,
    PROCESSING,
    ACKING,
    FINISHED,
    SUSPENDED,
    ABORTED;

    private LooperState() {
    }
}