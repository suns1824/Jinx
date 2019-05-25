package com.raysurf.client.consumer;

public enum ConsumerState {
    INITIALIZING,
    INITIALIZED,
    STARTING,
    STARTED,
    STOPPING,
    STOPPED,
    SUSPENDED,
    ABORTED,
    EXITING,
    EXITED;

    private ConsumerState() {
    }
}
