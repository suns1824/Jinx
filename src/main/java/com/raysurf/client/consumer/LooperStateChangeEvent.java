package com.raysurf.client.consumer;

import java.util.EventObject;

public class LooperStateChangeEvent extends EventObject {
    private static final long serialVersionUID = 7414680611657299651L;
    private final LooperState oldState;
    private final LooperState newState;

    public LooperStateChangeEvent(Looper source, LooperState oldState, LooperState newState) {
        super(source);
        this.oldState = oldState;
        this.newState = newState;
    }

    public LooperState getOldState() {
        return this.oldState;
    }

    public LooperState getNewState() {
        return this.newState;
    }
}
