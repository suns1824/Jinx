package com.raysurf.client.consumer.Exception;

public class CommunicationException extends RuntimeException {
    private static final long serialVersionUID = 2571577496775821845L;

    public CommunicationException(String message) {
        super(message);
    }

    public CommunicationException(Throwable cause) {
        super(cause);
    }

    public CommunicationException(String message, Throwable cause) {
        super(message, cause);
    }
}
