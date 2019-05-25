package com.raysurf.client.consumer.Exception;

import com.raysurf.client.consumer.KafkaMessage;

public class ProcessingException extends RuntimeException {
    private static final long serialVersionUID = -5396663571334966402L;
    private KafkaMessage kafkaMessage;

    public ProcessingException(KafkaMessage kafkaMessage, Throwable cause) {
        super(cause);
        this.kafkaMessage = kafkaMessage;
    }

    public KafkaMessage getKafkaMessage() {
        return kafkaMessage;
    }
}
