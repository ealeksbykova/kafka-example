package com.example.orders.producer.messaging;

/** Thrown when an event could not be acknowledged by the Kafka broker. */
public class EventPublishException extends RuntimeException {

    public EventPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
