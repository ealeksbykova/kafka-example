package com.example.orders.consumer.messaging;

/** Raised when a consumed record does not satisfy the expected contract. */
public class InvalidRecordException extends RuntimeException {

    public InvalidRecordException(String message) {
        super(message);
    }
}
