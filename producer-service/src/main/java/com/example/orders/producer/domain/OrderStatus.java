package com.example.orders.producer.domain;

/** Lifecycle of an order as seen by the producer (write side). */
public enum OrderStatus {

    /** Persisted, the event has been acknowledged by the broker. */
    PUBLISHED,

    /** Persisted, but the broker did not acknowledge the event. Retried by the relay job. */
    PUBLISH_FAILED
}
