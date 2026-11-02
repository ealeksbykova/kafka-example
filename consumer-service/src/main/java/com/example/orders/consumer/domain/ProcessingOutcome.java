package com.example.orders.consumer.domain;

/** Possible results of handing one Kafka record to the processor. */
public enum ProcessingOutcome {

    /** The order was not known before and has been stored. */
    CREATED,

    /** The order was already stored; the record was a redelivery and has been ignored. */
    DUPLICATE_IGNORED,

    /** The record could not be processed and has been parked in the dead-letter table. */
    FAILED
}
