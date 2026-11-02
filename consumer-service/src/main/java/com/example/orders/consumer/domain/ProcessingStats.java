package com.example.orders.consumer.domain;

import java.time.Instant;
import java.util.Map;

/** Counters per processing outcome, exposed through the consumer API. */
public interface ProcessingStats {

    void increment(ProcessingOutcome outcome, Instant now);

    Map<String, Long> counts();
}
