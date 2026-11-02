package com.example.orders.consumer.support;

import com.example.orders.consumer.domain.ProcessingOutcome;
import com.example.orders.consumer.domain.ProcessingStats;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** In-memory fake of the {@link ProcessingStats} port. */
public class InMemoryProcessingStats implements ProcessingStats {

    private final Map<String, Long> counts = new LinkedHashMap<>();

    @Override
    public void increment(ProcessingOutcome outcome, Instant now) {
        counts.merge(outcome.name(), 1L, Long::sum);
    }

    @Override
    public Map<String, Long> counts() {
        return Map.copyOf(counts);
    }

    public long count(ProcessingOutcome outcome) {
        return counts.getOrDefault(outcome.name(), 0L);
    }
}
