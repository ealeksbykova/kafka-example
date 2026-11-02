package com.example.orders.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point of the consumer service.
 *
 * <p>Responsibilities: consume {@code OrderCreatedEvent}s from Kafka, store them idempotently in
 * PostgreSQL and expose the resulting read model.</p>
 */
@SpringBootApplication
public class ConsumerServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConsumerServiceApplication.class, args);
    }
}
