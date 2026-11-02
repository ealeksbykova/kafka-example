package com.example.orders.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point of the producer service.
 *
 * <p>Responsibilities: expose the order write API, persist orders in PostgreSQL and publish an
 * {@code OrderCreatedEvent} for every accepted order.</p>
 */
@SpringBootApplication
public class ProducerServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProducerServiceApplication.class, args);
    }
}
