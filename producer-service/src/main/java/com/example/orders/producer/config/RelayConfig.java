package com.example.orders.producer.config;

import com.example.orders.producer.domain.OrderStore;
import com.example.orders.producer.messaging.OrderEventPublisher;
import com.example.orders.producer.messaging.OrderEventRelay;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.time.Duration;

/**
 * Creates the {@link OrderEventRelay} unless {@code app.relay.enabled=false}.
 *
 * <p>The default profile keeps it on: it is the production safety net for failed publishes.
 * {@code src/test/resources/application.yml} switches it off so tests observe a stable state.</p>
 */
@Configuration
public class RelayConfig {

    @Bean
    @ConditionalOnProperty(name = "app.relay.enabled", havingValue = "true", matchIfMissing = true)
    OrderEventRelay orderEventRelay(OrderStore orders,
                                    OrderEventPublisher publisher,
                                    Clock clock,
                                    @Value("${app.relay.batch-size:100}") int batchSize,
                                    @Value("${app.relay.min-age:5s}") Duration minAge) {
        return new OrderEventRelay(orders, publisher, clock, batchSize, minAge);
    }
}
