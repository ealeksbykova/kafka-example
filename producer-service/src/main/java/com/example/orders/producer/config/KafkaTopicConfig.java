package com.example.orders.producer.config;

import com.example.orders.contract.KafkaTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Topic provisioning and scheduling.
 *
 * <p>Creating topics from the application is convenient for a sample; in production the topics are
 * usually managed by platform tooling. The configuration is idempotent: if the topic already exists
 * with the same partition count nothing happens.</p>
 */
@Configuration
@EnableScheduling
public class KafkaTopicConfig {

    @Bean
    NewTopic orderCreatedTopic(@Value("${app.kafka.order-created-topic:" + KafkaTopics.ORDER_CREATED + "}") String topic,
                               @Value("${app.kafka.order-created-partitions:3}") int partitions,
                               @Value("${app.kafka.order-created-replicas:1}") int replicas) {
        return TopicBuilder.name(topic)
                .partitions(partitions)
                .replicas(replicas)
                .build();
    }
}
