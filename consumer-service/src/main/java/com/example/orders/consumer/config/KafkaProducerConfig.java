package com.example.orders.consumer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Generic JSON producer used exclusively by the dead-letter recoverer.
 *
 * <p>It is intentionally untyped: whatever could not be processed (including payloads that failed to
 * deserialize) must still be publishable to {@code orders.created.DLT}.</p>
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    KafkaTemplate<String, Object> dltKafkaTemplate(KafkaProperties properties,
                                                   ObjectMapper objectMapper,
                                                   SslBundles sslBundles) {
        Map<String, Object> config = new HashMap<>(properties.buildProducerProperties(sslBundles));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        JsonSerializer<Object> valueSerializer = new JsonSerializer<>(objectMapper);
        valueSerializer.setAddTypeInfo(false);

        var factory = new DefaultKafkaProducerFactory<String, Object>(config, new StringSerializer(), valueSerializer);
        return new KafkaTemplate<>(factory);
    }
}
