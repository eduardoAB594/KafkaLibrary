package com.course.libraryeventsconsumer.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

//@EnableKafka Used for order versions of Kafka
@Slf4j
@Configuration
public class ConsumerConfig {

    public DefaultErrorHandler errorHandler() {
        // Override default configuration 6 times with no interval
        var fixedBackoff = new FixedBackOff(1000L, 3);

        // Exponential backoff can be used instead based on project requirements
        var exponentialBackoff = new ExponentialBackOffWithMaxRetries(2);
        exponentialBackoff.setInitialInterval(1000L);
        exponentialBackoff.setMultiplier(2.0);
        exponentialBackoff.setMaxInterval(2000L);

        var errorHandler = new DefaultErrorHandler(fixedBackoff);

        // Add to retry only on specific exceptions
        var exceptionsToIgnore = List.of(
            IllegalArgumentException.class
        );

        exceptionsToIgnore.forEach(
                errorHandler::addNotRetryableExceptions
        );

        // Add retry listeners
        errorHandler.setRetryListeners(
                (record, ex, deliveryAttempt) ->
                    log.warn("Retry listener :: delivery attempt: {} :: Exception: {}", deliveryAttempt, ex.getMessage())
        );

        return errorHandler;
    }

    @Bean // Override some Kafka default configurations
    public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory
    ) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);

        // Add to set MANUAL acknowledgement mode
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // Add to enable concurrent consumers, these are useful when the application is not running on a cloud environment
        factory.setConcurrency(3);

        // Add to override error handler
        factory.setCommonErrorHandler(errorHandler());

        return factory;
    }
}
