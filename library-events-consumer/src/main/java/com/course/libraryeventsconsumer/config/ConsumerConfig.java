package com.course.libraryeventsconsumer.config;

import com.course.libraryeventsconsumer.service.FailureService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

//@EnableKafka Used for order versions of Kafka
@Slf4j
@Configuration
public class ConsumerConfig {

    public static final String RETRY = "RETRY";
    public static final String DEAD_LETTER = "DEAD_LETTER";
    public static final String SENT = "SENT";

    @Autowired
    private FailureService failureService;

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Value("${recovery.topics.retry:library-events.RETRY}")
    private String retryTopic;

    @Value("${recovery.topics.dlt:library-events.DLT}")
    private String deadLetterTopic;

    // Add to recover or deadletter messages
    public DeadLetterPublishingRecoverer publishingRecoverer() {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate
                , (r, e) -> {
            log.error("Exception in publishingRecoverer : {} ", e.getMessage(), e);
            if (e.getCause() instanceof RecoverableDataAccessException) {
                return new TopicPartition(retryTopic, r.partition());
            } else {
                return new TopicPartition(deadLetterTopic, r.partition());
            }
        });

        return recoverer;
    }

    ConsumerRecordRecoverer consumerRecordRecoverer = (record, exception) -> {
        log.error("Exception in consumerRecordRecoverer : {} ", exception.getMessage(), exception);
        var consumerRecord = (ConsumerRecord<Integer, String>) record;

        if (exception.getCause() instanceof RecoverableDataAccessException) {
            // Recovery logic
            log.warn("Saving message to retry later :: {}", record);
            failureService.saveFailedRecord(consumerRecord, exception, RETRY);
        } else {
            // Non recovery logic
            log.warn("Dead lettering received message :: {}", record);
            failureService.saveFailedRecord(consumerRecord, exception, DEAD_LETTER);
        }
    };


    public DefaultErrorHandler errorHandler() {
        // Override default configuration 6 times with no interval
        var fixedBackoff = new FixedBackOff(1000L, 3);

        // Exponential backoff can be used instead based on project requirements
        var exponentialBackoff = new ExponentialBackOffWithMaxRetries(2);
        exponentialBackoff.setInitialInterval(1000L);
        exponentialBackoff.setMultiplier(2.0);
        exponentialBackoff.setMaxInterval(2000L);

        var errorHandler = new DefaultErrorHandler(
                //publishingRecoverer(),
                consumerRecordRecoverer,
                fixedBackoff
        );

        // Add to retry only on specific exceptions
        var NonRetryableExceptions = List.of(
            IllegalArgumentException.class
        );

        NonRetryableExceptions.forEach(
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
