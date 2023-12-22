package com.course.libraryeventsconsumer.config;

import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

//@EnableKafka Used for order versions of Kafka
@Configuration
public class ConsumerConfig {

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

        return factory;
    }
}
