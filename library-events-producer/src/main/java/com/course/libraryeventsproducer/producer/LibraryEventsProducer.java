package com.course.libraryeventsproducer.producer;

import com.course.libraryeventsproducer.domain.LibraryEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
@RequiredArgsConstructor
@Slf4j
public class LibraryEventsProducer {

    @Value("${spring.kafka.topic}")
    private String topicName;

    private final KafkaTemplate<Integer, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public void sendLibraryEvent(LibraryEvent event) throws JsonProcessingException {
        var key = event.libraryEventId();
        var value = objectMapper.writeValueAsString(event);

        var completableFuture = kafkaTemplate.send(topicName, key, value);

        kafkaTemplate.send(topicName, key, value)
                .whenComplete((sendResult, ex) -> {
                    // Check if send was successful
                    if (!Objects.isNull(ex)) {
                        handleFailure(key, value, ex);
                    } else {
                        handleSuccess(key, value, sendResult);
                    }
        });
    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Exception occurred while attempting to send message to Kafka topic :: ", ex);
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
        // SendResult objects contain message metadata and other information
        log.info("Message sent successfully for key {} and value {} :: Partition : {} - Offset - {}",
                key, value, sendResult.getRecordMetadata().partition(), sendResult.getRecordMetadata().offset());
    }
}
