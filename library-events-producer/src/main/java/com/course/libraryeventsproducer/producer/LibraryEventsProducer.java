package com.course.libraryeventsproducer.producer;

import com.course.libraryeventsproducer.domain.LibraryEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RequiredArgsConstructor
@Slf4j
@Component
public class LibraryEventsProducer {

    @Value("${spring.kafka.topic}")
    private String topicName;

    private final KafkaTemplate<Integer, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    // Approach 1
    public void sendLibraryEventAsync(LibraryEvent event) throws JsonProcessingException {
        var key = event.libraryEventId();
        var value = objectMapper.writeValueAsString(event);

        // blocking call to get kafka cluster metadata
        // completableFuture finishes in the background and doesn't block the current program execution
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

    // Approach 2
    public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent event)
            throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        var key = event.libraryEventId();
        var value = objectMapper.writeValueAsString(event);

        // blocking call to get kafka cluster metadata
        // blocks and waits until the message is sent to kafka
        var sendResult = kafkaTemplate.send(topicName, key, value).get(2, TimeUnit.SECONDS);

        handleSuccess(key, value, sendResult);

        return sendResult;
    }

    // Approach 3
    public SendResult<Integer, String> sendLibraryEventProducerRecord(LibraryEvent event)
            throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        var key = event.libraryEventId();
        var value = objectMapper.writeValueAsString(event);

        // blocking call to get kafka cluster metadata
        // blocks and waits until the message is sent to kafka
        var sendResult = kafkaTemplate
                .send(new ProducerRecord<>(topicName, key, value))
                .get(2, TimeUnit.SECONDS);

        handleSuccess(key, value, sendResult);

        return sendResult;
    }

    // With headers
    public SendResult<Integer, String> sendLibraryEventWithHeaders(LibraryEvent event)
            throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        var key = event.libraryEventId();
        var value = objectMapper.writeValueAsString(event);

        var producerRecord = buildProducerRecord(key, value);

        // blocking call to get kafka cluster metadata
        // blocks and waits until the message is sent to kafka
        var sendResult = kafkaTemplate
                .send(producerRecord)
                .get(2, TimeUnit.SECONDS);

        handleSuccess(key, value, sendResult);

        return sendResult;
    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {
        List<Header> headers = Collections.singletonList(
                new RecordHeader("event-source", "library-events-producer".getBytes()));

        // Set partition as null to let the partitioner assign partition automatically
        return new ProducerRecord<>(topicName, null, key, value, headers);
    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Exception occurred while attempting to send message to Kafka topic :: ", ex);
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
        // SendResult objects contain message metadata and other information
        log.info("Message sent successfully for key [{}] and value {} :: Partition : {} - Offset - {}",
                key, value, sendResult.getRecordMetadata().partition(), sendResult.getRecordMetadata().offset());
    }
}
