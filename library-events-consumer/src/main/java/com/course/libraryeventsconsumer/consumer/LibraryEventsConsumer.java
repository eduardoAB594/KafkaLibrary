package com.course.libraryeventsconsumer.consumer;

import com.course.libraryeventsconsumer.service.LibraryEventService;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class LibraryEventsConsumer {

    private final LibraryEventService libraryEventService;

    @KafkaListener(topics = {"library-events"})
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("Received new ConsumerRecord :: {}", consumerRecord);

        libraryEventService.processLibraryEvent(consumerRecord);
    }

}
