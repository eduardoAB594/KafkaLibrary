package com.course.libraryeventsproducer.controller;

import com.course.libraryeventsproducer.domain.LibraryEvent;
import com.course.libraryeventsproducer.domain.LibraryEventType;
import com.course.libraryeventsproducer.producer.LibraryEventsProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RequiredArgsConstructor
@Slf4j
@Controller
public class LibraryEventsController {

    private final LibraryEventsProducer libraryEventsProducer;

    @PostMapping("/v1/libraryEvent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(
            @RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {

        log.info("Received library event :: {}", libraryEvent);

        // Invoke kafka message producer
        libraryEventsProducer.sendLibraryEventAsync(libraryEvent);

        return ResponseEntity.status(HttpStatus.CREATED)
                .body(libraryEvent);
    }

    @PutMapping("/v1/libraryEvent")
    public ResponseEntity<?> updateLibraryEvent(
            @RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {

        log.info("Received library event :: {}", libraryEvent);

        // Event validations
        if (Objects.isNull(libraryEvent.libraryEventId()))
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please provide an event ID,");

        if (!LibraryEventType.UPDATE.equals(libraryEvent.libraryEventType()))
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only UPDATE event type is supported on this endpoint.");

        // Invoke kafka message producer
        libraryEventsProducer.sendLibraryEventAsync(libraryEvent);

        return ResponseEntity.status(HttpStatus.OK)
                .body(libraryEvent);
    }
}
