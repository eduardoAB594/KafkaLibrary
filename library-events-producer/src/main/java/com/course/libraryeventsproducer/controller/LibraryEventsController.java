package com.course.libraryeventsproducer.controller;

import com.course.libraryeventsproducer.domain.LibraryEvent;
import com.course.libraryeventsproducer.producer.LibraryEventsProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RequiredArgsConstructor
@Slf4j
@Controller
public class LibraryEventsController {

    private final LibraryEventsProducer libraryEventsProducer;

    @PostMapping("/v1/libraryEvent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(
            @RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {

        log.info("Received library event :: {}", libraryEvent);

        // Invoke kafka message producer
        libraryEventsProducer.sendLibraryEventAsync(libraryEvent);

        return ResponseEntity.status(HttpStatus.CREATED)
                .body(libraryEvent);
    }
}
