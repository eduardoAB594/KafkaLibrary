package com.course.libraryeventsproducer.controller;

import com.course.libraryeventsproducer.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@Controller
@Slf4j
public class LibraryEventsController {

    @PostMapping("/v1/libraryEvent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(
            @RequestBody LibraryEvent libraryEvent) {

        log.info("Received library event :: {}", libraryEvent);

        // Invoke producer

        return ResponseEntity.status(HttpStatus.CREATED)
                .body(libraryEvent);
    }
}
