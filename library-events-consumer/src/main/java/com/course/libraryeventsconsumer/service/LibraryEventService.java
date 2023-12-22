package com.course.libraryeventsconsumer.service;

import com.course.libraryeventsconsumer.entity.BookEntity;
import com.course.libraryeventsconsumer.entity.LibraryEventEntity;
import com.course.libraryeventsconsumer.model.Book;
import com.course.libraryeventsconsumer.model.LibraryEvent;
import com.course.libraryeventsconsumer.repository.LibraryEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Slf4j
@RequiredArgsConstructor
@Service
public class LibraryEventService {

    private final LibraryEventRepository libraryEventRepository;
    private final ObjectMapper objectMapper;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("Parsed LibraryEvent result :: {}", libraryEvent);

        switch (libraryEvent.libraryEventType()) {
            case NEW -> save(libraryEvent);
            case UPDATE -> update(libraryEvent);
            default -> {
                // do nothing
            }
        }

    }

    private void save(LibraryEvent libraryEvent) {
        Book book = libraryEvent.book();

        BookEntity bookEntity = BookEntity.builder()
                .bookId(book.bookId())
                .bookAuthor(book.bookAuthor())
                .bookName(book.bookName())
                .build();

        LibraryEventEntity libraryEventEntity = LibraryEventEntity.builder()
                .libraryEventType(libraryEvent.libraryEventType())
                .bookEntity(bookEntity)
                .build();

        libraryEventEntity.getBookEntity().setLibraryEventEntity(libraryEventEntity);

        libraryEventRepository.save(libraryEventEntity);

        log.info("Current library event was persisted successfully!");
    }

    private void update(LibraryEvent libraryEvent) {
        if (Objects.isNull(libraryEvent.libraryEventId()))
            throw new IllegalArgumentException("Library event ID is missing.");

        var libraryEventToUpdate = libraryEventRepository
                .findById(libraryEvent.libraryEventId())
                .orElseThrow(() ->  new IllegalArgumentException("Provided event ID is invalid"));

        libraryEventToUpdate.setLibraryEventType(libraryEvent.libraryEventType());

        Book book = libraryEvent.book();
        BookEntity bookToUpdate = libraryEventToUpdate.getBookEntity();

        bookToUpdate.setBookName(book.bookName());
        bookToUpdate.setBookAuthor(book.bookAuthor());
        bookToUpdate.setLibraryEventEntity(libraryEventToUpdate);

        libraryEventRepository.save(libraryEventToUpdate);

        log.info("Current library event was updated successfully!");
    }

}
