package com.course.libraryeventsconsumer.model;

import com.course.libraryeventsconsumer.entity.LibraryEventType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

public record LibraryEvent(
        Integer libraryEventId,
        LibraryEventType libraryEventType,
        @NotNull
        @Valid
        Book book
) {
}
