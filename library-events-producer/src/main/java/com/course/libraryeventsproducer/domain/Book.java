package com.course.libraryeventsproducer.domain;

public record Book(
        Integer bookId,
        String bookName,
        String bookAuthor
) {
}
