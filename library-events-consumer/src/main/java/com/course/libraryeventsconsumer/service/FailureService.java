package com.course.libraryeventsconsumer.service;

import com.course.libraryeventsconsumer.entity.BookEntity;
import com.course.libraryeventsconsumer.entity.FailureRecordEntity;
import com.course.libraryeventsconsumer.entity.LibraryEventEntity;
import com.course.libraryeventsconsumer.model.Book;
import com.course.libraryeventsconsumer.model.LibraryEvent;
import com.course.libraryeventsconsumer.repository.FailureRecordRepository;
import com.course.libraryeventsconsumer.repository.LibraryEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.Authenticator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Slf4j
@RequiredArgsConstructor
@Service
public class FailureService {

    private final FailureRecordRepository failureRecordRepository;

    public void saveFailedRecord(ConsumerRecord<Integer, String> record, Exception exception, String recordStatus) {
        var f = FailureRecordEntity.builder()
                .bookId(null)
                .topic(record.topic())
                .key_value(record.key())
                .errorRecord(record.value())
                .partition(record.partition())
                .offset_value(record.offset())
                .exception(exception.getCause().getMessage())
                .status(recordStatus)
                .build();

        var failureRecord = new FailureRecordEntity(null,record.topic(), record.key(),  record.value(), record.partition(),record.offset(),
                exception.getCause().getMessage(),
                recordStatus);

        failureRecordRepository.save(failureRecord);
    }

}
