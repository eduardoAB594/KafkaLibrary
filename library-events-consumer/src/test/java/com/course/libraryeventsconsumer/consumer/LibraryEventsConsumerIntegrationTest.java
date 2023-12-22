package com.course.libraryeventsconsumer.consumer;

import com.course.libraryeventsconsumer.entity.BookEntity;
import com.course.libraryeventsconsumer.entity.LibraryEventEntity;

import com.course.libraryeventsconsumer.entity.LibraryEventType;
import com.course.libraryeventsconsumer.model.Book;
import com.course.libraryeventsconsumer.model.LibraryEvent;
import com.course.libraryeventsconsumer.repository.LibraryEventRepository;
import com.course.libraryeventsconsumer.service.LibraryEventService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
        , "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
class LibraryEventsConsumerIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    LibraryEventService libraryEventsServiceSpy;

    @Autowired
    LibraryEventRepository libraryEventsRepository;

    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        for (MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown() {
        libraryEventsRepository.deleteAll();
    }

    @Test
    void shouldPersistNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        String json = " {\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka course\",\"bookAuthor\":\"Eduardo\"}}";
        kafkaTemplate.sendDefault(json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(1)).onMessage(any(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(any(ConsumerRecord.class));

        List<LibraryEventEntity> libraryEventList = libraryEventsRepository.findAll();

        assertThat(libraryEventList).hasSize(1)
                .extracting(LibraryEventEntity::getBookEntity)
                .extracting(BookEntity::getBookId)
                .containsExactly(456);
    }

    @Test
    void shouldUpdateExistingLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka course\",\"bookAuthor\":\"Eduardo\"}}";
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);

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

        libraryEventsRepository.save(libraryEventEntity);

        Book updatedBook = new Book(456, "Kafka Course 2", "Eduardo Avila");
        LibraryEvent updatedLibraryEvent = new LibraryEvent(1, LibraryEventType.UPDATE, updatedBook);

        String updatedJson = objectMapper.writeValueAsString(updatedLibraryEvent);
        kafkaTemplate.sendDefault(updatedLibraryEvent.libraryEventId(), updatedJson).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(1)).onMessage(any(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(any(ConsumerRecord.class));

        var persistedLibraryEvent = libraryEventsRepository.findById(1).get();

        assertThat(persistedLibraryEvent).isNotNull()
                .extracting(LibraryEventEntity::getLibraryEventType)
                .isEqualTo(LibraryEventType.UPDATE);

        assertThat(persistedLibraryEvent).isNotNull()
                .extracting(LibraryEventEntity::getBookEntity)
                .extracting(BookEntity::getBookName)
                .isEqualTo("Kafka Course 2");
    }

    @Test
    void shouldThrowExceptionWhenUpdatingWithNullLibraryEventId() throws JsonProcessingException, ExecutionException, InterruptedException {
        Book updatedBook = new Book(456, "Kafka Course 2", "Eduardo Avila");
        LibraryEvent updatedLibraryEvent = new LibraryEvent(null, LibraryEventType.UPDATE, updatedBook);

        String updatedJson = objectMapper.writeValueAsString(updatedLibraryEvent);
        kafkaTemplate.sendDefault(updatedLibraryEvent.libraryEventId(), updatedJson).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(6)).onMessage(any(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(6)).processLibraryEvent(any(ConsumerRecord.class));
    }
}