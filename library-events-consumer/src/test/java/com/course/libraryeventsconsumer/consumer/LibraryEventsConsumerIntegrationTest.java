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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EmbeddedKafka(
        topics = {
                "library-events",
                "library-events.RETRY",
                "library-events.DLT"
        }, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
        , "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}", "retryListener.startup=false"})
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

    private Consumer<Integer, String> consumer;

    @Value("${recovery.topics.retry}")
    private String retryTopic;

    @Value("${recovery.topics.dlt}")
    private String deadLetterTopic;

    @BeforeEach
    void setUp() {
        // Added to not wait for the retry topic to start up
        var container = endpointRegistry.getListenerContainers()
                .stream().filter(messageListenerContainer ->
                        Objects.equals(messageListenerContainer.getGroupId(), "library-events-listener-group"))
                .collect(Collectors.toList()).get(0);

        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());

        /*for (MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }*/
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

        verify(libraryEventsConsumerSpy, times(1)).onMessage(any(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(any(ConsumerRecord.class));
    }

    @Test
    void shouldSendToRetryTopicOnRetryException() throws ExecutionException, InterruptedException, JsonProcessingException {
        String json = " {\"libraryEventId\":0,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka course\",\"bookAuthor\":\"Eduardo\"}}";
        kafkaTemplate.sendDefault(json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(4)).onMessage(any(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(4)).processLibraryEvent(any(ConsumerRecord.class));

        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, retryTopic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, retryTopic);

        assertThat(json).isEqualTo(consumerRecord.value());
    }
}