package libraryeventsproducer.integration.controller;

import com.course.libraryeventsproducer.LibraryEventsProducerApplication;
import com.course.libraryeventsproducer.domain.LibraryEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import libraryeventsproducer.util.TestUtil;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(classes = LibraryEventsProducerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"})
// This overrides any other property specified on files
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventsControllerIntegrationTest {

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    ObjectMapper objectMapper;

    private Consumer<Integer, String> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer())
                .createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    void postLibraryEvent() throws JsonProcessingException {
        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());
        HttpEntity<LibraryEvent> request = new HttpEntity<>(TestUtil.libraryEventRecord(), headers);

        ResponseEntity<LibraryEvent> responseEntity = restTemplate
                .exchange("/v1/libraryEvent", HttpMethod.POST, request, LibraryEvent.class);

        assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());

        ConsumerRecords<Integer, String> consumerRecords = KafkaTestUtils.getRecords(consumer);
        assert consumerRecords.count() == 1;

        consumerRecords.forEach(record -> {
            var libraryEventActual = TestUtil.parseLibraryEventRecord(objectMapper, record.value());
            assertEquals(libraryEventActual, TestUtil.libraryEventRecord());
        });
    }
}