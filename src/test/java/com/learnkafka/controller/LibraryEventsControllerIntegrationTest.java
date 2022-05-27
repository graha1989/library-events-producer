package com.learnkafka.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = { "library-events" }, partitions = 3)
@TestPropertySource(properties = { "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}" })
class LibraryEventsControllerIntegrationTest {

    @Autowired
    private TestRestTemplate testRestTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<Integer, String> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group-one", "true", embeddedKafkaBroker));

        consumer = new DefaultKafkaConsumerFactory<Integer, String>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    @Timeout(value = 5)
    void testPostAsynchronusLibraryEvent() {
        // given
        Book book = createBook();
        LibraryEvent libraryEvent = createLibraryEvent(book);

        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON_VALUE);
        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, headers);
        
        String expectedValue = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Aleksandar Grahovac\"}}";
        
        // when
        ResponseEntity<LibraryEvent> response = testRestTemplate.exchange("/v1/asynchronous-libraryevent", HttpMethod.POST, request, LibraryEvent.class);

        // then
        assertEquals(HttpStatus.CREATED, response.getStatusCode());

        ConsumerRecord<Integer, String> actualConsumerRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");
        String actualValue = actualConsumerRecord.value();

        assertEquals(expectedValue, actualValue);
    }

    @Test
    @Timeout(value = 5)
    void testPutAsynchronusLibraryEvent() {
        // given
        Book book = createBook();
        book.setBookId(456);
        LibraryEvent libraryEvent = createLibraryEvent(book);
        libraryEvent.setLibraryEventId(123);

        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON_VALUE);
        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, headers);

        String expectedValue = "{\"libraryEventId\":123,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Aleksandar Grahovac\"}}";

        // when
        ResponseEntity<LibraryEvent> response = testRestTemplate.exchange("/v1/asynchronous-libraryevent", HttpMethod.PUT, request, LibraryEvent.class);

        // then
        assertEquals(HttpStatus.CREATED, response.getStatusCode());

        ConsumerRecord<Integer, String> actualConsumerRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");
        String actualValue = actualConsumerRecord.value();

        assertEquals(expectedValue, actualValue);
    }


    private LibraryEvent createLibraryEvent(Book book) {
        return LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();
    }

    private Book createBook() {
        return Book.builder()
                .bookId(123)
                .bookName("Kafka Using Spring Boot")
                .bookAuthor("Aleksandar Grahovac")
                .build();
    }

}
