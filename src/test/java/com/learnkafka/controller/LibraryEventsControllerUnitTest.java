package com.learnkafka.controller;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.web.servlet.MockMvc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.producer.LibraryEventProducer;

@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc
class LibraryEventsControllerUnitTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private LibraryEventProducer libraryEventProducer;

    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testPostLibraryEvent_shouldSendLibraryEventToKafkaBroker() throws Exception {
        // given
        Book book = createBook();
        LibraryEvent libraryEvent = createLibraryEvent(book);
        String jsonLibraryEvent = objectMapper.writeValueAsString(libraryEvent);
        when(libraryEventProducer.sendAsynchronusLibraryEventUsingProducerRecord(libraryEvent)).thenReturn(null);

        // when
        mockMvc.perform(post("/v1/asynchronous-libraryevent")
                .content(jsonLibraryEvent)
                .contentType(MediaType.APPLICATION_JSON_VALUE))
                // then
                .andExpect(status().isCreated());

    }

    @Test
    void testPostAsynchronousLibraryEvent_shouldFailValidationOnBook() throws Exception {
        // given
        String expectedErrorMessage = "book.bookAuthor-must not be blank, book.bookId-must not be null";
        Book book = Book.builder()
                .bookId(null)
                .bookAuthor(null)
                .bookName("Kafka Using Spring Boot")
                .build();

        LibraryEvent libraryEvent = createLibraryEvent(book);
        String jsonLibraryEvent = objectMapper.writeValueAsString(libraryEvent);
        doNothing().when(libraryEventProducer).sendAsynchronusLibraryEvent(libraryEvent);

        // when
        mockMvc.perform(post("/v1/asynchronous-libraryevent")
                .content(jsonLibraryEvent)
                .contentType(MediaType.APPLICATION_JSON_VALUE))
                // then
                .andExpect(status().is4xxClientError())
                .andExpect(content().string(expectedErrorMessage));

    }

    @SuppressWarnings("unchecked")
    @Test
    void testPostSynchronousLibraryEvent_shouldSendLibraryEventToKafkaBroker() throws Exception {
        // given
        Book book = createBook();
        LibraryEvent libraryEvent = createLibraryEvent(book);
        String jsonLibraryEvent = objectMapper.writeValueAsString(libraryEvent);

        when(libraryEventProducer.sendSynchronousLibraryEventUsingTimeout(libraryEvent)).thenReturn(isA(SendResult.class));

        // when
        mockMvc.perform(post("/v1/synchronous-libraryevent")
                .content(jsonLibraryEvent)
                .contentType(MediaType.APPLICATION_JSON_VALUE))
                // then
                .andExpect(status().isCreated());

    }

    @Test
    void testPostAsynchronusLibraryEventWithProducerRecord_shouldSendLibraryEventToKafkaBroker() throws Exception {
        // given
        Book book = createBook();
        LibraryEvent libraryEvent = createLibraryEvent(book);
        String jsonLibraryEvent = objectMapper.writeValueAsString(libraryEvent);

        when(libraryEventProducer.sendAsynchronusLibraryEventUsingProducerRecord(libraryEvent)).thenReturn(null);

        // when
        mockMvc.perform(post("/v1/asynchronous-libraryevent-producerrecord")
                .content(jsonLibraryEvent)
                .contentType(MediaType.APPLICATION_JSON_VALUE))
                // then
                .andExpect(status().isCreated());

    }

    @Test
    void testPutAsynchronusLibraryEventWithProducerRecord_shouldUpdateLibraryEventToKafkaBroker() throws Exception {
        // given
        Book book = createBook();
        book.setBookId(456);
        LibraryEvent libraryEvent = createLibraryEvent(book);
        libraryEvent.setLibraryEventId(123);
        String jsonLibraryEvent = objectMapper.writeValueAsString(libraryEvent);

        when(libraryEventProducer.sendAsynchronusLibraryEventUsingProducerRecord(libraryEvent)).thenReturn(null);

        // when
        mockMvc.perform(put("/v1/asynchronous-libraryevent")
                .content(jsonLibraryEvent)
                .contentType(MediaType.APPLICATION_JSON_VALUE))
                // then
                .andExpect(status().isCreated());

    }

    @Test
    void testPutAsynchronusLibraryEventWithProducerRecord_shouldThrowExceptionWhenLibraryEventIdIsNull() throws Exception {
        // given
        Book book = createBook();
        book.setBookId(456);
        LibraryEvent libraryEvent = createLibraryEvent(book);
        String jsonLibraryEvent = objectMapper.writeValueAsString(libraryEvent);

        when(libraryEventProducer.sendAsynchronusLibraryEventUsingProducerRecord(libraryEvent)).thenReturn(null);

        // when
        mockMvc.perform(put("/v1/asynchronous-libraryevent")
                .content(jsonLibraryEvent)
                .contentType(MediaType.APPLICATION_JSON_VALUE))
                // then
                .andExpect(status().isBadRequest());

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
