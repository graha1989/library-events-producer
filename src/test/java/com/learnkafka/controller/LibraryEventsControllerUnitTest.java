package com.learnkafka.controller;

import static org.mockito.Mockito.doNothing;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
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
        doNothing().when(libraryEventProducer).sendAsynchronusLibraryEvent(libraryEvent);

        // when
        mockMvc.perform(post("/v1/asynchronous-libraryevent")
                .content(jsonLibraryEvent)
                .contentType(MediaType.APPLICATION_JSON_VALUE))
                // then
                .andExpect(status().isCreated());

    }

    @Test
    void testPostLibraryEvent_shouldFailValidationOnBook() throws Exception {
        // given
        LibraryEvent libraryEvent = createLibraryEvent(null);
        String jsonLibraryEvent = objectMapper.writeValueAsString(libraryEvent);
        doNothing().when(libraryEventProducer).sendAsynchronusLibraryEvent(libraryEvent);

        // when
        mockMvc.perform(post("/v1/asynchronous-libraryevent")
                .content(jsonLibraryEvent)
                .contentType(MediaType.APPLICATION_JSON_VALUE))
                // then
                .andExpect(status().is4xxClientError());

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
