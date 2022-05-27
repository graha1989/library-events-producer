package com.learnkafka.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;

@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerUnitTest {

    @InjectMocks
    LibraryEventProducer libraryEventProducer;

    @Mock
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Spy
    ObjectMapper objectMapper = new ObjectMapper();


    @SuppressWarnings("unchecked")
    @Test
    void testSendAsynchronusLibraryEventUsingProducerRecord_shouldThrowExceptionListenableFutureReturnError() throws Exception {
        // given
        Book book = createBook();
        LibraryEvent libraryEvent = createLibraryEvent(book);
        SettableListenableFuture<?> settableListenableFuture = new SettableListenableFuture<>();
        settableListenableFuture.setException(new RuntimeException("Error calling Kafka"));
        
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(settableListenableFuture);

        // when
        libraryEventProducer.sendAsynchronusLibraryEventUsingProducerRecord(libraryEvent);
        
        // then
        assertThrows(Exception.class,
                () -> libraryEventProducer.sendAsynchronusLibraryEventUsingProducerRecord(libraryEvent).get());
    }

    @SuppressWarnings("unchecked")
    @Test
    void testSendAsynchronusLibraryEventUsingProducerRecord_shouldSendTheEventToKafkaBroker() throws Exception {
        // given
        Book book = createBook();
        LibraryEvent libraryEvent = createLibraryEvent(book);
        String record = objectMapper.writeValueAsString(libraryEvent);
        SettableListenableFuture<SendResult<Integer, String>> settableListenableFuture = new SettableListenableFuture<>();
        
        ProducerRecord<Integer, String> producerRecord = 
                new ProducerRecord<Integer, String>("library-events", libraryEvent.getLibraryEventId(), record);
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1), 1, 1, 342, (int) System.currentTimeMillis(), 1);
        SendResult<Integer, String> sendResult = new SendResult<Integer, String>(producerRecord, recordMetadata);
        settableListenableFuture.set(sendResult);

        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(settableListenableFuture);

        // when
        ListenableFuture<SendResult<Integer, String>> actualListenableFuture = libraryEventProducer.sendAsynchronusLibraryEventUsingProducerRecord(libraryEvent);

        // then
        SendResult<Integer, String> actualResult = actualListenableFuture.get();
        assertEquals(actualResult.getRecordMetadata().partition(), 1);
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
