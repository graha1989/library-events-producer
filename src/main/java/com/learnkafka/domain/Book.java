package com.learnkafka.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class Book {

    @Builder.Default
    private Integer bookId;

    @Builder.Default
    private String bookName;

    @Builder.Default
    private String bookAuthor;

}
