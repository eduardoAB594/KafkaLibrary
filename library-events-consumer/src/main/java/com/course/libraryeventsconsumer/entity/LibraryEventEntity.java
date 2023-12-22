package com.course.libraryeventsconsumer.entity;

import jakarta.persistence.*;
import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
@Setter
@Entity(name = "library_event")
public class LibraryEventEntity {

    @Id
    @GeneratedValue
    private Integer libraryEventId;

    @Enumerated(EnumType.STRING)
    private LibraryEventType libraryEventType;

    @OneToOne(mappedBy = "libraryEventEntity", cascade = {CascadeType.ALL})
    @ToString.Exclude
    private BookEntity bookEntity;

}
