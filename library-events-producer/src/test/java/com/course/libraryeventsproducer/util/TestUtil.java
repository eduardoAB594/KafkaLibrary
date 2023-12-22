package libraryeventsproducer.util;

import com.course.libraryeventsproducer.domain.Book;
import com.course.libraryeventsproducer.domain.LibraryEvent;
import com.course.libraryeventsproducer.domain.LibraryEventType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestUtil {

    public static Book bookRecord() {
        return new Book(123, "Basic Kafka Principles","Eduardo");
    }

    public static Book bookRecordWithInvalidValues() {
        return new Book(null, "","Eduardo");
    }

    public static LibraryEvent libraryEventRecord() {
        return new LibraryEvent(null,
                        LibraryEventType.NEW,
                        bookRecord());
    }

    public static LibraryEvent newLibraryEventRecordWithLibraryEventId() {
        return new LibraryEvent(123,
                        LibraryEventType.NEW,
                        bookRecord());
    }

    public static LibraryEvent libraryEventRecordUpdate() {
        return new LibraryEvent(123,
                        LibraryEventType.UPDATE,
                        bookRecord());
    }

    public static LibraryEvent libraryEventRecordUpdateWithNullLibraryEventId() {
        return new LibraryEvent(null,
                        LibraryEventType.UPDATE,
                        bookRecord());
    }

    public static LibraryEvent libraryEventRecordWithInvalidBook() {
        return new LibraryEvent(null,
                        LibraryEventType.NEW,
                        bookRecordWithInvalidValues());
    }

    public static LibraryEvent parseLibraryEventRecord(ObjectMapper objectMapper , String json){
        try {
            return  objectMapper.readValue(json, LibraryEvent.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }
}
