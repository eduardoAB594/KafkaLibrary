package com.course.libraryeventsproducer.controller;

import com.course.libraryeventsproducer.domain.LibraryEvent;
import com.course.libraryeventsproducer.producer.LibraryEventsProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import libraryeventsproducer.util.TestUtil;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventsController.class)
public class LibraryEventsControllerTest {

    @Autowired
    MockMvc mockMvc;

    @Autowired
    ObjectMapper objectMapper;

    @MockBean
    LibraryEventsProducer libraryEventsProducer;

    @Test
    void postLibraryEvent() throws Exception {
        var json = objectMapper.writeValueAsString(TestUtil.libraryEventRecord());

        doNothing()
                .when(libraryEventsProducer)
                .sendLibraryEventAsync(any(LibraryEvent.class));

        mockMvc.perform(
                post("/v1/libraryEvent")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON)
        ).andExpect(status().isCreated());
    }

    @Test
    void shouldFailWithInvalidValues() throws Exception {
        var json = objectMapper.writeValueAsString(TestUtil.libraryEventRecordWithInvalidBook());

        doNothing()
                .when(libraryEventsProducer)
                .sendLibraryEventAsync(any(LibraryEvent.class));

        mockMvc.perform(
                post("/v1/libraryEvent")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON)
        ).andExpect(status().is4xxClientError());
    }
}
