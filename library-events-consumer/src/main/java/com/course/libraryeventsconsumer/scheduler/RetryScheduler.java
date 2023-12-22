package com.course.libraryeventsconsumer.scheduler;

import com.course.libraryeventsconsumer.config.ConsumerConfig;
import com.course.libraryeventsconsumer.entity.FailureRecordEntity;
import com.course.libraryeventsconsumer.repository.FailureRecordRepository;
import com.course.libraryeventsconsumer.service.LibraryEventService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class RetryScheduler {

    private final LibraryEventService libraryEventService;
    private final FailureRecordRepository failureRecordRepository;

    // Executes every 10 seconds
    @Scheduled(fixedRate = 10000)
    public void retryFailedRecords(){
        log.info("Started retrying process for failed records...");

        failureRecordRepository.findAllByStatus(ConsumerConfig.RETRY)
                .forEach(failureRecord -> {
                    try {
                        var consumerRecord = buildConsumerRecord(failureRecord);
                        libraryEventService.processLibraryEvent(consumerRecord);
                        failureRecord.setStatus(ConsumerConfig.SENT);

                        failureRecordRepository.save(failureRecord);
                    } catch (Exception e){
                        log.error("Exception in retryFailedRecords : ", e);
                    }
                });

    }

    private ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecordEntity failureRecord) {
        return new ConsumerRecord<>(
                failureRecord.getTopic(),
                failureRecord.getPartition(),
                failureRecord.getOffset_value(),
                failureRecord.getKey_value(),
                failureRecord.getErrorRecord()
        );
    }
}
