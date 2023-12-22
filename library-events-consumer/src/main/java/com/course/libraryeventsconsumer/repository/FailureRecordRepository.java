package com.course.libraryeventsconsumer.repository;

import com.course.libraryeventsconsumer.entity.FailureRecordEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface FailureRecordRepository extends JpaRepository<FailureRecordEntity, Integer> {
}
