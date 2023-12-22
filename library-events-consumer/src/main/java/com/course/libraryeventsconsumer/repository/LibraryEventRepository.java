package com.course.libraryeventsconsumer.repository;

import com.course.libraryeventsconsumer.entity.LibraryEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LibraryEventRepository extends JpaRepository<LibraryEventEntity, Integer> {
}
