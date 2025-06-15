// src/main/java/com/reflow/models/BaseEntity.java
package com.reflow.models;

import lombok.Data;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.LocalDateTime;

// BaseEntity for common fields across all documents
@Data // Lombok annotation for getters, setters, toString, equals, hashCode
public abstract class BaseEntity {

    // Represents the current status of the object
    @Field("state")
    private String state = State.PLANNED.name(); // Default state

    @CreatedDate
    @Field("createdAt")
    private LocalDateTime createdAt;

    @LastModifiedDate
    @Field("updatedAt")
    private LocalDateTime updatedAt;

    // Optional: for optimistic locking in concurrent environments
    @Version
    private Long version;
}
