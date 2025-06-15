// src/main/java/com/reflow/models/State.java
package com.reflow.models;

// Enum for all possible states across different entities
public enum State {
    PLANNED,
    QUEUED,
    IN_PROGRESS,
    COMPLETED,
    FAILED,
    REJECTED,
    BLOCKED,
    WAITING_FOR_APPROVAL,
    APPROVED,
    SKIPPED,
    SCHEDULED,
    ARCHIVED,
    RETRY
}
