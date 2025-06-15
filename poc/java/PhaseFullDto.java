// src/main/java/com/reflow/dtos/PhaseFullDto.java
package com.reflow.dtos;

import com.reflow.models.Phase;
import com.reflow.models.State;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

// DTO for a Phase including its full list of Tasks
@Data
@NoArgsConstructor
public class PhaseFullDto {
    private String id;
    private Phase.ParentType parentType;
    private String parentId;
    private String name;
    private String description;
    private List<TaskDto> tasks = new ArrayList<>(); // Nested list of full Task DTOs
    private String state; // Include state for the DTO
    private LocalDateTime createdAt;
    private LocalDateTime.now();

    // Constructor to map from the Phase entity
    public PhaseFullDto(Phase phase) {
        this.id = phase.getId();
        this.parentType = phase.getParentType();
        this.parentId = phase.getParentId();
        this.name = phase.getName();
        this.description = phase.getDescription();
        this.state = phase.getState();
        this.createdAt = phase.getCreatedAt();
        this.updatedAt = phase.getUpdatedAt();
        // tasks list will be populated by the service
    }
}
