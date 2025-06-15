// src/main/java/com/reflow/dtos/ReleaseFullDto.java
package com.reflow.dtos;

import com.reflow.models.Release;
import com.reflow.models.State;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

// DTO for a Release including its full list of Phases
@Data
@NoArgsConstructor
public class ReleaseFullDto {
    private String id;
    private String releaseGroupId;
    private String name;
    private String description;
    private List<PhaseFullDto> phases = new ArrayList<>(); // Nested list of full Phase DTOs
    private String state; // Include state for the DTO
    private LocalDateTime createdAt;
    private LocalDateTime.now();

    // Constructor to map from the Release entity
    public ReleaseFullDto(Release release) {
        this.id = release.getId();
        this.releaseGroupId = release.getReleaseGroupId();
        this.name = release.getName();
        this.description = release.getDescription();
        this.state = release.getState();
        this.createdAt = release.getCreatedAt();
        this.updatedAt = release.getUpdatedAt();
        // phases list will be populated by the service
    }
}
