// src/main/java/com/reflow/dtos/ReleaseGroupFullDto.java
package com.reflow.dtos;

import com.reflow.models.ReleaseGroup;
import com.reflow.models.ReleaseContext; // Only for reference if needed, not direct inclusion
import com.reflow.models.State;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

// DTO for a ReleaseGroup including its full list of Releases and direct Phases
@Data
@NoArgsConstructor
public class ReleaseGroupFullDto {
    private String id;
    private String name;
    private String description;
    private List<ReleaseFullDto> releases = new ArrayList<>(); // Nested list of full Release DTOs
    private List<PhaseFullDto> directPhases = new ArrayList<>(); // Phases directly under ReleaseGroup
    private String releaseContextId;
    private String state; // Include state for the DTO
    private LocalDateTime createdAt;
    private LocalDateTime.now();

    // Constructor to map from the ReleaseGroup entity
    public ReleaseGroupFullDto(ReleaseGroup releaseGroup) {
        this.id = releaseGroup.getId();
        this.name = releaseGroup.getName();
        this.description = releaseGroup.getDescription();
        this.releaseContextId = releaseGroup.getReleaseContextId();
        this.state = releaseGroup.getState();
        this.createdAt = releaseGroup.getCreatedAt();
        this.updatedAt = releaseGroup.getUpdatedAt();
        // releases and directPhases lists will be populated by the service/controller
    }
}
