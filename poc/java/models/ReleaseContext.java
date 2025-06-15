// src/main/java/com/reflow/models/ReleaseContext.java
package com.reflow.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// ReleaseContext Model
@Document(collection = "releaseContexts")
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class ReleaseContext extends BaseEntity {
    @Id
    // The ID of the ReleaseContext will typically match the ID of the Release or ReleaseGroup it belongs to.
    private String id;

    // Entity type this context belongs to (RELEASE_GROUP or RELEASE)
    @Field("entityType")
    public enum ContextEntityType { // Made public
        RELEASE_GROUP,
        RELEASE
    }
    private ContextEntityType entityType;

    // Global data shared across all environments for this release/release group
    // Example: "towerTemplateValidationStatus": { "templateId123": true }
    @Field("globalShared")
    private Map<String, Object> globalShared = new HashMap<>();

    // Map of environment-specific data
    // Key: Environment Name (e.g., "DEV", "QA", "PROD")
    // Value: Map containing deployment status, test results, environment-specific credentials/URLs, etc.
    // Example: "environments": { "DEV": { "deploymentStatus": "deployed", "baseUrl": "...", "taskParams": {} } }
    @Field("environments")
    private Map<String, Map<String, Object>> environments = new HashMap<>();

    // For Release Group contexts, this can track its children's contexts
    @Field("childReleaseContextIds")
    private List<String> childReleaseContextIds = new ArrayList<>();
}
