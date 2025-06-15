// src/main/java/com/reflow/models/AllModels.java
package com.reflow.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Enum for all possible states across different entities
enum State {
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

// BaseEntity for common fields across all documents
@Data // Lombok annotation for getters, setters, toString, equals, hashCode
abstract class BaseEntity {

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

// ReleaseContext Model
@Document(collection = "releaseContexts")
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
class ReleaseContext extends BaseEntity {
    @Id
    // The ID of the ReleaseContext will typically match the ID of the Release or ReleaseGroup it belongs to.
    private String id;

    // Entity type this context belongs to (RELEASE_GROUP or RELEASE)
    @Field("entityType")
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

    // Enum for ContextEntityType
    enum ContextEntityType {
        RELEASE_GROUP,
        RELEASE
    }
}


// ReleaseGroup Model
@Document(collection = "releaseGroups")
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true) // Include superclass fields in equals and hashCode
class ReleaseGroup extends BaseEntity {
    @Id // Marks this field as the document's primary key
    private String id;
    private String name;
    private String description;

    @Field("releaseIds")
    private List<String> releaseIds = new ArrayList<>(); // List of child Release IDs

    @Field("phaseIds")
    private List<String> phaseIds = new ArrayList<>(); // List of child Phase IDs directly under this ReleaseGroup

    @Field("releaseContextId")
    private String releaseContextId; // Link to its ReleaseContext document
}

// Release Model
@Document(collection = "releases")
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
class Release extends BaseEntity {
    @Id
    private String id;
    @Field("releaseGroupId")
    private String releaseGroupId; // Link to parent ReleaseGroup
    private String name;
    private String description;

    @Field("phaseIds")
    private List<String> phaseIds = new ArrayList<>(); // List of child Phase IDs

    @Field("releaseContextId")
    private String releaseContextId; // Link to its ReleaseContext document
}

// Phase Model
@Document(collection = "phases")
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
class Phase extends BaseEntity {
    @Id
    private String id;
    @Field("parentType")
    private ParentType parentType; // Enum: RELEASE_GROUP or RELEASE
    @Field("parentId")
    private String parentId; // Reference to the parent's actual ID (releaseGroupId or releaseId)
    private String name;
    private String description;
    // isGate and gateStatus moved to Task.java as per new requirement

    @Field("taskIds")
    private List<String> taskIds = new ArrayList<>(); // List of child Task IDs

    // Enum for ParentType
    enum ParentType {
        RELEASE_GROUP,
        RELEASE
    }
}

// Task Model
@Document(collection = "tasks")
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
class Task extends BaseEntity {
    @Id
    private String id;
    @Field("phaseId")
    private String phaseId;
    @Field("releaseId")
    private String releaseId; // Denormalized for easier querying and indexing
    private String name;
    private String description;
    @Field("type")
    private TaskType type; // Enum: REGULAR, PARALLEL_GROUP, SEQUENTIAL_GROUP, APPROVAL, SCHEDULER, TRIGGER
    @Field("previousTaskId")
    private String previousTaskId; // Link to the task that precedes this one
    @Field("nextTaskId")
    private String nextTaskId;     // Link to the task that follows this one
    @Field("groupId")
    private String groupId;       // If this task is a child of a group task (e.g., in PARALLEL_GROUP)
    @Field("scheduledTime")
    private LocalDateTime scheduledTime; // For SCHEDULER tasks: when it's supposed to run
    private List<String> approvers;                   // For APPROVAL tasks: list of user IDs or roles
    @Field("approvedBy")
    private String approvedBy;                  // Who approved the task
    @Field("approvalDate")
    private LocalDateTime approvalDate;                  // When it was approved
    private String reason;                      // Reason for approval/rejection

    @Field("isGate")
    private boolean isGate; // New: True if this task acts as a gate
    @Field("gateStatus")
    private GateStatus gateStatus; // New: Status for gate tasks

    @Field("actor")
    private String actor; // New: Defines which part of the system or team executes this task type.

    @Field("taskVariables")
    private Map<String, Object> taskVariables = new HashMap<>(); // NEW: Dynamic variables for task execution context

    @Field("logs")
    private String logs; // NEW: To capture stdout/stderr from JAR executions

    @Field("triggerInterval")
    private Integer triggerInterval; // NEW: For TRIGGER tasks, in seconds

    @Field("inputVariables")
    private Map<String, Object> inputVariables = new HashMap<>(); // NEW: Input parameters for the task's execution
    @Field("outputVariables")
    private Map<String, Object> outputVariables = new HashMap<>(); // NEW: Output results from the task's execution

    // Enum for TaskType
    enum TaskType {
        REGULAR,
        PARALLEL_GROUP,
        SEQUENTIAL_GROUP,
        APPROVAL,
        SCHEDULER,
        TRIGGER
    }

    // Enum for GateStatus
    enum GateStatus {
        PENDING, // Awaiting decision
        APPROVED, // The gate has been approved
        REJECTED // The gate has been rejected
    }
}
