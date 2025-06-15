// src/main/java/com/reflow/models/Task.java
package com.reflow.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@Document(collection = "tasks")
@JsonInclude(JsonInclude.Include.NON_NULL) // Exclude null fields from JSON serialization
public class Task extends BaseEntity {

    public enum TaskType {
        REGULAR, // A standard, atomic task
        APPROVAL, // A task requiring human approval (gate)
        PARALLEL_GROUP, // A composite task that executes children concurrently
        SEQUENTIAL_GROUP, // A composite task that executes children sequentially
        TRIGGER, // A task that continuously triggers an external system until completion
        SCHEDULER // A task that waits for a specific scheduled time before being queued
    }

    public enum GateStatus {
        PENDING,
        PASSED,
        FAILED
    }

    private String name;
    private String description;
    private TaskType type;

    // Workflow chaining: ID of the previous and next task in a sequence
    private String previousTaskId;
    private String nextTaskId;

    // Grouping: ID of the parent group task if this is a child task
    private String groupId;

    // NEW: Direct field for child task IDs for PARALLEL_GROUP/SEQUENTIAL_GROUP tasks
    private List<String> childTaskIds;

    // Relationships: IDs of parent entities
    private String phaseId;
    private String releaseId; // Can be null if phase is directly under ReleaseGroup

    private LocalDateTime scheduledTime; // For SCHEDULER tasks or tasks with a specific start time

    // Approval specific fields
    private List<String> approvers; // List of user IDs/roles required for approval
    private String approvedBy;     // User ID who approved/rejected
    private LocalDateTime approvalDate;
    private String reason;         // Reason for approval/rejection or state change

    // Gate specific fields
    private boolean isGate;
    private GateStatus gateStatus;
    private String gateCategory; // e.g., "Security Compliance", "Development Readiness"

    // Actor responsible for executing/processing the task
    private String actor; // e.g., "DEPLOYMENT_SYSTEM", "QA_LEAD", "ORCHESTRATOR"

    // Dynamic variables for task execution/context
    private Map<String, Object> taskVariables = new HashMap<>();

    // Execution logs
    private String logs;

    // For TRIGGER tasks: interval in seconds to re-trigger the associated action
    private Integer triggerInterval;

    // Input and output variables for task execution
    private Map<String, Object> inputVariables = new HashMap<>();
    private Map<String, Object> outputVariables = new HashMap<>();

    // Constructor for initial creation (simplified)
    public Task(String name, String description, TaskType type, String phaseId, String releaseId, String actor) {
        super(State.PLANNED.name()); // Default state for new tasks
        this.name = name;
        this.description = description;
        this.type = type;
        this.phaseId = phaseId;
        this.releaseId = releaseId;
        this.actor = actor;
    }
}
