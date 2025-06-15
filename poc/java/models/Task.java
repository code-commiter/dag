// src/main/java/com/reflow/models/Task.java
package com.reflow.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Document(collection = "tasks")
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class Task extends BaseEntity {
    @Id
    private String id;
    @Field("phaseId")
    private String phaseId;
    @Field("releaseId")
    private String releaseId; // Denormalized for easier querying and indexing
    private String name;
    private String description;
    @Field("type")
    public enum TaskType { // Made public
        REGULAR,
        PARALLEL_GROUP,
        SEQUENTIAL_GROUP,
        APPROVAL,
        SCHEDULER,
        TRIGGER
    }
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
    public enum GateStatus { // Made public
        PENDING, // Awaiting decision
        APPROVED, // The gate has been approved
        REJECTED // The gate has been rejected
    }
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

    @Field("childTaskIds")
    private List<String> childTaskIds = new ArrayList<>(); // NEW: Direct field for group tasks

}
