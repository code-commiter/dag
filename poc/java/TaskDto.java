// src/main/java/com/reflow/dtos/TaskDto.java
package com.reflow.dtos;

import com.reflow.models.State;
import com.reflow.models.Task;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

@Data
@NoArgsConstructor
public class TaskDto {
    private String id;
    private String phaseId;
    private String releaseId;
    private String name;
    private String description;
    private Task.TaskType type;
    private String previousTaskId;
    private String nextTaskId;
    private String groupId;
    private List<String> childTaskIds; // NEW: Direct field in DTO
    private LocalDateTime scheduledTime;
    private List<String> approvers;
    private String approvedBy;
    private LocalDateTime approvalDate;
    private String reason;
    private boolean isGate;
    private Task.GateStatus gateStatus;
    private String actor;
    private Map<String, Object> taskVariables = new HashMap<>();
    private String logs;
    private Integer triggerInterval;
    private Map<String, Object> inputVariables = new HashMap<>();
    private Map<String, Object> outputVariables = new HashMap<>();
    private String state;
    private List<TaskDto> childrenTasks = new ArrayList<>(); // For nested DTO representation

    // Constructor to map from the Task entity
    public TaskDto(Task task) {
        this.id = task.getId();
        this.phaseId = task.getPhaseId();
        this.releaseId = task.getReleaseId();
        this.name = task.getName();
        this.description = task.getDescription();
        this.type = task.getType();
        this.previousTaskId = task.getPreviousTaskId();
        this.nextTaskId = task.getNextTaskId();
        this.groupId = task.getGroupId();
        this.childTaskIds = task.getChildTaskIds(); // Map the new direct field
        this.scheduledTime = task.getScheduledTime();
        this.approvers = task.getApprovers();
        this.approvedBy = task.getApprovedBy();
        this.approvalDate = task.getApprovalDate();
        this.reason = task.getReason();
        this.isGate = task.isGate();
        this.gateStatus = task.getGateStatus();
        this.actor = task.getActor();
        this.taskVariables = task.getTaskVariables() != null ? new HashMap<>(task.getTaskVariables()) : new HashMap<>();
        this.logs = task.getLogs();
        this.triggerInterval = task.getTriggerInterval();
        this.inputVariables = task.getInputVariables() != null ? new HashMap<>(task.getInputVariables()) : new HashMap<>();
        this.outputVariables = task.getOutputVariables() != null ? new HashMap<>(task.getOutputVariables()) : new HashMap<>();
        this.state = task.getState();
        // childrenTasks will be populated by the service recursively, not from the entity directly
    }
}
