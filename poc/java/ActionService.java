// src/main/java/com/reflow/services/ActionService.java
package com.reflow.services;

import com.reflow.models.*;
import com.reflow.models.Task.GateStatus;
import com.reflow.models.Task.TaskType;
import com.reflow.repositories.*;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.HashMap; // Added for taskVariables initialization

@Service
@RequiredArgsConstructor
public class ActionService {

    private final ReleaseGroupRepository releaseGroupRepository;
    private final ReleaseRepository releaseRepository;
    private final PhaseRepository phaseRepository;
    private final TaskRepository taskRepository;
    private final KafkaProducerService kafkaProducerService; // Still here, but not used for cascading directly now

    // Defines states considered "modifiable" for an entity.
    private static final List<String> MODIFIABLE_STATES = Arrays.asList(
            State.PLANNED.name()
    );

    // Defines states considered "terminal" for workflow progression.
    private static final List<String> TERMINAL_STATES = Arrays.asList(
            State.COMPLETED.name(), State.SKIPPED.name(),
            State.ARCHIVED.name(), State.FAILED.name()
    );

    private boolean isTerminalState(String stateStr) {
        return TERMINAL_STATES.contains(stateStr);
    }

    // Helper to check if a task can be modified
    public boolean isTaskModifiable(Task task) {
        // Only PLANNED tasks are generally modifiable for structural changes
        if (!MODIFIABLE_STATES.contains(task.getState())) {
            return false;
        }

        // Check if its parent Phase is also modifiable (e.g., not IN_PROGRESS or terminal)
        Optional<Phase> parentPhaseOpt = phaseRepository.findById(task.getPhaseId());
        if (parentPhaseOpt.isPresent()) {
            Phase parentPhase = parentPhaseOpt.get();
            if (parentPhase.getState().equals(State.IN_PROGRESS.name()) || isTerminalState(parentPhase.getState())) {
                return false;
            }
        } else {
            // Parent phase not found, this indicates a data inconsistency
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Parent phase not found for task " + task.getId());
        }

        // Check if its associated Release is also modifiable
        Optional<Release> associatedReleaseOpt = releaseRepository.findById(task.getReleaseId());
        if (associatedReleaseOpt.isPresent()) {
            Release associatedRelease = associatedReleaseOpt.get();
            if (associatedRelease.getState().equals(State.IN_PROGRESS.name()) || isTerminalState(associatedRelease.getState())) {
                return false;
            }
        } else {
            // Associated release not found, data inconsistency
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Associated release not found for task " + task.getId());
        }

        // If part of a group, check the group task too
        if (task.getGroupId() != null && !task.getGroupId().trim().isEmpty()) {
            Optional<Task> parentGroupTaskOpt = taskRepository.findById(task.getGroupId());
            if (parentGroupTaskOpt.isPresent()) {
                Task parentGroupTask = parentGroupTaskOpt.get();
                if (parentGroupTask.getState().equals(State.IN_PROGRESS.name()) || isTerminalState(parentGroupTask.getState())) {
                    return false;
                }
            } else {
                 throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Parent group task not found for task " + task.getId());
            }
        }

        return true;
    }

    @Transactional
    public BaseEntity transitionState(String entityType, String entityId, String newState, String approvedBy, String reason) {
        BaseEntity entity;
        Optional<? extends BaseEntity> entityOpt;

        switch (entityType) {
            case "ReleaseGroup":
                entityOpt = releaseGroupRepository.findById(entityId);
                break;
            case "Release":
                entityOpt = releaseRepository.findById(entityId);
                break;
            case "Phase":
                entityOpt = phaseRepository.findById(entityId);
                break;
            case "Task":
                entityOpt = taskRepository.findById(entityId);
                break;
            default:
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid entityType: " + entityType);
        }

        entity = entityOpt.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, entityType + " not found with ID: " + entityId));

        String currentState = entity.getState();
        String actualNewStateToApply = newState; // This will be the actual state applied to the entity

        // Store the original incoming newState string from the request for cascading logic and enum mapping
        String originalIncomingStateFromRequest = newState;


        // Handle special cases for Task (especially gates)
        if (entity instanceof Task) {
            Task task = (Task) entity;
            // Intercept "APPROVED" / "REJECTED" strings from UI for gates to update gateStatus and determine next workflow state
            if (task.isGate()) {
                // Determine if the gate's workflow can proceed based on previous task completion
                boolean canGateWorkflowProceed = false;
                if (task.getPreviousTaskId() == null || task.getPreviousTaskId().trim().isEmpty()) {
                    // This is the first task in its sequence
                    canGateWorkflowProceed = true;
                } else {
                    Optional<Task> previousTaskOpt = taskRepository.findById(task.getPreviousTaskId());
                    if (previousTaskOpt.isPresent()) {
                        Task previousTask = previousTaskOpt.get();
                        if (isTerminalState(previousTask.getState())) { // Previous task is COMPLETED, FAILED, or SKIPPED
                            canGateWorkflowProceed = true;
                        }
                    }
                }

                if (originalIncomingStateFromRequest.equalsIgnoreCase(GateStatus.APPROVED.name())) { // Using GateStatus.APPROVED.name()
                    task.setGateStatus(GateStatus.APPROVED);
                    task.setApprovedBy(approvedBy);
                    task.setApprovalDate(LocalDateTime.now());
                    task.setReason(reason);
                    
                    if (canGateWorkflowProceed) {
                        // If approved and previous task completed, it can transition to QUEUED
                        if (currentState.equalsIgnoreCase(State.PLANNED.name()) || currentState.equalsIgnoreCase(State.WAITING_FOR_APPROVAL.name())) {
                            actualNewStateToApply = State.QUEUED.name();
                        } else {
                            // If it's already IN_PROGRESS, COMPLETED, FAILED, etc., just keep its state.
                            actualNewStateToApply = currentState;
                        }
                    } else {
                        // If approved but previous task not completed, keep current state (e.g., WAITING_FOR_APPROVAL or PLANNED)
                        actualNewStateToApply = currentState;
                        System.out.println(String.format("Gate task %s (ID: %s) APPROVED, but previous task not completed. Task state remains %s.",
                            task.getName(), task.getId(), currentState));
                    }
                    System.out.println(String.format("Gate task %s (ID: %s) received APPROVED. GateStatus set to APPROVED. Task state set to %s (from %s). CanProceed: %s",
                        task.getName(), task.getId(), actualNewStateToApply, currentState, canGateWorkflowProceed));
                } else if (originalIncomingStateFromRequest.equalsIgnoreCase(GateStatus.REJECTED.name())) { // Using GateStatus.REJECTED.name()
                    task.setGateStatus(GateStatus.REJECTED);
                    task.setApprovedBy(approvedBy);
                    task.setApprovalDate(LocalDateTime.now());
                    task.setReason(reason);
                    // If a gate is rejected, its workflow state should always become FAILED regardless of preceding task
                    if (currentState.equalsIgnoreCase(State.PLANNED.name()) || currentState.equalsIgnoreCase(State.WAITING_FOR_APPROVAL.name()) || currentState.equalsIgnoreCase(State.QUEUED.name()) || currentState.equalsIgnoreCase(State.IN_PROGRESS.name())) {
                        actualNewStateToApply = State.FAILED.name();
                    } else {
                        actualNewStateToApply = currentState; // Already in a terminal/unmodifiable state
                    }
                    System.out.println(String.format("Gate task %s (ID: %s) received REJECTED. GateStatus set to REJECTED. Task state set to %s (from %s).",
                        task.getName(), task.getId(), actualNewStateToApply, currentState));
                }
                // For other transitions (e.g., from DAG: QUEUED, IN_PROGRESS, WAITING_FOR_APPROVAL), `newState` applies to `task.state` normally.
            }
        }


        // Basic state transition validation (can be expanded)
        boolean isValidTransition = false;
        try {
            State targetStateEnum = State.valueOf(actualNewStateToApply.toUpperCase()); // Validate the state we *intend* to apply
            State currentEnumState = State.valueOf(currentState.toUpperCase());

            // General transitions allowed
            if (currentEnumState == State.PLANNED) {
                isValidTransition = true;
            } else if (currentEnumState == State.QUEUED) { // QUEUED can go to IN_PROGRESS or FAILED
                 isValidTransition = (targetStateEnum == State.IN_PROGRESS || targetStateEnum == State.FAILED);
            } else if (currentEnumState == State.IN_PROGRESS) {
                isValidTransition = (targetStateEnum == State.COMPLETED || targetStateEnum == State.FAILED || targetStateEnum == State.BLOCKED || targetStateEnum == State.WAITING_FOR_APPROVAL);
            } else if (currentEnumState == State.WAITING_FOR_APPROVAL) {
                 // WAITING_FOR_APPROVAL can go to QUEUED (if approved), or IN_PROGRESS (if manually resumed)
                 isValidTransition = (targetStateEnum == State.QUEUED || targetStateEnum == State.IN_PROGRESS || targetStateEnum == State.FAILED);
            } else if (currentEnumState == State.FAILED) {
                isValidTransition = (targetStateEnum == State.IN_PROGRESS || targetStateEnum == State.RETRY || targetStateEnum == State.BLOCKED);
            } else if (currentEnumState == State.BLOCKED) {
                isValidTransition = (targetStateEnum == State.IN_PROGRESS || targetStateEnum == State.QUEUED);
            } else if (currentEnumState == State.SCHEDULED) {
                 isValidTransition = (targetStateEnum == State.QUEUED);
            } else if (currentEnumState == State.RETRY) {
                isValidTransition = (targetStateEnum == State.IN_PROGRESS || targetStateEnum == State.QUEUED);
            } else if (isTerminalState(currentState)) { // For already terminal states (COMPLETED, SKIPPED, FAILED, ARCHIVED)
                isValidTransition = (targetStateEnum == State.ARCHIVED && !currentState.equals(State.ARCHIVED.name()));
            }

            // Edge case: If actualNewStateToApply is same as currentState (e.g., gate status updated, but state not changed by design)
            if (actualNewStateToApply.equalsIgnoreCase(currentState)) {
                isValidTransition = true; // Always allow idempotent updates
            }


        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid newState value: " + actualNewStateToApply);
        }

        if (!isValidTransition) {
             throw new ResponseStatusException(HttpStatus.BAD_REQUEST, String.format("Invalid state transition from %s to %s for %s %s. (Attempted new state: %s)", currentState, actualNewStateToApply, entityType, entityId, newState));
        }

        entity.setState(actualNewStateToApply); // Apply the determined actualNewStateToApply
        entity.setUpdatedAt(LocalDateTime.now()); // Ensure updatedAt is set

        // Save the entity
        if (entity instanceof Task) {
            taskRepository.save((Task) entity);
        } else if (entity instanceof Phase) {
            phaseRepository.save((Phase) entity);
        } else if (entity instanceof Release) {
            releaseRepository.save((Release) entity);
        } else if (entity instanceof ReleaseGroup) {
            releaseGroupRepository.save((ReleaseGroup) entity);
        }
        
        // --- Direct Cascading Approval for Release Group Gates ---
        // If this is a gate task and its parent phase is for a Release Group,
        // and its *original incoming* state was "APPROVED" or "REJECTED" (from UI),
        // then directly update child tasks' gateStatus and their workflow state.
        
        if (entity instanceof Task) { // Only proceed with cascading if the primary entity was a Task
            Task primaryTask = (Task) entity;
            if (primaryTask.isGate() && (originalIncomingStateFromRequest != null &&
                                          (originalIncomingStateFromRequest.equalsIgnoreCase(GateStatus.APPROVED.name()) || originalIncomingStateFromRequest.equalsIgnoreCase(GateStatus.REJECTED.name())))) { // Using GateStatus.name()
                Optional<Phase> parentPhaseOpt = phaseRepository.findById(primaryTask.getPhaseId());
                if (parentPhaseOpt.isPresent()) {
                    Phase parentPhase = parentPhaseOpt.get();
                    if (parentPhase.getParentType() == Phase.ParentType.RELEASE_GROUP) {
                        String releaseGroupId = parentPhase.getParentId();
                        String gateTaskName = primaryTask.getName(); // Use task name for matching cascaded gates

                        // Determine the target gateStatus for cascaded tasks
                        GateStatus cascadedGateStatus = originalIncomingStateFromRequest.equalsIgnoreCase(GateStatus.APPROVED.name()) ? GateStatus.APPROVED : GateStatus.REJECTED; // Using GateStatus enum directly
                        // Determine the target workflow state for cascaded tasks based on the decision
                        String cascadedTaskWorkflowState = originalIncomingStateFromRequest.equalsIgnoreCase(GateStatus.APPROVED.name()) ? State.QUEUED.name() : State.FAILED.name(); // Using GateStatus.name()

                        System.out.println(String.format("Gate task %s (ID: %s) under ReleaseGroup %s received %s. Initiating direct cascading approval.",
                                gateTaskName, primaryTask.getId(), releaseGroupId, originalIncomingStateFromRequest));

                        // Find all releases belonging to this Release Group
                        List<Release> releasesInGroup = releaseRepository.findByReleaseGroupId(releaseGroupId);

                        for (Release rel : releasesInGroup) {
                            // Find phases within this release
                            List<Phase> releasePhases = phaseRepository.findByParentIdAndParentType(rel.getId(), Phase.ParentType.RELEASE);
                            for (Phase rp : releasePhases) {
                                // Find matching gate tasks within this release's phase
                                List<Task> matchingGateTasks = taskRepository.findByPhaseIdAndIsGateIsTrueAndName(rp.getId(), gateTaskName);
                                for (Task cascadedGateTask : matchingGateTasks) {
                                    System.out.println(String.format("Directly updating gate status for Release %s, Phase %s, Task %s to gateStatus %s and state %s.",
                                            rel.getId(), rp.getId(), cascadedGateTask.getId(), cascadedGateStatus, cascadedTaskWorkflowState));
                                    
                                    // Determine if the cascaded gate's workflow can proceed based on previous task completion
                                    boolean canCascadedGateWorkflowProceed = false;
                                    if (cascadedGateTask.getPreviousTaskId() == null || cascadedGateTask.getPreviousTaskId().trim().isEmpty()) {
                                        canCascadedGateWorkflowProceed = true;
                                    } else {
                                        Optional<Task> prevCascadedTaskOpt = taskRepository.findById(cascadedGateTask.getPreviousTaskId());
                                        if (prevCascadedTaskOpt.isPresent()) {
                                            Task prevCascadedTask = prevCascadedTaskOpt.get();
                                            if (isTerminalState(prevCascadedTask.getState())) {
                                                canCascadedGateWorkflowProceed = true;
                                            }
                                        }
                                    }

                                    // Only update if the gateStatus or state is different or is not already a terminal state (COMPLETED, FAILED, ARCHIVED)
                                    boolean needsUpdate = false;
                                    if (cascadedGateTask.getGateStatus() != cascadedGateStatus) {
                                        needsUpdate = true;
                                    }
                                    // Decide cascadedTaskWorkflowState for update
                                    String finalCascadedTaskState = cascadedGateTask.getState(); // Default to current
                                    if (originalIncomingStateFromRequest.equalsIgnoreCase(GateStatus.APPROVED.name())) { // Using GateStatus.APPROVED.name()
                                        if (canCascadedGateWorkflowProceed) {
                                            if (cascadedGateTask.getState().equalsIgnoreCase(State.PLANNED.name()) || cascadedGateTask.getState().equalsIgnoreCase(State.WAITING_FOR_APPROVAL.name())) {
                                                finalCascadedTaskState = State.QUEUED.name();
                                            } else {
                                                finalCascadedTaskState = cascadedGateTask.getState(); // Keep existing if IN_PROGRESS etc.
                                            }
                                        } else {
                                            finalCascadedTaskState = cascadedGateTask.getState(); // Keep existing state if not ready to proceed
                                        }
                                    } else if (originalIncomingStateFromRequest.equalsIgnoreCase(GateStatus.REJECTED.name())) { // Using GateStatus.REJECTED.name()
                                        if (!isTerminalState(cascadedGateTask.getState())) { // If not already in a terminal state
                                            finalCascadedTaskState = State.FAILED.name(); // Always fail if rejected
                                        } else {
                                            finalCascadedTaskState = cascadedGateTask.getState();
                                        }
                                    }
                                    
                                    if (!finalCascadedTaskState.equalsIgnoreCase(cascadedGateTask.getState())) {
                                        needsUpdate = true;
                                    }
                                    

                                    if (needsUpdate) {
                                        cascadedGateTask.setGateStatus(cascadedGateStatus);
                                        cascadedGateTask.setApprovedBy(approvedBy != null ? approvedBy : "SYSTEM_CASCADE");
                                        cascadedGateTask.setApprovalDate(LocalDateTime.now());
                                        cascadedGateTask.setReason(String.format("Cascaded from Release Group Gate %s (ID: %s).", gateTaskName, primaryTask.getId()));
                                        cascadedGateTask.setUpdatedAt(LocalDateTime.now());
                                        cascadedGateTask.setState(finalCascadedTaskState);
                                        
                                        taskRepository.save(cascadedGateTask); // Save the updated child task
                                    } else {
                                        System.out.println(String.format("Task %s gateStatus is already %s and state is %s. Skipping direct update from cascading approval.",
                                                cascadedGateTask.getId(), cascadedGateTask.getGateStatus(), cascadedGateTask.getState()));
                                    }
                                    
                                    // Break after finding one matching gate per phase/release if uniqueness is expected
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        
        return entity;
    }

    @Transactional
    public Task moveTask(String taskId, String newPreviousTaskId, String newNextTaskId) {
        Task taskToMove = taskRepository.findById(taskId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Task not found with ID: " + taskId));

        if (!isTaskModifiable(taskToMove)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                String.format("Task '%s' in state '%s' cannot be moved. Ensure it's PLANNED and its parents are not in a terminal state.",
                    taskId, taskToMove.getState()));
        }

        // Logic to update previous and next tasks' links
        // Remove old links
        if (taskToMove.getPreviousTaskId() != null) {
            taskRepository.findById(taskToMove.getPreviousTaskId()).ifPresent(prev -> {
                if (prev.getNextTaskId() != null && prev.getNextTaskId().equals(taskId)) {
                    prev.setNextTaskId(taskToMove.getNextTaskId());
                    taskRepository.save(prev);
                }
            });
        }
        if (taskToMove.getNextTaskId() != null) {
            taskRepository.findById(taskToMove.getNextTaskId()).ifPresent(next -> {
                if (next.getPreviousTaskId() != null && next.getPreviousTaskId().equals(taskId)) {
                    next.setPreviousTaskId(taskToMove.getPreviousTaskId());
                    taskRepository.save(next);
                }
            });
        }

        // Set new links for the task to move
        taskToMove.setPreviousTaskId(newPreviousTaskId);
        taskToMove.setNextTaskId(newNextTaskId);
        taskRepository.save(taskToMove);

        // Update new links' neighbors
        if (newPreviousTaskId != null) {
            taskRepository.findById(newPreviousTaskId).ifPresent(newPrev -> {
                newPrev.setNextTaskId(taskId);
                taskRepository.save(newPrev);
            });
        }
        if (newNextTaskId != null) {
            taskRepository.findById(newNextTaskId).ifPresent(newNext -> {
                newNext.setPreviousTaskId(taskId);
                taskRepository.save(newNext);
            });
        }

        return taskToMove;
    }

    @Transactional
    public Task updateTaskVariables(String taskId, Map<String, Object> variables) {
        Task task = taskRepository.findById(taskId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Task not found with ID: " + taskId));
        
        // Allow updating taskVariables even if task is IN_PROGRESS (e.g., by automation)
        // However, if the task is in a terminal state, prevent updates.
        if (isTerminalState(task.getState())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                String.format("Cannot update variables for task '%s' in terminal state '%s'.", taskId, task.getState()));
        }

        if (variables != null) {
            task.getTaskVariables().putAll(variables); // Merge new variables
            task.setUpdatedAt(LocalDateTime.now());
            taskRepository.save(task);
        }
        return task;
    }

    @Transactional
    public Task appendTaskLogs(String taskId, String newLogContent) {
        Task task = taskRepository.findById(taskId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Task not found with ID: " + taskId));
        
        // Allow appending logs even if task is IN_PROGRESS or FAILED.
        // Prevent if COMPLETED or ARCHIVED, etc. (unless specifically needed for post-mortem)
        if (isTerminalState(task.getState()) && !task.getState().equals(State.FAILED.name())) { // Allow logs for FAILED tasks
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                String.format("Cannot append logs for task '%s' in terminal state '%s'.", taskId, task.getState()));
        }

        String timestamp = LocalDateTime.now().toString();
        String logEntry = String.format("\n--- Log Entry [%s] ---\n%s", timestamp, newLogContent);
        task.setLogs(task.getLogs() + logEntry);
        task.setUpdatedAt(LocalDateTime.now());
        taskRepository.save(task);
        return task;
    }
}
