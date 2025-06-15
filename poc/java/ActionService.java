// src/main/java/com/reflow/services/ActionService.java
package com.reflow.services;

import com.reflow.models.*;
import com.reflow.models.Phase.ParentType;
import com.reflow.models.Task.GateStatus; // Import Task.GateStatus
import com.reflow.models.Task.TaskType; // Import Task.TaskType
import com.reflow.repositories.*;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.HashMap;

@Service
@RequiredArgsConstructor
public class ActionService {

    private final ReleaseGroupRepository releaseGroupRepository;
    private final ReleaseRepository releaseRepository;
    private final PhaseRepository phaseRepository;
    private final TaskRepository taskRepository;
    private final KafkaProducerService kafkaProducerService; // Still inject for other events

    // Defines states considered "modifiable" for an entity.
    private static final List<String> MODIFIABLE_STATES = Arrays.asList(
            State.PLANNED.name()
    );

    // Defines states considered "terminal" for workflow progression.
    private static final List<String> TERMINAL_STATES = Arrays.asList(
            State.COMPLETED.name(), State.SKIPPED.name(), State.REJECTED.name(),
            State.ARCHIVED.name(), State.FAILED.name(), State.APPROVED.name()
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
    public BaseEntity transitionState(String entityType, String entityId, String newStateStr, String approvedBy, String reason) {
        State newState = State.valueOf(newStateStr.toUpperCase()); // Convert string to enum

        BaseEntity entity;
        Optional<? extends BaseEntity> optionalEntity;

        // Fetch the entity based on type
        switch (entityType.toUpperCase()) {
            case "RELEASEGROUP":
                optionalEntity = releaseGroupRepository.findById(entityId);
                break;
            case "RELEASE":
                optionalEntity = releaseRepository.findById(entityId);
                break;
            case "PHASE":
                optionalEntity = phaseRepository.findById(entityId);
                break;
            case "TASK":
                optionalEntity = taskRepository.findById(entityId);
                break;
            default:
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid entityType provided.");
        }

        entity = optionalEntity.orElseThrow(() ->
                new ResponseStatusException(HttpStatus.NOT_FOUND, entityType + " with ID " + entityId + " not found."));

        String oldStateStr = entity.getState(); // Get the current state as string
        State oldState = State.valueOf(oldStateStr); // Convert to enum

        // --- Perform State Transition Validation ---
        // Validates the requested transition based on predefined rules.
        validateStateTransition(entityType, oldState, newState);

        // Update the entity's state
        entity.setState(newState.name());
        entity.setUpdatedAt(LocalDateTime.now()); // Ensure updatedAt is set

        BaseEntity savedEntity = null; // To hold the saved entity

        if (entity instanceof Task task) {
            if (newState == State.APPROVED || newState == State.REJECTED) {
                task.setApprovedBy(approvedBy);
                task.setApprovalDate(LocalDateTime.now());
                task.setReason(reason);
                // If it's a gate task, update its gate status as well
                if (task.isGate()) {
                    task.setGateStatus(GateStatus.valueOf(newState.name()));
                }
            } else if (task.isGate() && newState == State.BLOCKED) {
                // If a gate task is blocked, its gate status might need to be reset or set to a relevant state
                task.setGateStatus(null);
            }
            savedEntity = taskRepository.save(task); // Save specific task changes
            
            // --- Direct DB Update for Cascading Approval of Release Group Gates ---
            // If this is a gate task and its parent phase is for a Release Group,
            // and it's being APPROVED or REJECTED, then directly update child tasks.
            if (task.isGate() && (newState == State.APPROVED || newState == State.REJECTED)) {
                Optional<Phase> parentPhaseOpt = phaseRepository.findById(task.getPhaseId());
                if (parentPhaseOpt.isPresent()) {
                    Phase parentPhase = parentPhaseOpt.get();
                    if (parentPhase.getParentType() == ParentType.RELEASE_GROUP) {
                        String releaseGroupId = parentPhase.getParentId();
                        String gateTaskName = task.getName(); // Use task name for matching cascaded gates

                        System.out.println(String.format("Gate task %s (ID: %s) under ReleaseGroup %s is %s. Initiating direct cascading approval.",
                                gateTaskName, task.getId(), releaseGroupId, newState.name()));

                        // Find all releases belonging to this Release Group
                        List<Release> releasesInGroup = releaseRepository.findByReleaseGroupId(releaseGroupId);

                        for (Release rel : releasesInGroup) {
                            // Find phases within this release
                            List<Phase> releasePhases = phaseRepository.findByParentIdAndParentType(rel.getId(), ParentType.RELEASE);
                            for (Phase rp : releasePhases) {
                                // Find matching gate tasks within this release's phase by name and isGate=true
                                List<Task> matchingGateTasks = taskRepository.findByPhaseIdAndIsGateIsTrueAndName(rp.getId(), gateTaskName);
                                for (Task cascadedGateTask : matchingGateTasks) {
                                    System.out.println(String.format("Directly updating cascaded gate task in Release %s, Phase %s, Task %s to %s.",
                                            rel.getId(), rp.getId(), cascadedGateTask.getId(), newState.name()));
                                    
                                    // Set the state, gateStatus, and approval details directly
                                    cascadedGateTask.setState(newState.name()); // New state for the cascaded task
                                    cascadedGateTask.setGateStatus(GateStatus.valueOf(newState.name())); // Set gateStatus
                                    cascadedGateTask.setActor("SYSTEM_CASCADER"); // Indicate system action
                                    cascadedGateTask.setApprovedBy(approvedBy != null ? approvedBy : "SYSTEM_CASCADER");
                                    cascadedGateTask.setApprovalDate(LocalDateTime.now());
                                    cascadedGateTask.setReason(String.format("Cascaded from Release Group Gate %s (ID: %s).", gateTaskName, task.getId()));
                                    cascadedGateTask.setUpdatedAt(LocalDateTime.now()); // Update timestamp
                                    
                                    taskRepository.save(cascadedGateTask); // Save the directly updated task
                                    
                                    // Publish a Kafka event for the cascaded task's state change
                                    // This event is for *observability*, not for driving the cascade logic.
                                    kafkaProducerService.sendTaskEvent(cascadedGateTask.getId(), Map.of(
                                        "id", cascadedGateTask.getId(),
                                        "name", cascadedGateTask.getName(),
                                        "description", cascadedGateTask.getDescription(),
                                        "state", cascadedGateTask.getState(),
                                        "type", cascadedGateTask.getType().name(),
                                        "phaseId", cascadedGateTask.getPhaseId(),
                                        "releaseId", cascadedGateTask.getReleaseId(),
                                        "isGate", true,
                                        "gateStatus", cascadedGateTask.getGateStatus().name(),
                                        "actor", cascadedGateTask.getActor(),
                                        "approvedBy", cascadedGateTask.getApprovedBy(),
                                        "approvalDate", cascadedGateTask.getApprovalDate().toString(),
                                        "reason", cascadedGateTask.getReason()
                                    ));

                                    // Break after finding one matching gate per phase/release if uniqueness is expected
                                    break;
                                }
                            }
                        }
                    }
                }
            }

        } else if (entity instanceof Phase phase) {
            savedEntity = phaseRepository.save(phase);

            // Logic for Phase completion to trigger next phase
            if (newState == State.COMPLETED) {
                System.out.println(String.format("Phase %s (ID: %s) COMPLETED. Checking for next phase to queue...", phase.getName(), phase.getId()));
                String nextPhaseId = phase.getNextPhaseId();
                if (nextPhaseId != null && !nextPhaseId.trim().isEmpty()) {
                    Optional<Phase> nextPhaseOpt = phaseRepository.findById(nextPhaseId);
                    if (nextPhaseOpt.isPresent()) {
                        Phase nextPhase = nextPhaseOpt.get();
                        // Only transition next phase if it's currently PLANNED or BLOCKED
                        if (nextPhase.getState().equals(State.PLANNED.name()) || nextPhase.getState().equals(State.BLOCKED.name())) {
                            System.out.println(String.format("Transitioning next phase %s (ID: %s) to IN_PROGRESS.", nextPhase.getName(), nextPhase.getId()));
                            // Use recursive call to transitionState for the next phase
                            transitionState(
                                "Phase",
                                nextPhase.getId(),
                                State.IN_PROGRESS.name(),
                                "SYSTEM_ORCHESTRATOR", // Actor for this transition
                                String.format("Auto-transitioned by completion of previous phase %s (ID: %s).", phase.getName(), phase.getId())
                            );

                            // Find the first task of the next phase and queue it
                            List<Task> firstTasks = taskRepository.findByPhaseIdAndPreviousTaskIdIsNullAndGroupIdIsNull(nextPhase.getId());
                            if (!firstTasks.isEmpty()) {
                                Task firstTaskOfNextPhase = firstTasks.get(0); // Assuming one true starting task
                                // Only queue if it's PLANNED or BLOCKED
                                if (firstTaskOfNextPhase.getState().equals(State.PLANNED.name()) || firstTaskOfNextPhase.getState().equals(State.BLOCKED.name())) {
                                    System.out.println(String.format("Queuing first task %s (ID: %s) of phase %s (ID: %s).",
                                        firstTaskOfNextPhase.getName(), firstTaskOfNextPhase.getId(), nextPhase.getName(), nextPhase.getId()));
                                    transitionState(
                                        "Task",
                                        firstTaskOfNextPhase.getId(),
                                        State.QUEUED.name(),
                                        "SYSTEM_ORCHESTRATOR", // Actor for this transition
                                        String.format("Auto-queued as first task of phase %s (ID: %s) transitioned to IN_PROGRESS.", nextPhase.getName(), nextPhase.getId())
                                    );
                                } else {
                                    System.out.println(String.format("First task %s (ID: %s) of phase %s is in state %s, not queuing.",
                                        firstTaskOfNextPhase.getName(), firstTaskOfNextPhase.getId(), nextPhase.getName(), nextPhase.getId(), firstTaskOfNextPhase.getState()));
                                }
                            } else {
                                System.out.println(String.format("No starting task found for next phase %s (ID: %s).", nextPhase.getName(), nextPhase.getId()));
                            }
                        } else {
                            System.out.println(String.format("Next phase %s (ID: %s) is in state %s, not auto-transitioning.", nextPhase.getName(), nextPhase.getId(), nextPhase.getState()));
                        }
                    } else {
                        System.out.println(String.format("Next phase with ID %s not found for phase %s (ID: %s).", nextPhaseId, phase.getName(), phase.getId()));
                    }
                } else {
                    System.out.println(String.format("Phase %s (ID: %s) has no next phase configured.", phase.getName(), phase.getId()));
                }
            }

        } else if (entity instanceof Release release) {
            savedEntity = releaseRepository.save(release);
        } else if (entity instanceof ReleaseGroup releaseGroup) {
            savedEntity = releaseGroupRepository.save(releaseGroup);
        } else {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Unknown entity type encountered during save.");
        }
        
        // Publish generic entity state change event for all entities (for observability)
        // Ensure that the entity is saved and has an ID before publishing
        if (savedEntity != null && savedEntity.getId() != null) {
            Map<String, Object> kafkaEventData = new HashMap<>();
            kafkaEventData.put("id", savedEntity.getId());
            kafkaEventData.put("entityType", savedEntity.getClass().getSimpleName());
            kafkaEventData.put("oldState", oldStateStr);
            kafkaEventData.put("newState", savedEntity.getState());
            // Add other relevant fields for the event if necessary
            if (savedEntity instanceof Task) {
                Task task = (Task) savedEntity;
                kafkaEventData.put("type", task.getType().name());
                kafkaEventData.put("phaseId", task.getPhaseId());
                kafkaEventData.put("releaseId", task.getReleaseId());
                kafkaEventData.put("isGate", task.isGate());
                if (task.getApprovedBy() != null) kafkaEventData.put("approvedBy", task.getApprovedBy());
                if (task.getReason() != null) kafkaEventData.put("reason", task.getReason());
            } else if (savedEntity instanceof Phase) {
                Phase phase = (Phase) savedEntity;
                kafkaEventData.put("parentId", phase.getParentId());
                kafkaEventData.put("parentType", phase.getParentType().name());
            } else if (savedEntity instanceof Release) {
                 Release release = (Release) savedEntity;
                 if (release.getReleaseGroupId() != null) kafkaEventData.put("releaseGroupId", release.getReleaseGroupId());
            }

            kafkaProducerService.sendEntityStateChangeEvent(savedEntity.getId(), kafkaEventData);
        }

        // Publish specific entity events (Task, Phase, Release, ReleaseGroup)
        // These can be more detailed or consumed by specific services (still via Kafka)
        if (savedEntity instanceof Task task) {
            kafkaProducerService.sendTaskEvent(task.getId(), Map.of(
                "id", task.getId(),
                "name", task.getName(),
                "state", task.getState(),
                "type", task.getType().name(),
                "phaseId", task.getPhaseId(),
                "releaseId", task.getReleaseId()
                // ... potentially more fields
            ));
        }
        // Add more specific event publications if needed for Phase, Release, ReleaseGroup
        
        return savedEntity;
    }

    /**
     * Validates if a state transition from oldState to newState is allowed for a given entity type.
     * This method enforces the business rules for state changes.
     *
     * @param entityType The type of the entity (e.g., "TASK", "PHASE").
     * @param oldState The current state of the entity.
     * @param newState The desired new state for the entity.
     * @throws ResponseStatusException if the transition is not allowed.
     */
    private void validateStateTransition(String entityType, State oldState, State newState) {
        // No actual change, so always allowed
        if (oldState == newState) {
            return;
        }

        // Universal rule: Cannot transition from any terminal state except to ARCHIVED
        if (isTerminalState(oldState.name())) {
            if (newState == State.ARCHIVED) {
                return; // Allow archiving from other terminal states
            } else {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                        String.format("Invalid transition from terminal state %s to %s for %s.", oldState.name(), newState.name(), entityType));
            }
        }
        
        // Specific transition rules based on oldState and entityType
        switch (entityType.toUpperCase()) {
            case "RELEASEGROUP":
            case "RELEASE":
            case "PHASE":
                // All these entity types share the same basic lifecycle
                validateCommonLifecycleEntityTransition(oldState, newState, entityType);
                break;
            case "TASK":
                validateTaskTransition(oldState, newState);
                break;
            default:
                throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Internal error: Unhandled entity type for state validation: " + entityType);
        }
    }

    /**
     * Helper method to validate state transitions for entities with a common lifecycle:
     * ReleaseGroup, Release, and Phase.
     * @param oldState The current state.
     * @param newState The proposed new state.
     */
    private void validateCommonLifecycleEntityTransition(State oldState, State newState, String entityType) {
        switch (oldState) {
            case PLANNED:
                if (List.of(State.IN_PROGRESS, State.ARCHIVED).contains(newState)) return;
                break;
            case IN_PROGRESS:
                if (List.of(State.COMPLETED, State.BLOCKED, State.REJECTED, State.ARCHIVED).contains(newState)) return;
                break;
            case BLOCKED:
                if (List.of(State.IN_PROGRESS, State.REJECTED, State.ARCHIVED).contains(newState)) return;
                break;
            // No default here; if it falls through, it's an invalid transition handled by the final throw
        }
        throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                String.format("Invalid state transition for %s from %s to %s.", entityType, oldState.name(), newState.name()));
    }

    /**
     * Helper method to validate state transitions specifically for Task entities.
     * @param oldState The current state.
     * @param newState The proposed new state.
     */
    private void validateTaskTransition(State oldState, State newState) {
        switch (oldState) {
            case PLANNED: if (List.of(State.QUEUED, State.IN_PROGRESS, State.SCHEDULED, State.SKIPPED, State.REJECTED, State.ARCHIVED).contains(newState)) return; break;
            case QUEUED: if (List.of(State.IN_PROGRESS, State.FAILED, State.SKIPPED, State.REJECTED, State.ARCHIVED).contains(newState)) return; break;
            case IN_PROGRESS: if (List.of(State.COMPLETED, State.FAILED, State.BLOCKED, State.REJECTED, State.ARCHIVED).contains(newState)) return; break;
            case FAILED: if (List.of(State.RETRY, State.BLOCKED, State.SKIPPED, State.REJECTED, State.ARCHIVED).contains(newState)) return; break;
            case BLOCKED: if (List.of(State.QUEUED, State.IN_PROGRESS, State.RETRY, State.REJECTED, State.ARCHIVED).contains(newState)) return; break;
            case WAITING_FOR_APPROVAL: if (List.of(State.APPROVED, State.REJECTED, State.SKIPPED, State.ARCHIVED).contains(newState)) return; break;
            case APPROVED: if (List.of(State.COMPLETED, State.SKIPPED, State.REJECTED, State.ARCHIVED).contains(newState)) return; break;
            case SCHEDULED: if (List.of(State.QUEUED, State.IN_PROGRESS, State.FAILED, State.SKIPPED, State.REJECTED, State.ARCHIVED).contains(newState)) return; break;
            case RETRY: if (List.of(State.QUEUED, State.IN_PROGRESS, State.SKIPPED, State.REJECTED, State.ARCHIVED).contains(newState)) return; break;
            // No default here; if it falls through, it's an invalid transition handled by the final throw
        }
        throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                String.format("Invalid state transition for Task from %s to %s.", oldState.name(), newState.name()));
    }


    /**
     * Moves a task within a phase or sequential group by re-linking its previous and next tasks.
     * This method can be used to reorder tasks.
     *
     * @param taskIdToMove The ID of the task to be moved.
     * @param newPreviousTaskId The ID of the task that will precede taskIdToMove. Null for a new starting task.
     * @param newNextTaskId The ID of the task that will follow taskIdToMove. Null for moving to the end.
     * @return The updated Task object.
     * @throws ResponseStatusException if the move is invalid (e.g., task not found, in invalid state, or creating cycles).
     */
    @Transactional
    public Task moveTask(String taskIdToMove, String newPreviousTaskId, String newNextTaskId) {
        Task taskToMove = taskRepository.findById(taskIdToMove)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Task to move not found with ID: " + taskIdToMove));

        // Enforce modifiability: Only PLANNED tasks can be moved and their parent entities must not be terminal
        if (!isTaskModifiable(taskToMove)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                String.format("Task '%s' in state '%s' cannot be moved. Ensure it's PLANNED and its parents are not in a terminal state.",
                    taskIdToMove, taskToMove.getState()));
        }

        // Validate newPreviousTaskId and newNextTaskId existence (if provided)
        Task newPreviousTask = null;
        if (newPreviousTaskId != null && !newPreviousTaskId.trim().isEmpty()) {
            newPreviousTask = taskRepository.findById(newPreviousTaskId)
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "New previous task not found with ID: " + newPreviousTaskId));
            // Ensure newPreviousTask is in the same phase/group as taskToMove
            if (!newPreviousTask.getPhaseId().equals(taskToMove.getPhaseId()) ||
                !Optional.ofNullable(newPreviousTask.getGroupId()).equals(Optional.ofNullable(taskToMove.getGroupId()))) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "New previous task must be in the same phase/group as the task to move.");
            }
            // Ensure newPreviousTask is not the same as taskToMove
            if (newPreviousTask.getId().equals(taskIdToMove)) {
                 throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "New previous task cannot be the task to move itself.");
            }
        }

        Task newNextTask = null;
        if (newNextTaskId != null && !newNextTaskId.trim().isEmpty()) {
            newNextTask = taskRepository.findById(newNextTaskId)
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "New next task not found with ID: " + newNextTaskId));
            // Ensure newNextTask is in the same phase/group as taskToMove
            if (!newNextTask.getPhaseId().equals(taskToMove.getPhaseId()) ||
                !Optional.ofNullable(newNextTask.getGroupId()).equals(Optional.ofNullable(taskToMove.getGroupId()))) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "New next task must be in the same phase/group as the task to move.");
            }
            // Ensure newNextTask is not the same as taskToMove
            if (newNextTask.getId().equals(taskIdToMove)) {
                 throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "New next task cannot be the task to move itself.");
            }
            // Ensure newPreviousTask and newNextTask don't create a cycle with taskToMove
            if (newPreviousTask != null && newPreviousTask.getId().equals(newNextTaskId)) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Cannot move task between itself and its proposed next task (forms a cycle).");
            }
        }


        // 1. Break existing links for taskToMove
        Task oldPreviousTask = null;
        if (taskToMove.getPreviousTaskId() != null) {
            oldPreviousTask = taskRepository.findById(taskToMove.getPreviousTaskId()).orElse(null);
            if (oldPreviousTask != null) {
                oldPreviousTask.setNextTaskId(taskToMove.getNextTaskId()); // oldPrevious now points to oldNext
                taskRepository.save(oldPreviousTask);
            }
        }
        
        Task oldNextTask = null;
        if (taskToMove.getNextTaskId() != null) {
            oldNextTask = taskRepository.findById(taskToMove.getNextTaskId()).orElse(null);
            if (oldNextTask != null) {
                oldNextTask.setPreviousTaskId(taskToMove.getPreviousTaskId()); // oldNext now points to oldPrevious
                taskRepository.save(oldNextTask);
            }
        }

        // 2. Establish new links for taskToMove
        taskToMove.setPreviousTaskId(newPreviousTaskId);
        taskToMove.setNextTaskId(newNextTaskId);
        taskRepository.save(taskToMove);

        // 3. Update tasks that now link to taskToMove
        if (newPreviousTask != null) {
            newPreviousTask.setNextTaskId(taskIdToMove);
            taskRepository.save(newPreviousTask);
        } else {
            // If newPreviousTaskId is null, and taskToMove is now the new start of its sequence,
            // we must ensure no other task in its sequence currently has previousTaskId=null.
            // This is handled by the creation service, but for a move, we explicitly check here.
            // Find current starting task in the phase/group if newPreviousTaskId is null
            List<Task> currentStartingTasks;
            if (taskToMove.getGroupId() == null || taskToMove.getGroupId().trim().isEmpty()) {
                currentStartingTasks = taskRepository.findByPhaseIdAndPreviousTaskIdIsNullAndGroupIdIsNull(taskToMove.getPhaseId());
            } else {
                Task parentGroupTask = taskRepository.findById(taskToMove.getGroupId())
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Parent group task not found with ID: " + taskToMove.getGroupId()));
                if (parentGroupTask.getType() == TaskType.SEQUENTIAL_GROUP) {
                    currentStartingTasks = taskRepository.findByGroupIdAndPreviousTaskIdIsNull(taskToMove.getGroupId());
                } else {
                    currentStartingTasks = List.of(); // Parallel group, multiple starts allowed
                }
            }

            for (Task startingTask : currentStartingTasks) {
                if (!startingTask.getId().equals(taskIdToMove)) {
                    // This scenario means we are trying to make taskToMove the new starting task,
                    // but another task is also a starting task. This could imply a broken sequence
                    // or an invalid move. For now, we disallow this unless the user explicitly wants
                    // to break a chain by moving a task to the very beginning.
                    // If taskToMove becomes the new start, any other task with previousTaskId=null
                    // should either be linked to (if it's the old start) or an error.
                    // This is complex. For now, relying on the 'break old links' to handle this implicitly.
                    // The 'createTask' handles ensuring only one starting task at creation.
                    // If a task is moved to `newPreviousTaskId = null`, and there's another
                    // task with `previousTaskId = null`, it means a split chain.
                    // For now, allow this as long as no cycles are created.
                    System.out.println("Warning: After moving " + taskIdToMove + " to be a starting task, " + startingTask.getId() + " remains a starting task.");
                }
            }
        }
        
        if (newNextTask != null) {
            newNextTask.setPreviousTaskId(taskIdToMove);
            taskRepository.save(newNextTask);
        }

        // Re-fetch the task to ensure all fields are up-to-date after transactional saves
        return taskRepository.findById(taskIdToMove).orElseThrow(() -> new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to retrieve updated task after move."));
    }

    /**
     * Updates specific fields within the taskVariables map for a given task.
     * This is useful for dynamically storing runtime context for a task.
     * @param taskId The ID of the task to update.
     * @param updates A map of key-value pairs to set or update within taskVariables.
     * @return The updated Task object.
     * @throws ResponseStatusException if the task is not found or is in a terminal state.
     */
    @Transactional
    public Task updateTaskVariables(String taskId, Map<String, Object> updates) {
        Task task = taskRepository.findById(taskId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Task not found with ID: " + taskId));

        // Allow updates if the task is NOT in a TERMINAL_STATES.
        if (isTerminalState(task.getState())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                String.format("Cannot update variables for task '%s' in terminal state '%s'.",
                    taskId, task.getState()));
        }

        // Ensure taskVariables map exists
        if (task.getTaskVariables() == null) {
            task.setTaskVariables(new HashMap<>());
        }
        
        // Apply updates to the taskVariables map
        task.getTaskVariables().putAll(updates);
        task.setUpdatedAt(LocalDateTime.now()); // Update timestamp

        Task updatedTask = taskRepository.save(task);
        return updatedTask;
    }

    /**
     * Appends new log content to the existing logs field of a task.
     * This is useful for capturing stdout/stderr from external executions.
     * @param taskId The ID of the task to update.
     * @param newLogContent The new content to append.
     * @return The updated Task object.
     * @throws ResponseStatusException if the task is not found.
     */
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

        String timestamp = LocalDateTime.now().format(java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        String logEntry = String.format("\n--- Log Entry [%s] ---\n%s", timestamp, newLogContent);
        task.setLogs(task.getLogs() + logEntry);
        task.setUpdatedAt(LocalDateTime.now()); // Update timestamp
        taskRepository.save(task);
        return task;
    }


    /**
     * Starts a Release Group workflow.
     * Transitions the Release Group to IN_PROGRESS.
     * Iterates through all child Releases and directly calls startRelease on them.
     * If the Release Group has direct phases (not under child releases), it will
     * start the first direct phase and its first task.
     *
     * @param releaseGroupId The ID of the Release Group to start.
     * @return The updated ReleaseGroup entity.
     * @throws ResponseStatusException if the Release Group is not found or cannot be started.
     */
    @Transactional
    public ReleaseGroup startReleaseGroup(String releaseGroupId) {
        ReleaseGroup releaseGroup = releaseGroupRepository.findById(releaseGroupId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Release Group not found with ID: " + releaseGroupId));

        // Only allow starting if in PLANNED state
        if (releaseGroup.getState() != State.PLANNED.name()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, String.format("Cannot start Release Group %s. Current state is %s, but must be PLANNED.", releaseGroupId, releaseGroup.getState()));
        }

        // 1. Transition Release Group state to IN_PROGRESS
        transitionState("ReleaseGroup", releaseGroupId, State.IN_PROGRESS.name(), "SYSTEM_INITIATOR", "Release Group initiated.");
        System.out.println(String.format("Release Group %s transitioned to IN_PROGRESS.", releaseGroupId));

        // 2. Start all child Releases
        if (releaseGroup.getReleaseIds() != null && !releaseGroup.getReleaseIds().isEmpty()) {
            System.out.println(String.format("Starting %d child releases for Release Group %s...", releaseGroup.getReleaseIds().size(), releaseGroupId));
            for (String releaseId : releaseGroup.getReleaseIds()) {
                try {
                    startRelease(releaseId); // Recursively call startRelease
                } catch (ResponseStatusException e) {
                    System.err.println(String.format("Warning: Could not start child Release %s for Release Group %s: %s", releaseId, releaseGroupId, e.getReason()));
                    // You might want to block the Release Group here or log a critical error
                }
            }
        } else {
            System.out.println(String.format("Release Group %s has no child releases.", releaseGroupId));
        }

        // 3. Start direct phases of the Release Group (if any)
        if (releaseGroup.getPhaseIds() != null && !releaseGroup.getPhaseIds().isEmpty()) {
            // Find the first direct phase (one with previousPhaseId = null)
            Optional<Phase> firstDirectPhaseOpt = phaseRepository.findByParentIdAndParentTypeAndPreviousPhaseIdIsNull(releaseGroupId, ParentType.RELEASE_GROUP);
            if (firstDirectPhaseOpt.isPresent()) {
                Phase firstDirectPhase = firstDirectPhaseOpt.get();
                System.out.println(String.format("Starting first direct Phase %s for Release Group %s...", firstDirectPhase.getId(), releaseGroupId));
                // Transition phase to IN_PROGRESS
                transitionState(
                    "Phase",
                    firstDirectPhase.getId(),
                    State.IN_PROGRESS.name(),
                    "SYSTEM_INITIATOR",
                    "Direct phase initiated by Release Group start."
                );

                // Find and queue the first task of this direct phase
                List<Task> firstTasks = taskRepository.findByPhaseIdAndPreviousTaskIdIsNullAndGroupIdIsNull(firstDirectPhase.getId());
                if (!firstTasks.isEmpty()) {
                    Task firstTaskOfDirectPhase = firstTasks.get(0);
                    transitionState(
                        "Task",
                        firstTaskOfDirectPhase.getId(),
                        State.QUEUED.name(),
                        "SYSTEM_INITIATOR",
                        "First task of direct phase queued by Release Group start."
                    );
                    System.out.println(String.format("Queued first task %s of direct Phase %s for Release Group %s.", firstTaskOfDirectPhase.getId(), firstDirectPhase.getId(), releaseGroupId));
                } else {
                    System.out.println(String.format("Direct Phase %s has no starting tasks.", firstDirectPhase.getId()));
                }
            } else {
                System.out.println(String.format("Release Group %s has no direct starting phases.", releaseGroupId));
            }
        } else {
            System.out.println(String.format("Release Group %s has no direct phases configured.", releaseGroupId));
        }
        
        return releaseGroupRepository.findById(releaseGroupId).get(); // Re-fetch to return latest state
    }

    /**
     * Starts a Release workflow.
     * Transitions the Release to IN_PROGRESS.
     * Finds the first Phase in the Release and transitions it to IN_PROGRESS.
     * Finds the first Task in that Phase and transitions it to QUEUED.
     *
     * @param releaseId The ID of the Release to start.
     * @return The updated Release entity.
     * @throws ResponseStatusException if the Release is not found or cannot be started.
     */
    @Transactional
    public Release startRelease(String releaseId) {
        Release release = releaseRepository.findById(releaseId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Release not found with ID: " + releaseId));

        // Only allow starting if in PLANNED state
        if (release.getState() != State.PLANNED.name()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, String.format("Cannot start Release %s. Current state is %s, but must be PLANNED.", releaseId, release.getState()));
        }

        // 1. Transition Release state to IN_PROGRESS
        transitionState("Release", releaseId, State.IN_PROGRESS.name(), "SYSTEM_INITIATOR", "Release initiated.");
        System.out.println(String.format("Release %s transitioned to IN_PROGRESS.", releaseId));

        // 2. Find and start the first Phase in this Release
        // The "first" phase is defined as the one with previousPhaseId = null for this parent.
        Optional<Phase> firstPhaseOpt = phaseRepository.findByParentIdAndParentTypeAndPreviousPhaseIdIsNull(releaseId, ParentType.RELEASE);

        if (firstPhaseOpt.isPresent()) {
            Phase firstPhase = firstPhaseOpt.get();
            System.out.println(String.format("Starting first Phase %s for Release %s...", firstPhase.getId(), releaseId));
            // Transition phase to IN_PROGRESS
            transitionState(
                "Phase",
                firstPhase.getId(),
                State.IN_PROGRESS.name(),
                "SYSTEM_INITIATOR",
                "First phase initiated by Release start."
            );

            // 3. Find and queue the first Task of the first Phase
            // The "first" task is defined as the one with previousTaskId = null AND groupId = null
            List<Task> firstTasks = taskRepository.findByPhaseIdAndPreviousTaskIdIsNullAndGroupIdIsNull(firstPhase.getId());

            if (!firstTasks.isEmpty()) {
                Task firstTask = firstTasks.get(0); // Assuming there's only one true starting task
                System.out.println(String.format("Queuing first Task %s of Phase %s for Release %s...", firstTask.getId(), firstPhase.getId(), releaseId));
                // Transition task to QUEUED
                transitionState(
                    "Task",
                    firstTask.getId(),
                    State.QUEUED.name(),
                    "SYSTEM_INITIATOR",
                    "First task of first phase queued by Release start."
                );
            } else {
                System.out.println(String.format("Phase %s has no starting tasks. No tasks queued.", firstPhase.getId()));
            }
        } else {
            System.out.println(String.format("Release %s has no starting phases. No phases or tasks initiated.", releaseId));
        }

        return releaseRepository.findById(releaseId).get(); // Re-fetch to return latest state
    }


    // Helper methods for fetching entities - will be needed by Airflow for complex logic
    public Optional<Task> findTaskById(String taskId) {
        return taskRepository.findById(taskId);
    }

    public Optional<Phase> findPhaseById(String phaseId) {
        return phaseRepository.findById(phaseId);
    }

    public Optional<Release> findReleaseById(String releaseId) {
        return releaseRepository.findById(releaseId);
    }

    public Optional<ReleaseGroup> findReleaseGroupById(String releaseGroupId) {
        return releaseGroupRepository.findById(releaseGroupId);
    }

    // Helper method to find releases by release group ID (for cascading)
    public List<Release> findReleasesByReleaseGroupId(String releaseGroupId) {
        return releaseRepository.findByReleaseGroupId(releaseGroupId);
    }

    // Helper method to find phases by parent ID, parent type (for Airflow to find phases of a Release)
    public List<Phase> findPhasesByParentIdAndParentType(String parentId, ParentType parentType) {
        return phaseRepository.findByParentIdAndParentType(parentId, parentType);
    }

    // New helper method for Airflow to find a specific gate task within a phase by its name
    public List<Task> findGateTasksByPhaseIdAndName(String phaseId, String name) {
        return taskRepository.findByPhaseIdAndIsGateIsTrueAndName(phaseId, name);
    }

    // New helper method to find a gate task within a phase by its gateCategory and name (for cascading)
    public List<Task> findGateTasksByPhaseIdAndGateCategoryAndName(String phaseId, String gateCategory, String name) {
        return taskRepository.findByPhaseIdAndIsGateIsTrueAndGateCategoryAndName(phaseId, gateCategory, name);
    }
}
