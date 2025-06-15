// src/main/java/com/reflow/services/CustomIdGenerator.java
package com.reflow.services;

import org.springframework.stereotype.Service;

import java.util.UUID;

// A simple ID generator service. In a real application, consider a more robust, distributed ID generation strategy.
@Service
public class CustomIdGenerator {

    /**
     * Generates a custom ID with a prefix and an optional context ID.
     * Format: PREFIX_CONTEXTID_UUID
     * If contextId is null or empty, format is PREFIX_UUID.
     *
     * @param prefix The prefix for the ID (e.g., "RG", "R", "P", "T").
     * @param contextId An optional ID of a parent entity for context (e.g., releaseGroupId, phaseId).
     * @return A generated ID string.
     */
    public String generateId(String prefix, String contextId) {
        if (contextId != null && !contextId.trim().isEmpty()) {
            // Replace any invalid characters in contextId for safety in ID concatenation
            String sanitizedContextId = contextId.replaceAll("[^a-zA-Z0-9-_]", "");
            return String.format("%s_%s_%s", prefix, sanitizedContextId, UUID.randomUUID().toString().substring(0, 8)); // Use substring for shorter ID
        } else {
            return String.format("%s_%s", prefix, UUID.randomUUID().toString().substring(0, 8));
        }
    }

    /**
     * Overload for generating IDs without a context.
     * @param prefix The prefix for the ID.
     * @return A generated ID string.
     */
    public String generateId(String prefix) {
        return generateId(prefix, null);
    }
}


// src/main/java/com/reflow/services/ReleaseGroupCreationService.java
package com.reflow.services;

import com.reflow.models.ReleaseContext;
import com.reflow.models.ReleaseContext.ContextEntityType;
import com.reflow.models.ReleaseGroup;
import com.reflow.repositories.ReleaseContextRepository;
import com.reflow.repositories.ReleaseGroupRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.util.HashMap;

@Service
@RequiredArgsConstructor
public class ReleaseGroupCreationService {

    private final ReleaseGroupRepository releaseGroupRepository;
    private final ReleaseContextRepository releaseContextRepository;
    private final CustomIdGenerator idGenerator;

    @Transactional
    public ReleaseGroup createReleaseGroup(ReleaseGroup releaseGroup) {
        if (releaseGroup.getName() == null || releaseGroup.getName().trim().isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Release Group name is required.");
        }

        String releaseGroupId = idGenerator.generateId("RG");
        releaseGroup.setId(releaseGroupId);

        // Create an initial ReleaseContext for the ReleaseGroup
        ReleaseContext releaseContext = new ReleaseContext();
        releaseContext.setId(releaseGroupId); // Context ID matches ReleaseGroup ID
        releaseContext.setEntityType(ContextEntityType.RELEASE_GROUP);
        // Initialize globalShared and environments maps
        releaseContext.setGlobalShared(new HashMap<>());
        releaseContext.setEnvironments(new HashMap<>());
        releaseContextRepository.save(releaseContext);

        releaseGroup.setReleaseContextId(releaseContext.getId()); // Link ReleaseGroup to its context

        ReleaseGroup savedReleaseGroup = releaseGroupRepository.save(releaseGroup);
        return savedReleaseGroup;
    }
}


// src/main/java/com/reflow/services/ReleaseCreationService.java
package com.reflow.services;

import com.reflow.models.Release;
import com.reflow.models.ReleaseContext;
import com.reflow.models.ReleaseContext.ContextEntityType;
import com.reflow.models.ReleaseGroup;
import com.reflow.models.State;
import com.reflow.repositories.ReleaseContextRepository;
import com.reflow.repositories.ReleaseGroupRepository;
import com.reflow.repositories.ReleaseRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional; // For optional chaining for parentReleaseGroup

@Service
@RequiredArgsConstructor
public class ReleaseCreationService {

    private final ReleaseRepository releaseRepository;
    private final ReleaseGroupRepository releaseGroupRepository;
    private final ReleaseContextRepository releaseContextRepository; // Inject ReleaseContextRepository
    private final CustomIdGenerator idGenerator;

    // Defines states considered "active" for a Release or ReleaseGroup for mapping/modification
    private static final List<State> ACTIVE_STATES = Arrays.asList(
            State.PLANNED, State.QUEUED, State.IN_PROGRESS, State.BLOCKED,
            State.WAITING_FOR_APPROVAL, State.SCHEDULED, State.APPROVED, State.RETRY
    );

    private boolean isActiveState(String stateStr) {
        try {
            State state = State.valueOf(stateStr);
            return ACTIVE_STATES.contains(state);
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    @Transactional // Ensure atomicity for both save and update operations
    public Release createRelease(Release release) {
        if (release.getName() == null || release.getName().trim().isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Release name is required.");
        }

        String releaseId = idGenerator.generateId("R", release.getReleaseGroupId());
        release.setId(releaseId);

        // Create an initial ReleaseContext for the Release
        ReleaseContext releaseContext = new ReleaseContext();
        releaseContext.setId(releaseId); // Context ID matches Release ID
        releaseContext.setEntityType(ContextEntityType.RELEASE);
        // Initialize globalShared and environments maps
        releaseContext.setGlobalShared(new HashMap<>());
        releaseContext.setEnvironments(new HashMap<>());
        releaseContextRepository.save(releaseContext);

        release.setReleaseContextId(releaseContext.getId()); // Link Release to its context

        Release savedRelease = releaseRepository.save(release); // Save the new Release

        // If a releaseGroupId is provided, update the parent ReleaseGroup and its ReleaseContext
        if (savedRelease.getReleaseGroupId() != null && !savedRelease.getReleaseGroupId().trim().isEmpty()) {
            ReleaseGroup parentReleaseGroup = releaseGroupRepository.findById(savedRelease.getReleaseGroupId())
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Parent Release Group not found with ID: " + savedRelease.getReleaseGroupId()));

            // Ensure the target Release Group is active for assignment
            if (!isActiveState(parentReleaseGroup.getState())) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Cannot create release mapped to an inactive Release Group.");
            }

            parentReleaseGroup.getReleaseIds().add(savedRelease.getId()); // Add child Release ID to parent's list
            releaseGroupRepository.save(parentReleaseGroup); // Save updated parent

            // Update parent ReleaseGroup's ReleaseContext to track this child
            if (parentReleaseGroup.getReleaseContextId() != null) {
                ReleaseContext parentReleaseGroupContext = releaseContextRepository.findById(parentReleaseGroup.getReleaseContextId())
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Release Group Context not found for ID: " + parentReleaseGroup.getReleaseContextId()));
                
                parentReleaseGroupContext.getChildReleaseContextIds().add(savedRelease.getReleaseContextId());
                releaseContextRepository.save(parentReleaseGroupContext);
            }
        }

        return savedRelease;
    }
}

// src/main/java/com/reflow/services/PhaseCreationService.java
package com.reflow.services;

import com.reflow.models.Phase;
import com.reflow.models.Release;
import com.reflow.models.ReleaseGroup;
import com.reflow.repositories.PhaseRepository;
import com.reflow.repositories.ReleaseGroupRepository;
import com.reflow.repositories.ReleaseRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.util.Optional;

@Service
@RequiredArgsConstructor
public class PhaseCreationService {

    private final PhaseRepository phaseRepository;
    private final ReleaseGroupRepository releaseGroupRepository;
    private final ReleaseRepository releaseRepository;
    private final CustomIdGenerator idGenerator;

    @Transactional
    public Phase createPhase(Phase phase) {
        if (phase.getParentType() == null || phase.getParentId() == null || phase.getParentId().trim().isEmpty() ||
            phase.getName() == null || phase.getName().trim().isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Parent type, parent ID, and name are required.");
        }

        // Validate parent existence and update parent's list
        if (phase.getParentType() == Phase.ParentType.RELEASE_GROUP) {
            ReleaseGroup parentReleaseGroup = releaseGroupRepository.findById(phase.getParentId())
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Parent Release Group not found with ID: " + phase.getParentId()));
            
            phase.setId(idGenerator.generateId("P", phase.getParentId())); // Generate ID
            Phase savedPhase = phaseRepository.save(phase); // Save the new Phase

            // Add child Phase ID to parent ReleaseGroup's phaseIds list
            parentReleaseGroup.getPhaseIds().add(savedPhase.getId());
            releaseGroupRepository.save(parentReleaseGroup); // Save updated parent
            return savedPhase;

        } else if (phase.getParentType() == Phase.ParentType.RELEASE) {
            Release parentRelease = releaseRepository.findById(phase.getParentId())
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Parent Release not found with ID: " + phase.getParentId()));
            
            phase.setId(idGenerator.generateId("P", phase.getParentId())); // Generate ID
            Phase savedPhase = phaseRepository.save(phase); // Save the new Phase

            parentRelease.getPhaseIds().add(savedPhase.getId()); // Add child ID to parent's list
            releaseRepository.save(parentRelease); // Save updated parent
            return savedPhase;
        } else {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid parentType. Must be RELEASE_GROUP or RELEASE.");
        }
    }
}

// src/main/java/com/reflow/services/TaskCreationService.java
package com.reflow.services;

import com.reflow.models.Task;
import com.reflow.models.Phase;
import com.reflow.models.Task.GateStatus; // Import Task.GateStatus
import com.reflow.models.Task.TaskType; // Import Task.TaskType
import com.reflow.repositories.TaskRepository;
import com.reflow.repositories.PhaseRepository;
import com.reflow.repositories.ReleaseRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Optional; // Import Optional
import java.util.HashMap; // For initializing maps

@Service
@RequiredArgsConstructor
public class TaskCreationService {

    private final TaskRepository taskRepository;
    private final PhaseRepository phaseRepository;
    private final ReleaseRepository releaseRepository;
    private final CustomIdGenerator idGenerator;

    @Transactional
    public Task createTask(Task task) {
        // Essential checks: Task must always have a phaseId, releaseId, name, and type.
        if (task.getPhaseId() == null || task.getPhaseId().trim().isEmpty() ||
            task.getReleaseId() == null || task.getReleaseId().trim().isEmpty() ||
            task.getName() == null || task.getName().trim().isEmpty() ||
            task.getType() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Phase ID, Release ID, name, and type are required for a task.");
        }

        // 1. Validate existence of parent phase
        Phase parentPhase = phaseRepository.findById(task.getPhaseId())
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Parent Phase not found with ID: " + task.getPhaseId()));

        // 2. Validate existence of the associated Release
        // All tasks must now belong to an existing Release.
        Release associatedRelease = releaseRepository.findById(task.getReleaseId())
            .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Associated Release not found with ID: " + task.getReleaseId()));

        // 3. (Optional but good practice): Further validate if the task's releaseId is logically consistent with its parent phase
        // If the phase is directly under a Release, the task's releaseId *must* match that Release's ID.
        if (parentPhase.getParentType() == Phase.ParentType.RELEASE && !task.getReleaseId().equals(parentPhase.getParentId())) {
             throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Task's Release ID must match its parent Phase's Release ID when phase is under a Release.");
        }
        // If the phase is under a ReleaseGroup, the task's releaseId should be one of the child releases of that ReleaseGroup.
        // This is a more complex check; for simplicity, we'll rely on ReleaseId existence for now.
        // A future enhancement could check `associatedRelease.getReleaseGroupId()` matches `parentPhase.getParentId()`.


        // --- New Validation: Enforce single starting task per sequential flow with auto-linking ---
        // If this task is intended to be a starting task (no previous task)
        if (task.getPreviousTaskId() == null || task.getPreviousTaskId().trim().isEmpty()) {
            List<Task> existingStartingTasks = null;
            Task existingStartingTask = null;

            // Determine the scope of the starting task check (phase or sequential group)
            if (task.getGroupId() == null || task.getGroupId().trim().isEmpty()) {
                // Task is a direct child of a Phase (not part of an explicit group task)
                existingStartingTasks = taskRepository.findByPhaseIdAndPreviousTaskIdIsNullAndGroupIdIsNull(task.getPhaseId());
            } else {
                // Task is a child of a Group Task
                Task parentGroupTask = taskRepository.findById(task.getGroupId())
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Parent group task not found with ID: " + task.getGroupId()));

                if (parentGroupTask.getType() == TaskType.SEQUENTIAL_GROUP) {
                    // It's a sequential group, so only one starting task is allowed within it
                    existingStartingTasks = taskRepository.findByGroupIdAndPreviousTaskIdIsNull(task.getGroupId());
                }
                // If it's a PARALLEL_GROUP, multiple children can have previousTaskId=null, so we don't apply this restriction.
            }

            if (existingStartingTasks != null && !existingStartingTasks.isEmpty()) {
                // Found an existing starting task. We need to prepend the new task.
                existingStartingTask = existingStartingTasks.get(0); // Get the current first task

                // Update the existing starting task to link to the new task
                existingStartingTask.setPreviousTaskId(task.getId());
                taskRepository.save(existingStartingTask);
                
                // Set the new task's nextTaskId to the old starting task's ID
                task.setNextTaskId(existingStartingTask.getId());
                // The new task's previousTaskId remains null, making it the new start.
                System.out.println("Info: New task " + task.getId() + " is now the starting task. Old starting task " + existingStartingTask.getId() + " linked as its next task.");
            }
        }


        // Set initial gate status if it's a gate task
        if (task.isGate()) {
            // A gate task should initially be in a neutral or unapproved state
            task.setGateStatus(GateStatus.valueOf(task.getState())); // Set to its current state, Airflow will change it to APPROVED/REJECTED
        } else {
            // Non-gate tasks should not have a gate status
            task.setGateStatus(null);
        }

        // Set actor field - if not provided in request, it might default to null or a system value
        // No default set in Task.java, so it will be null if not provided in the POST request body.

        // Initialize taskVariables if null to prevent NullPointerException later
        if (task.getTaskVariables() == null) {
            task.setTaskVariables(new HashMap<>());
        }
        // Initialize logs if null
        if (task.getLogs() == null) {
            task.setLogs("");
        }
        // Initialize triggerInterval if it's a TRIGGER task and not provided
        if (task.getType() == TaskType.TRIGGER && task.getTriggerInterval() == null) {
            // Default to a sensible interval, e.g., 60 seconds, or throw error if not provided
            task.setTriggerInterval(60); // Default trigger interval
            System.out.println("Warning: TRIGGER task created without triggerInterval. Defaulting to 60 seconds.");
        } else if (task.getType() != TaskType.TRIGGER) {
            // For non-TRIGGER tasks, ensure triggerInterval is null
            task.setTriggerInterval(null);
        }

        // Initialize new inputVariables and outputVariables
        if (task.getInputVariables() == null) {
            task.setInputVariables(new HashMap<>());
        }
        if (task.getOutputVariables() == null) {
            task.setOutputVariables(new HashMap<>());
        }


        task.setId(idGenerator.generateId("T", task.getPhaseId())); // Generate custom ID
        Task savedTask = taskRepository.save(task); // Save the new Task

        // Update parent phase's taskIds list
        parentPhase.getTaskIds().add(savedTask.getId());
        phaseRepository.save(parentPhase);

        return savedTask;
    }
}
