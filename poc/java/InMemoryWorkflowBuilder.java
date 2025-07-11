// src/main/java/com/reflow/utils/InMemoryWorkflowBuilder.java
package com.reflow.utils;

import com.reflow.models.*; // Ensure all models are imported

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Utility class responsible for building and linking workflow entities (Phases, Tasks)
 * purely in memory. It collects all created entities into lists for later batch persistence.
 * This class is designed to be instantiated per workflow creation request to ensure thread safety.
 */
public class InMemoryWorkflowBuilder {

    private final List<ReleaseContext> contexts = new ArrayList<>();
    private final List<Release> releases = new ArrayList<>();
    private final List<Phase> phases = new ArrayList<>();
    private final List<Task> tasks = new ArrayList<>();

    public InMemoryWorkflowBuilder() {
        // No-op constructor
    }

    /**
     * Adds a new ReleaseGroup to the builder's collection and creates its associated ReleaseContext.
     * This is typically the starting point for building a new workflow.
     *
     * @param name The name of the ReleaseGroup.
     * @param description The description of the ReleaseGroup.
     * @return The created ReleaseGroup object.
     */
    public ReleaseGroup addReleaseGroup(String name, String description) {
        ReleaseGroup releaseGroup = new ReleaseGroup();
        String id = UUID.randomUUID().toString();
        releaseGroup.setId(id);
        releaseGroup.setName(name);
        releaseGroup.setDescription(description);
        releaseGroup.setCreatedAt(LocalDateTime.now());
        releaseGroup.setUpdatedAt(LocalDateTime.now());

        ReleaseContext releaseGroupContext = new ReleaseContext();
        releaseGroupContext.setId(id);
        releaseGroupContext.setEntityType(ReleaseContext.ContextEntityType.RELEASE_GROUP);
        releaseGroupContext.setCreatedAt(LocalDateTime.now());
        releaseGroupContext.setUpdatedAt(LocalDateTime.now());
        this.contexts.add(releaseGroupContext);

        releaseGroup.setReleaseContextId(releaseGroupContext.getId());
        return releaseGroup;
    }

    /**
     * Adds a new Release to the builder's collection, links it to its parent ReleaseGroup,
     * and creates its associated ReleaseContext.
     *
     * @param releaseGroup The parent ReleaseGroup object (must have its ID set).
     * @param name The name of the Release.
     * @param description The description of the Release.
     * @return The created Release object.
     */
    public Release addRelease(ReleaseGroup releaseGroup, String name, String description) {
        if (releaseGroup.getId() == null) {
            throw new IllegalArgumentException("Parent ReleaseGroup must have an ID set.");
        }

        Release release = new Release();
        String id = UUID.randomUUID().toString();
        release.setId(id);
        release.setReleaseGroupId(releaseGroup.getId());
        release.setName(name);
        release.setDescription(description);
        release.setCreatedAt(LocalDateTime.now());
        release.setUpdatedAt(LocalDateTime.now());

        ReleaseContext releaseContext = new ReleaseContext();
        releaseContext.setId(id);
        releaseContext.setEntityType(ReleaseContext.ContextEntityType.RELEASE);
        releaseContext.setCreatedAt(LocalDateTime.now());
        releaseContext.setUpdatedAt(LocalDateTime.now());
        this.contexts.add(releaseContext);

        release.setReleaseContextId(releaseContext.getId());

        // Update ReleaseGroup to link the new Release (in memory)
        releaseGroup.getReleaseIds().add(release.getId());
        // Also update the ReleaseGroup's context to include the child ReleaseContextId
        this.contexts.stream()
                .filter(rc -> rc.getId().equals(releaseGroup.getId()) && rc.getEntityType() == ReleaseContext.ContextEntityType.RELEASE_GROUP)
                .findFirst()
                .ifPresent(parentContext -> parentContext.getChildReleaseContextIds().add(release.getId()));

        this.releases.add(release); // Add to builder's collection
        return release;
    }

    /**
     * Adds a pre-created Phase object to the builder's collection and links it sequentially
     * to its parent (ReleaseGroup or Release) in memory.
     * The Phase object passed in must have its ID, name, description, and timestamps already set.
     *
     * @param newPhase The pre-created Phase object to link.
     * @param parentObject The parent object (either a ReleaseGroup or a Release), inherited from BaseEntity.
     * @return The linked Phase object.
     * @throws IllegalArgumentException If the newPhase or parentObject are invalid.
     */
    public Phase addPhase(Phase newPhase, BaseEntity parentObject) { // Changed Object to BaseEntity
        if (newPhase.getId() == null) {
            throw new IllegalArgumentException("Phase object must have ID set before adding.");
        }
        if (parentObject == null) {
            throw new IllegalArgumentException("Parent object cannot be null for phase linking.");
        }
        // Ensure parentObject has an ID (inherited from BaseEntity)
        if (parentObject.getId() == null) {
            throw new IllegalArgumentException("Parent object must have an ID set.");
        }

        String parentId;
        Phase.ParentType parentType;

        if (parentObject instanceof Release) {
            Release parentRelease = (Release) parentObject;
            parentId = parentRelease.getId();
            parentType = Phase.ParentType.RELEASE;
            parentRelease.getPhaseIds().add(newPhase.getId()); // Update parent Release in memory
        } else if (parentObject instanceof ReleaseGroup) {
            ReleaseGroup parentReleaseGroup = (ReleaseGroup) parentObject;
            parentId = parentReleaseGroup.getId();
            parentType = Phase.ParentType.RELEASE_GROUP;
            parentReleaseGroup.getPhaseIds().add(newPhase.getId()); // Update parent ReleaseGroup in memory
        } else {
            throw new IllegalArgumentException("Unsupported parent object type for phase linking: " + parentObject.getClass().getName());
        }

        newPhase.setParentId(parentId);
        newPhase.setParentType(parentType);
        if (newPhase.getCreatedAt() == null) newPhase.setCreatedAt(LocalDateTime.now());
        newPhase.setUpdatedAt(LocalDateTime.now());

        // Find the last phase in the current in-memory list for this parent
        Optional<Phase> lastPhaseOpt = this.phases.stream()
                .filter(p -> p.getParentId().equals(parentId) && p.getParentType() == parentType)
                .filter(p -> p.getNextPhaseId() == null || p.getNextPhaseId().isEmpty())
                .max(Comparator.comparing(Phase::getCreatedAt));

        if (lastPhaseOpt.isPresent()) {
            Phase previousPhase = lastPhaseOpt.get();
            previousPhase.setNextPhaseId(newPhase.getId()); // Update previous phase in memory
            newPhase.setPreviousPhaseId(previousPhase.getId());
        }
        this.phases.add(newPhase); // Add new phase to the builder's collection
        return newPhase;
    }

    /**
     * Intelligently adds a pre-created Task object to the builder's collection and links it
     * to its parent (either a Phase or a Group Task) in memory.
     * The newTask object must have its ID, name, description, type, and importantly, its ReleaseId already set.
     * This method will deduce phaseId and groupId from the parentObject and set them on newTask.
     *
     * @param newTask The pre-created Task object to link.
     * @param parentObject The parent object (either a Phase or a Task that is a group), inherited from BaseEntity.
     * @return The linked Task object.
     * @throws IllegalArgumentException If the new task or parentObject are invalid or their types are incompatible.
     */
    public Task addTask(Task newTask, BaseEntity parentObject) { // Changed Object to BaseEntity
        if (newTask.getId() == null || parentObject == null) {
            throw new IllegalArgumentException("Task and parent objects must have IDs and be non-null before adding.");
        }
        // Ensure parentObject has an ID (inherited from BaseEntity)
        if (parentObject.getId() == null) {
            throw new IllegalArgumentException("Parent object must have an ID set.");
        }
        // Essential: Task must know its overall ReleaseId as it's denormalized across the hierarchy
        if (newTask.getReleaseId() == null) {
            throw new IllegalArgumentException("Task object must have its ReleaseId set by the client before linking.");
        }

        String actualPhaseId;
        String actualGroupId;

        // Set timestamps if not already set (good practice)
        if (newTask.getCreatedAt() == null) newTask.setCreatedAt(LocalDateTime.now());
        newTask.setUpdatedAt(LocalDateTime.now());

        // --- Determine parent type and extract IDs ---
        if (parentObject instanceof Phase) {
            Phase parentPhase = (Phase) parentObject;
            actualPhaseId = parentPhase.getId();
            actualGroupId = null; // Direct child of a phase, so no group ID
            newTask.setPhaseId(actualPhaseId); // Set on task
            newTask.setGroupId(actualGroupId);  // Set on task

            // Add task ID to the parent Phase's taskIds list (in memory)
            parentPhase.getTaskIds().add(newTask.getId());
        } else if (parentObject instanceof Task) {
            Task parentGroupTask = (Task) parentObject;
            // Assuming Task.TaskType has an isGroup() method
            if (!parentGroupTask.getType().isGroup()) { // Check if it's actually a group task
                throw new IllegalArgumentException("Parent object must be a group Task for child task linking.");
            }
            // Ensure parent group task has its phaseId and releaseId set (inherited from its own linking)
            if (parentGroupTask.getPhaseId() == null || parentGroupTask.getReleaseId() == null) {
                throw new IllegalArgumentException("Parent group task must have its PhaseId and ReleaseId set.");
            }
            actualPhaseId = parentGroupTask.getPhaseId();
            actualGroupId = parentGroupTask.getId(); // Child's group ID is parent group's ID
            newTask.setPhaseId(actualPhaseId); // Set on task
            newTask.setGroupId(actualGroupId);  // Set on task
        } else {
            throw new IllegalArgumentException("Unsupported parent object type for task linking: " + parentObject.getClass().getName());
        }

        // Find the last sibling task in the current in-memory list within the same phase and group context
        Optional<Task> lastSiblingTaskOpt = this.tasks.stream()
                .filter(t -> t.getPhaseId().equals(actualPhaseId))
                .filter(t -> Objects.equals(t.getGroupId(), actualGroupId)) // Check groupId
                .filter(t -> t.getNextTaskId() == null || t.getNextTaskId().isEmpty())
                .max(Comparator.comparing(Task::getCreatedAt));

        if (lastSiblingTaskOpt.isPresent()) {
            Task previousTask = lastSiblingTaskOpt.get();
            previousTask.setNextTaskId(newTask.getId()); // Update previous task in memory
            newTask.setPreviousTaskId(previousTask.getId());
        }
        this.tasks.add(newTask); // Add new task to the builder's collection
        return newTask;
    }

    /**
     * Retrieves all ReleaseContext objects collected by the builder.
     * @return A list of ReleaseContext objects.
     */
    public List<ReleaseContext> getContexts() {
        return new ArrayList<>(contexts); // Return a copy to prevent external modification
    }

    /**
     * Retrieves all Release objects collected by the builder.
     * @return A list of Release objects.
     */
    public List<Release> getReleases() {
        return new ArrayList<>(releases);
    }

    /**
     * Retrieves all Phase objects collected by the builder.
     * @return A list of Phase objects.
     */
    public List<Phase> getPhases() {
        return new ArrayList<>(phases);
    }

    /**
     * Retrieves all Task objects collected by the builder.
     * @return A list of Task objects.
     */
    public List<Task> getTasks() {
        return new ArrayList<>(tasks);
    }
}
