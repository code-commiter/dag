// src/main/java/com/reflow/services/DataGeneratorService.java
package com.reflow.services;

import com.reflow.models.*;
import com.reflow.models.Phase.ParentType;
import com.reflow.models.Task.GateCategory;
import com.reflow.models.Task.GateStatus;
import com.reflow.models.Task.TaskType;
import com.reflow.repositories.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom; // For random numbers

@Service
@RequiredArgsConstructor
public class DataGeneratorService {

    private final ReleaseGroupRepository releaseGroupRepository;
    private final ReleaseRepository releaseRepository;
    private final PhaseRepository phaseRepository;
    private final TaskRepository taskRepository;
    private final ReleaseContextRepository releaseContextRepository; // Assuming you have this for completeness

    /**
     * Generates a complete Release with a hierarchy of Phases and Tasks,
     * populating all properties with dummy data and maintaining sequential links.
     *
     * @param releaseGroupName The name for the optional Release Group.
     * @param releaseName The name for the Release.
     * @param numPhases The number of phases to create within the release.
     * @param tasksPerPhase The average number of tasks per phase.
     * @return The created Release object.
     */
    @Transactional
    public Release generateFullReleaseWithDummyData(String releaseGroupName, String releaseName, int numPhases, int tasksPerPhase) {
        System.out.println("--- Starting Dummy Data Generation ---");

        // 1. Create Release Group (Optional, but good for hierarchy)
        ReleaseGroup releaseGroup = createDummyReleaseGroup(releaseGroupName);
        System.out.println("Created Release Group: " + releaseGroup.getName() + " (ID: " + releaseGroup.getId() + ")");

        // 2. Create Release
        Release release = createDummyRelease(releaseName, releaseGroup.getId());
        System.out.println("Created Release: " + release.getName() + " (ID: " + release.getId() + ") under RG: " + releaseGroup.getId());

        // 3. Create Phases and Tasks
        String lastPhaseId = null;
        for (int i = 0; i < numPhases; i++) {
            String phaseName = "Phase " + (i + 1) + " for " + releaseName;
            Phase phase = createDummyPhase(release.getId(), phaseName, lastPhaseId, ParentType.RELEASE);

            // Update previous phase's nextPhaseId
            if (lastPhaseId != null) {
                phaseRepository.findById(lastPhaseId).ifPresent(prevPhase -> {
                    prevPhase.setNextPhaseId(phase.getId());
                    phaseRepository.save(prevPhase);
                    System.out.println("Linked Phase " + prevPhase.getId() + " next to " + phase.getId());
                });
            }
            lastPhaseId = phase.getId();
            System.out.println("Created Phase: " + phase.getName() + " (ID: " + phase.getId() + ") under Release: " + release.getId());

            // Create Tasks within this phase
            createDummyTasksForPhase(phase.getId(), release.getId(), tasksPerPhase);
        }

        // 4. Create Release Context (Optional, but ensures all top-level entities are linked)
        ReleaseContext releaseContext = createDummyReleaseContext(releaseGroup.getId(), release.getId());
        System.out.println("Created Release Context (ID: " + releaseContext.getId() + ") for Release Group: " + releaseGroup.getId());

        System.out.println("--- Dummy Data Generation Complete ---");
        return release;
    }

    /**
     * Creates a dummy ReleaseGroup entity.
     */
    private ReleaseGroup createDummyReleaseGroup(String name) {
        ReleaseGroup releaseGroup = new ReleaseGroup();
        releaseGroup.setId(UUID.randomUUID().toString());
        releaseGroup.setName(name);
        releaseGroup.setDescription("Release group for " + name);
        releaseGroup.setState(State.PLANNED.name());
        releaseGroup.setCreatedAt(LocalDateTime.now());
        releaseGroup.setCreatedBy("DataGenerator");
        releaseGroup.setUpdatedAt(LocalDateTime.now());
        releaseGroup.setUpdatedBy("DataGenerator");
        // Other properties can be set here if needed
        return releaseGroupRepository.save(releaseGroup);
    }

    /**
     * Creates a dummy Release entity.
     */
    private Release createDummyRelease(String name, String releaseGroupId) {
        Release release = new Release();
        release.setId(UUID.randomUUID().toString());
        release.setName(name);
        release.setDescription("Description for " + name);
        release.setReleaseGroupId(releaseGroupId);
        release.setPlannedStartDate(LocalDateTime.now());
        release.setPlannedEndDate(LocalDateTime.now().plusWeeks(2));
        release.setState(State.PLANNED.name());
        release.setCreatedAt(LocalDateTime.now());
        release.setCreatedBy("DataGenerator");
        release.setUpdatedAt(LocalDateTime.now());
        release.setUpdatedBy("DataGenerator");
        // Other properties like releaseManager, team
        return releaseRepository.save(release);
    }

    /**
     * Creates a dummy Phase entity.
     *
     * @param parentId The ID of the parent (Release or ReleaseGroup).
     * @param name The name of the phase.
     * @param previousPhaseId The ID of the preceding phase in the sequence (can be null).
     * @param parentType The type of the parent (RELEASE or RELEASE_GROUP).
     * @return The created Phase object.
     */
    private Phase createDummyPhase(String parentId, String name, String previousPhaseId, ParentType parentType) {
        Phase phase = new Phase();
        phase.setId(UUID.randomUUID().toString());
        phase.setName(name);
        phase.setDescription("Description for " + name);
        phase.setParentId(parentId);
        phase.setParentType(parentType);
        phase.setState(State.PLANNED.name());
        phase.setPlannedStartDate(LocalDateTime.now());
        phase.setPlannedEndDate(LocalDateTime.now().plusDays(5));
        phase.setPreviousPhaseId(previousPhaseId);
        // nextPhaseId will be set by the next phase creation if applicable
        phase.setCreatedAt(LocalDateTime.now());
        phase.setCreatedBy("DataGenerator");
        phase.setUpdatedAt(LocalDateTime.now());
        phase.setUpdatedBy("DataGenerator");
        // Other properties
        return phaseRepository.save(phase);
    }

    /**
     * Creates a dummy ReleaseContext entity.
     */
    private ReleaseContext createDummyReleaseContext(String releaseGroupId, String releaseId) {
        ReleaseContext context = new ReleaseContext();
        context.setId(UUID.randomUUID().toString());
        context.setReleaseGroupId(releaseGroupId);
        context.setReleaseId(releaseId);
        context.setCreatedAt(LocalDateTime.now());
        context.setCreatedBy("DataGenerator");
        return releaseContextRepository.save(context);
    }

    /**
     * Creates a set of dummy tasks for a given phase, including various types and a task group.
     *
     * @param phaseId The ID of the parent phase.
     * @param releaseId The ID of the associated release.
     * @param count The number of tasks to create in this phase.
     */
    private void createDummyTasksForPhase(String phaseId, String releaseId, int count) {
        String lastTaskIdInPhase = null;
        String groupTaskId = null; // To hold the ID of a potential group task

        // Optionally create a Task Group early in the phase
        if (count >= 3 && ThreadLocalRandom.current().nextDouble() < 0.3) { // 30% chance to have a group
            groupTaskId = UUID.randomUUID().toString();
            Task groupTask = buildDummyTask(
                groupTaskId,
                phaseId,
                releaseId,
                null, // No group parent for the group itself
                "Task Group: Setup " + UUID.randomUUID().toString().substring(0,4),
                TaskType.GROUP,
                false,
                null,
                lastTaskIdInPhase // Link group to previous task in phase
            );
            groupTask.setTaskVariables(Map.of("groupPurpose", "A collection of related tasks"));
            taskRepository.save(groupTask);
            System.out.println("  Created Task Group: " + groupTask.getName() + " (ID: " + groupTask.getId() + ") in Phase: " + phaseId);
            lastTaskIdInPhase = groupTask.getId(); // Group becomes the last task in phase for now

            // Create some tasks inside this group
            String lastTaskIdInGroup = null;
            int tasksInGroup = ThreadLocalRandom.current().nextInt(2, 5); // 2 to 4 tasks in group
            for (int j = 0; j < tasksInGroup; j++) {
                TaskType type = getRandomTaskType(false); // No groups inside groups
                String taskName = "Group Task " + (j + 1) + ": " + type.name().replace("_", " ");
                Task task = buildDummyTask(
                    UUID.randomUUID().toString(),
                    phaseId,
                    releaseId,
                    groupTaskId, // Link to the group task
                    taskName,
                    type,
                    type == TaskType.APPROVAL, // Only approval tasks are gates
                    type == TaskType.APPROVAL ? getRandomGateCategory() : null,
                    lastTaskIdInGroup // Link to previous task in group
                );
                taskRepository.save(task);
                if (lastTaskIdInGroup != null) {
                    taskRepository.findById(lastTaskIdInGroup).ifPresent(prevTask -> {
                        prevTask.setNextTaskId(task.getId());
                        taskRepository.save(prevTask);
                    });
                }
                lastTaskIdInGroup = task.getId();
                System.out.println("    Created Group Child Task: " + task.getName() + " (ID: " + task.getId() + ") in Group: " + groupTaskId);
            }
            // The last task in the group's nextTaskId remains null, signifying end of group sequence
            // The next task in the phase (if any) will link to the groupTaskId itself, not its children.
        }

        // Create remaining tasks directly in the phase
        // Adjust count if a group was created
        int remainingTasks = count - (groupTaskId != null ? 1 : 0);
        String currentPhaseLastTaskId = lastTaskIdInPhase; // Start linking from after the group, or null if no group

        for (int i = 0; i < remainingTasks; i++) {
            TaskType type = getRandomTaskType(true); // Can be any type
            String taskName = "Task " + (i + 1) + ": " + type.name().replace("_", " ") + " for Phase " + phaseId.substring(0,4);
            Task task = buildDummyTask(
                UUID.randomUUID().toString(),
                phaseId,
                releaseId,
                null, // Not part of a subgroup
                taskName,
                type,
                type == TaskType.APPROVAL,
                type == TaskType.APPROVAL ? getRandomGateCategory() : null,
                currentPhaseLastTaskId // Link to previous task in phase
            );
            taskRepository.save(task);

            if (currentPhaseLastTaskId != null) {
                // Update the previous task's nextTaskId
                taskRepository.findById(currentPhaseLastTaskId).ifPresent(prevTask -> {
                    prevTask.setNextTaskId(task.getId());
                    taskRepository.save(prevTask);
                });
            }
            currentPhaseLastTaskId = task.getId();
            System.out.println("  Created Phase Task: " + task.getName() + " (ID: " + task.getId() + ") in Phase: " + phaseId);
        }
    }


    /**
     * Helper to build a Task object with common dummy data.
     */
    private Task buildDummyTask(
            String id,
            String phaseId,
            String releaseId,
            String groupId,
            String name,
            TaskType type,
            boolean isGate,
            GateCategory gateCategory,
            String previousTaskId) {

        Task task = new Task();
        task.setId(id);
        task.setName(name);
        task.setDescription("Description for " + name);
        task.setPhaseId(phaseId);
        task.setReleaseId(releaseId);
        task.setGroupId(groupId); // Null if not part of a group
        task.setType(type);
        task.setState(State.PLANNED.name());
        task.setPlannedStartDate(LocalDateTime.now());
        task.setPlannedEndDate(LocalDateTime.now().plusDays(2));
        task.setAssignedTo("reflow-user-" + ThreadLocalRandom.current().nextInt(1, 4));
        task.setActor("actor-script-" + ThreadLocalRandom.current().nextInt(1, 3));
        task.setPreConditions("Pre-condition for " + name);
        task.setPostConditions("Post-condition for " + name);
        task.setRollbackInstructions("Rollback for " + name);
        task.setEstimatedDurationHours(ThreadLocalRandom.current().nextDouble(0.5, 8.0));
        task.setIsGate(isGate);
        if (isGate) {
            task.setGateStatus(GateStatus.PENDING);
            task.setGateCategory(gateCategory);
            // Example: Add a dummy approval rule
            task.setApprovalRule("Requires approval from " + (ThreadLocalRandom.current().nextBoolean() ? "Manager" : "Team Lead"));
            task.setApproverGroup("Group" + ThreadLocalRandom.current().nextInt(1, 3));
        } else {
            task.setGateStatus(null);
            task.setGateCategory(null);
        }
        task.setPreviousTaskId(previousTaskId);
        // nextTaskId will be set by the next task created, or remain null if it's the last
        task.setTaskVariables(new HashMap<>()); // Initialize an empty map for task variables
        task.setLogs(""); // Initialize empty logs
        task.setCreatedAt(LocalDateTime.now());
        task.setCreatedBy("DataGenerator");
        task.setUpdatedAt(LocalDateTime.now());
        task.setUpdatedBy("DataGenerator");

        return task;
    }

    /**
     * Returns a random TaskType, optionally excluding GROUP.
     */
    private TaskType getRandomTaskType(boolean excludeGroup) {
        TaskType[] types = TaskType.values();
        TaskType selectedType;
        do {
            selectedType = types[ThreadLocalRandom.current().nextInt(types.length)];
        } while (excludeGroup && selectedType == TaskType.GROUP);
        return selectedType;
    }

    /**
     * Returns a random GateCategory.
     */
    private GateCategory getRandomGateCategory() {
        GateCategory[] categories = GateCategory.values();
        return categories[ThreadLocalRandom.current().nextInt(categories.length)];
    }

    // You might want a method to clear all generated data for testing purposes
    @Transactional
    public void deleteAllGeneratedData() {
        System.out.println("--- Deleting All Generated Data ---");
        taskRepository.deleteAll();
        phaseRepository.deleteAll();
        releaseRepository.deleteAll();
        releaseGroupRepository.deleteAll();
        releaseContextRepository.deleteAll();
        System.out.println("--- All Generated Data Deleted ---");
    }
}
