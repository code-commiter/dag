// src/main/java/com/reflow/services/WorkflowService.java
package com.reflow.services;

import com.reflow.models.*;
import com.reflow.repositories.*;
import com.reflow.utils.InMemoryWorkflowBuilder; // Import the builder class
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.ArrayList; // Needed for DTOs
import java.util.Comparator; // Needed for DTOs
import java.util.Objects; // Needed for DTOs
import java.util.Optional; // Needed for DTOs

/**
 * Orchestration service for creating and managing workflow entities.
 * This service leverages InMemoryWorkflowBuilder to construct the workflow graph
 * in memory before persisting it to the database in a single transaction,
 * minimizing database round-trips.
 */
@Service
public class WorkflowService {

    private final ReleaseGroupRepository releaseGroupRepository;
    private final ReleaseRepository releaseRepository;
    private final PhaseRepository phaseRepository;
    private final TaskRepository taskRepository;
    private final ReleaseContextRepository releaseContextRepository;
    private final MongoTemplate mongoTemplate;

    /**
     * Constructor for WorkflowService, injecting necessary repositories and MongoTemplate.
     *
     * @param releaseGroupRepository Repository for ReleaseGroup entities.
     * @param releaseRepository      Repository for Release entities.
     * @param phaseRepository        Repository for Phase entities.
     * @param taskRepository         Repository for Task entities.
     * @param releaseContextRepository Repository for ReleaseContext entities.
     * @param mongoTemplate          MongoTemplate for batch database operations.
     */
    public WorkflowService(ReleaseGroupRepository releaseGroupRepository,
                           ReleaseRepository releaseRepository,
                           PhaseRepository phaseRepository,
                           TaskRepository taskRepository,
                           ReleaseContextRepository releaseContextRepository,
                           MongoTemplate mongoTemplate) {
        this.releaseGroupRepository = releaseGroupRepository;
        this.releaseRepository = releaseRepository;
        this.phaseRepository = phaseRepository;
        this.taskRepository = taskRepository;
        this.releaseContextRepository = releaseContextRepository;
        this.mongoTemplate = mongoTemplate;
    }

    /**
     * Orchestrates the creation of a full workflow graph from a DTO plan.
     * Uses InMemoryWorkflowBuilder to construct the graph in memory, then persists it
     * to the database in a single transaction.
     *
     * @param workflowPlanDto The DTO representing the full workflow structure to be created.
     * @return The fully linked and persisted ReleaseGroup, which is the root of the created workflow.
     */
    @Transactional
    public ReleaseGroup createFullWorkflowFromPlan(WorkflowPlanDTO workflowPlanDto) {

        // 1. Instantiate a new builder for this specific workflow creation request.
        // This ensures thread safety as each request gets its isolated in-memory builder.
        InMemoryWorkflowBuilder builder = new InMemoryWorkflowBuilder();

        // --- 2. Build the workflow graph in memory using the builder ---

        // Create Release Group (root of the workflow)
        // The builder handles setting IDs and initial properties.
        ReleaseGroup releaseGroup = builder.addReleaseGroup(
            workflowPlanDto.getReleaseName() + " Group", "Group for " + workflowPlanDto.getReleaseName());

        // Create Release, linking it to the Release Group
        Release release = builder.addRelease(
            releaseGroup, workflowPlanDto.getReleaseName(), workflowPlanDto.getReleaseDescription());

        // Process Phases from the DTO plan
        for (WorkflowPlanDTO.PhaseDTO phaseDto : workflowPlanDto.getPhases()) {
            // Create a new Phase object (client/orchestrator generates its ID)
            Phase newPhase = new Phase();
            newPhase.setId(UUID.randomUUID().toString());
            newPhase.setName(phaseDto.getName());
            newPhase.setDescription(phaseDto.getDescription());
            // Timestamps (createdAt, updatedAt) are handled by the builder if not explicitly set here.

            // Add the phase to the builder, which handles linking it sequentially to the 'release' object.
            builder.addPhase(newPhase, release);

            // Process Tasks within the current phase from the DTO plan
            for (WorkflowPlanDTO.TaskDTO taskDto : phaseDto.getTasks()) {
                // Create a new Task object (client/orchestrator generates its ID)
                Task newTask = new Task();
                newTask.setId(UUID.randomUUID().toString());
                newTask.setName(taskDto.getName());
                newTask.setDescription(taskDto.getDescription());
                newTask.setType(taskDto.getType());
                // IMPORTANT: Denormalized releaseId must be set on the task by the orchestrator
                // as it's a critical piece of context for the task.
                newTask.setReleaseId(release.getId());

                // Set any task-specific properties from the DTO
                if (taskDto.getApprovers() != null) {
                    newTask.setApprovers(taskDto.getApprovers());
                }
                // Add more task-specific properties (e.g., scheduledTime, triggerInterval, taskVariables)
                // if they are present in your TaskDTO and need to be set here.

                // Determine if the task is a group task (PARALLEL_GROUP or SEQUENTIAL_GROUP)
                if (taskDto.isGroup()) {
                    // If it's a group task, add it to the builder, linking it to the current phase.
                    // The builder will handle setting phaseId, groupId (null for top-level group),
                    // and sequential linking within the phase.
                    builder.addTask(newTask, newPhase);

                    // If this group task has children, process them
                    if (taskDto.getChildren() != null && !taskDto.getChildren().isEmpty()) {
                        for (WorkflowPlanDTO.TaskDTO childTaskDto : taskDto.getChildren()) {
                            // Create a new child Task object
                            Task childTask = new Task();
                            childTask.setId(UUID.randomUUID().toString());
                            childTask.setName(childTaskDto.getName());
                            childTask.setDescription(childTaskDto.getDescription());
                            childTask.setType(childTaskDto.getType());
                            // Child task also needs the denormalized releaseId
                            childTask.setReleaseId(release.getId());

                            // Set any child task-specific properties
                            if (childTaskDto.getApprovers() != null) {
                                childTask.setApprovers(childTaskDto.getApprovers());
                            }
                            // Add more child task-specific properties here.

                            // Add the child task to the builder, linking it to its parent group task (newTask).
                            // The builder will handle setting phaseId (inherited from group), groupId (group's ID),
                            // and sequential linking within the group (if it's a sequential group).
                            builder.addTask(childTask, newTask);
                        }
                    }
                } else { // It's a regular (non-group) task
                    // Add the regular task to the builder, linking it to the current phase.
                    // The builder will handle setting phaseId, groupId (null), and sequential linking.
                    builder.addTask(newTask, newPhase);
                }
            }
        }

        // --- 3. Final Database Write Operations ---
        // After the entire workflow graph is constructed and linked in memory,
        // perform all database saves in a single transactional block.

        // Save the root ReleaseGroup individually.
        releaseGroupRepository.save(releaseGroup);

        // Use MongoTemplate's insertAll for efficient bulk inserts of collected entities.
        if (!builder.getContexts().isEmpty()) {
            mongoTemplate.insertAll(builder.getContexts());
        }
        if (!builder.getReleases().isEmpty()) {
            mongoTemplate.insertAll(builder.getReleases());
        }
        if (!builder.getPhases().isEmpty()) {
            mongoTemplate.insertAll(builder.getPhases());
        }
        if (!builder.getTasks().isEmpty()) {
            mongoTemplate.insertAll(builder.getTasks());
        }

        return releaseGroup; // Return the fully linked and persisted ReleaseGroup
    }

    /**
     * Utility method to update the state of an existing Task.
     * This method directly interacts with the database to update a single task.
     *
     * @param taskId   The ID of the task to update.
     * @param newState The new state to set for the task.
     * @return The updated Task object.
     * @throws IllegalArgumentException If the task is not found.
     */
    @Transactional
    public Task updateTaskState(String taskId, State newState) {
        Task task = taskRepository.findById(taskId)
                .orElseThrow(() -> new IllegalArgumentException("Task not found with ID: " + taskId));
        task.setState(newState.name());
        task.setUpdatedAt(LocalDateTime.now());
        return taskRepository.save(task);
    }

    // --- Data Transfer Objects (DTOs) for Workflow Plan Input ---
    // These DTOs define the structure of the JSON input for creating a workflow.
    // They are nested static classes for convenience, but can be moved to a separate package.

    public static class WorkflowPlanDTO {
        private String releaseName;
        private String releaseDescription;
        private List<PhaseDTO> phases;

        // Getters and setters for WorkflowPlanDTO
        public String getReleaseName() { return releaseName; }
        public void setReleaseName(String releaseName) { this.releaseName = releaseName; }
        public String getReleaseDescription() { return releaseDescription; }
        public void setReleaseDescription(String releaseDescription) { this.releaseDescription = releaseDescription; }
        public List<PhaseDTO> getPhases() { return phases; }
        public void setPhases(List<PhaseDTO> phases) { this.phases = phases; }

        public static class PhaseDTO {
            private String name;
            private String description;
            private List<TaskDTO> tasks;

            // Getters and setters for PhaseDTO
            public String getName() { return name; }
            public void setName(String name) { this.name = name; }
            public String getDescription() { return description; }
            public void setDescription(String description) { this.description = description; }
            public List<TaskDTO> getTasks() { return tasks; }
            public void setTasks(List<TaskDTO> tasks) { this.tasks = tasks; }
        }

        public static class TaskDTO {
            private String name;
            private String description;
            private Task.TaskType type; // Uses the Task.TaskType enum from your model
            private List<String> approvers; // Example of a task-specific property
            private List<TaskDTO> children; // For group tasks, contains their child tasks

            // Getters and setters for TaskDTO
            public String getName() { return name; }
            public void setName(String name) { this.name = name; }
            public String getDescription() { return description; }
            public void setDescription(String description) { this.description = description; }
            public Task.TaskType getType() { return type; }
            public void setType(Task.TaskType type) { this.type = type; }
            public List<String> getApprovers() { return approvers; }
            public void setApprovers(List<String> approvers) { this.approvers = approvers; }
            public List<TaskDTO> getChildren() { return children; }
            public void setChildren(List<TaskDTO> children) { this.children = children; }

            /**
             * Helper method to determine if this TaskDTO represents a group task type.
             * This relies on the Task.TaskType enum having an 'isGroup()' method.
             * (You would need to add this to your Task.java enum if it's not there:
             * public boolean isGroup() { return this == PARALLEL_GROUP || this == SEQUENTIAL_GROUP; } )
             */
            public boolean isGroup() {
                return this.type == Task.TaskType.PARALLEL_GROUP || this.type == Task.TaskType.SEQUENTIAL_GROUP;
            }
        }
    }
}
