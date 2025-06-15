// src/main/java/com/reflow/controllers/ReleaseGroupController.java
package com.reflow.controllers;

import com.reflow.dtos.PhaseFullDto;
import com.reflow.dtos.ReleaseFullDto;
import com.reflow.dtos.ReleaseGroupFullDto;
import com.reflow.dtos.TaskDto;
import com.reflow.models.Phase;
import com.reflow.models.Release;
import com.reflow.models.ReleaseGroup;
import com.reflow.models.State;
import com.reflow.models.Task;
import com.reflow.repositories.PhaseRepository;
import com.reflow.repositories.ReleaseGroupRepository;
import com.reflow.repositories.ReleaseRepository;
import com.reflow.repositories.TaskRepository;
import com.reflow.services.CustomIdGenerator;
import com.reflow.services.ReleaseGroupCreationService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/release-groups")
@RequiredArgsConstructor
public class ReleaseGroupController {

    private final ReleaseGroupRepository releaseGroupRepository;
    private final ReleaseRepository releaseRepository;
    private final PhaseRepository phaseRepository;
    private final TaskRepository taskRepository;
    private final CustomIdGenerator idGenerator;
    private final ReleaseGroupCreationService releaseGroupCreationService;

    @PostMapping
    public ResponseEntity<ReleaseGroup> createReleaseGroup(@RequestBody ReleaseGroup releaseGroup) {
        if (releaseGroup.getName() == null || releaseGroup.getName().trim().isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Release Group name is required.");
        }
        ReleaseGroup savedReleaseGroup = releaseGroupCreationService.createReleaseGroup(releaseGroup);
        return new ResponseEntity<>(savedReleaseGroup, HttpStatus.CREATED);
    }

    @GetMapping
    public ResponseEntity<List<ReleaseGroup>> getAllReleaseGroups() {
        List<ReleaseGroup> releaseGroups = releaseGroupRepository.findAll();
        return ResponseEntity.ok(releaseGroups);
    }

    @GetMapping("/{id}")
    public ResponseEntity<ReleaseGroup> getReleaseGroupById(@PathVariable String id) {
        ReleaseGroup releaseGroup = releaseGroupRepository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Release Group not found with ID: " + id));
        return ResponseEntity.ok(releaseGroup);
    }

    @GetMapping("/{id}/full")
    public ResponseEntity<ReleaseGroupFullDto> getReleaseGroupFullById(@PathVariable String id) {
        ReleaseGroup releaseGroup = releaseGroupRepository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Release Group not found with ID: " + id));

        ReleaseGroupFullDto releaseGroupFullDto = new ReleaseGroupFullDto(releaseGroup);

        // 1. Fetch child Releases and their nested structure
        if (releaseGroup.getReleaseIds() != null && !releaseGroup.getReleaseIds().isEmpty()) {
            List<Release> childReleases = releaseRepository.findAllById(releaseGroup.getReleaseIds());
            List<ReleaseFullDto> releaseFullDtos = new ArrayList<>();

            for (Release release : childReleases) {
                ReleaseFullDto releaseFullDto = new ReleaseFullDto(release);
                
                // Fetch Phases for this Release (ordered by previous/next if possible)
                List<Phase> releasePhases = phaseRepository.findByParentIdAndParentType(release.getId(), Phase.ParentType.RELEASE);
                // Sort phases by their linked list order
                releasePhases = sortPhasesByLinkedList(releasePhases);

                List<PhaseFullDto> phaseFullDtos = new ArrayList<>();

                for (Phase phase : releasePhases) {
                    PhaseFullDto phaseFullDto = new PhaseFullDto(phase);

                    // Fetch Tasks for this Phase and recursively populate children
                    // We need to ensure that the order of tasks from the phase's taskIds is maintained
                    List<TaskDto> taskDtos = new ArrayList<>();
                    if (phase.getTaskIds() != null) {
                        List<Task> rawTasks = taskRepository.findAllById(phase.getTaskIds());
                        rawTasks = sortTasksByLinkedList(rawTasks); // Sort tasks by linked list order

                        for (Task task : rawTasks) {
                            taskDtos.add(populateChildrenTasks(task, taskRepository)); // Recursively map and add in order
                        }
                    }
                    phaseFullDto.setTasks(taskDtos);
                    phaseFullDtos.add(phaseFullDto);
                }
                releaseFullDto.setPhases(phaseFullDtos);
                releaseFullDtos.add(releaseFullDto);
            }
            releaseGroupFullDto.setReleases(releaseFullDtos);
        }

        // 2. Fetch direct child Phases (those directly under the ReleaseGroup) and their nested tasks
        if (releaseGroup.getPhaseIds() != null && !releaseGroup.getPhaseIds().isEmpty()) {
            List<Phase> directPhases = phaseRepository.findAllById(releaseGroup.getPhaseIds());
            // Sort phases by their linked list order
            directPhases = sortPhasesByLinkedList(directPhases);

            List<PhaseFullDto> directPhaseFullDtos = new ArrayList<>();

            for (Phase phase : directPhases) {
                PhaseFullDto phaseFullDto = new PhaseFullDto(phase);

                // Fetch Tasks for this direct Phase and recursively populate children
                List<TaskDto> taskDtos = new ArrayList<>();
                if (phase.getTaskIds() != null) {
                    List<Task> rawTasks = taskRepository.findAllById(phase.getTaskIds());
                    rawTasks = sortTasksByLinkedList(rawTasks); // Sort tasks by linked list order
                    for (Task task : rawTasks) {
                        taskDtos.add(populateChildrenTasks(task, taskRepository)); // Recursively map and add in order
                    }
                }
                phaseFullDto.setTasks(taskDtos);
                directPhaseFullDtos.add(phaseFullDto);
            }
            releaseGroupFullDto.setDirectPhases(directPhaseFullDtos);
        }

        return ResponseEntity.ok(releaseGroupFullDto);
    }

    // Helper method to recursively populate children tasks for group tasks
    private TaskDto populateChildrenTasks(Task task, TaskRepository taskRepository) {
        TaskDto taskDto = new TaskDto(task);

        if (task.getType() == Task.TaskType.PARALLEL_GROUP || task.getType() == Task.TaskType.SEQUENTIAL_GROUP) {
            // Now directly access childTaskIds from the Task entity
            List<String> childTaskIds = task.getChildTaskIds();
            if (childTaskIds != null && !childTaskIds.isEmpty()) {
                List<Task> rawChildren = taskRepository.findAllById(childTaskIds);
                List<TaskDto> childrenDtos = new ArrayList<>();

                // If it's a SEQUENTIAL_GROUP, sort children by linked list
                if (task.getType() == Task.TaskType.SEQUENTIAL_GROUP) {
                    rawChildren = sortTasksByLinkedList(rawChildren);
                }
                // For PARALLEL_GROUP, order doesn't strictly matter for execution, but maintain consistent view
                // if there's no previous/next set on parallel children, just use existing order or natural ID sort.

                for (Task childTask : rawChildren) {
                    childrenDtos.add(populateChildrenTasks(childTask, taskRepository)); // Recursive call
                }
                taskDto.setChildrenTasks(childrenDtos);
            }
        }
        return taskDto;
    }

    // Helper to sort a list of tasks based on previousTaskId/nextTaskId
    private List<Task> sortTasksByLinkedList(List<Task> tasks) {
        if (tasks == null || tasks.size() <= 1) {
            return tasks;
        }

        List<Task> sortedTasks = new ArrayList<>();
        // Find the starting task (one with no previousTaskId in the given list)
        Task currentTask = tasks.stream()
                                .filter(t -> t.getPreviousTaskId() == null || tasks.stream().noneMatch(other -> other.getId().equals(t.getPreviousTaskId())))
                                .findFirst()
                                .orElse(null);

        // If no explicit start, pick the first one and try to build from there
        if (currentTask == null) {
            currentTask = tasks.get(0); // Fallback
        }

        int safetyCounter = 0;
        final int maxIterations = tasks.size() * 2; // Prevent infinite loops for bad links

        while (currentTask != null && !sortedTasks.contains(currentTask) && safetyCounter < maxIterations) {
            sortedTasks.add(currentTask);
            String nextId = currentTask.getNextTaskId();
            currentTask = tasks.stream().filter(t -> t.getId().equals(nextId)).findFirst().orElse(null);
            safetyCounter++;
        }

        // Add any tasks not found in the sequence (e.g., if there are broken links or multiple starting points)
        // This ensures all tasks are returned, even if the linking is imperfect.
        for (Task task : tasks) {
            if (!sortedTasks.contains(task)) {
                sortedTasks.add(task);
            }
        }

        return sortedTasks;
    }

    // Helper to sort a list of phases based on previousPhaseId/nextPhaseId
    private List<Phase> sortPhasesByLinkedList(List<Phase> phases) {
        if (phases == null || phases.size() <= 1) {
            return phases;
        }

        List<Phase> sortedPhases = new ArrayList<>();
        // Find the starting phase (one with no previousPhaseId in the given list)
        Phase currentPhase = phases.stream()
                                .filter(p -> p.getPreviousPhaseId() == null || phases.stream().noneMatch(other -> other.getId().equals(p.getPreviousPhaseId())))
                                .findFirst()
                                .orElse(null);

        if (currentPhase == null) {
            currentPhase = phases.get(0); // Fallback if no clear start
        }

        int safetyCounter = 0;
        final int maxIterations = phases.size() * 2; // Prevent infinite loops

        while (currentPhase != null && !sortedPhases.contains(currentPhase) && safetyCounter < maxIterations) {
            sortedPhases.add(currentPhase);
            String nextId = currentPhase.getNextPhaseId();
            currentPhase = phases.stream().filter(p -> p.getId().equals(nextId)).findFirst().orElse(null);
            safetyCounter++;
        }

        // Add any phases not found in the sequence
        for (Phase phase : phases) {
            if (!sortedPhases.contains(phase)) {
                sortedPhases.add(phase);
            }
        }

        return sortedPhases;
    }


    @PutMapping("/{id}")
    public ResponseEntity<ReleaseGroup> updateReleaseGroup(@PathVariable String id, @RequestBody ReleaseGroup releaseGroupDetails) {
        ReleaseGroup releaseGroup = releaseGroupRepository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Release Group not found with ID: " + id));

        releaseGroup.setName(releaseGroupDetails.getName());
        releaseGroup.setDescription(releaseGroupDetails.getDescription());
        if (releaseGroupDetails.getState() != null && !releaseGroupDetails.getState().trim().isEmpty()) {
            releaseGroup.setState(releaseGroupDetails.getState());
        }
        ReleaseGroup updatedReleaseGroup = releaseGroupRepository.save(releaseGroup);
        return ResponseEntity.ok(updatedReleaseGroup);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteReleaseGroup(@PathVariable String id) {
        if (!releaseGroupRepository.existsById(id)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Release Group not found with ID: " + id);
        }
        releaseGroupRepository.deleteById(id);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
}

// src/main/java/com/reflow/controllers/ReleaseController.java
package com.reflow.controllers;

import com.reflow.dtos.PhaseFullDto;
import com.reflow.dtos.ReleaseFullDto;
import com.reflow.dtos.TaskDto;
import com.reflow.models.Phase;
import com.reflow.models.Release;
import com.reflow.models.ReleaseGroup;
import com.reflow.models.State;
import com.reflow.models.Task;
import com.reflow.repositories.PhaseRepository;
import com.reflow.repositories.ReleaseGroupRepository;
import com.reflow.repositories.ReleaseRepository;
import com.reflow.repositories.TaskRepository;
import com.reflow.services.CustomIdGenerator;
import com.reflow.services.ReleaseCreationService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/releases")
@RequiredArgsConstructor
public class ReleaseController {

    private final ReleaseRepository releaseRepository;
    private final ReleaseGroupRepository releaseGroupRepository;
    private final PhaseRepository phaseRepository;
    private final TaskRepository taskRepository;
    private final CustomIdGenerator idGenerator;
    private final ReleaseCreationService releaseCreationService;

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

    @PostMapping
    public ResponseEntity<Release> createRelease(@RequestBody Release release) {
        if (release.getName() == null || release.getName().trim().isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Release name is required.");
        }
        Release savedRelease = releaseCreationService.createRelease(release);
        return new ResponseEntity<>(savedRelease, HttpStatus.CREATED);
    }

    @GetMapping
    public ResponseEntity<List<Release>> getAllReleases(@RequestParam(required = false) String releaseGroupId) {
        List<Release> releases;
        if (releaseGroupId != null && !releaseGroupId.trim().isEmpty()) {
            releases = releaseRepository.findByReleaseGroupId(releaseGroupId);
        } else {
            releases = releaseRepository.findAll();
        }
        return ResponseEntity.ok(releases);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Release> getReleaseById(@PathVariable String id) {
        Release release = releaseRepository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Release not found with ID: " + id));
        return ResponseEntity.ok(release);
    }

    @GetMapping("/{id}/full")
    public ResponseEntity<ReleaseFullDto> getReleaseFullById(@PathVariable String id) {
        Release release = releaseRepository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Release not found with ID: " + id));

        ReleaseFullDto releaseFullDto = new ReleaseFullDto(release);

        // Fetch Phases for this Release
        if (release.getPhaseIds() != null && !release.getPhaseIds().isEmpty()) {
            List<Phase> releasePhases = phaseRepository.findAllById(release.getPhaseIds());
            releasePhases = sortPhasesByLinkedList(releasePhases); // Sort phases by linked list order

            List<PhaseFullDto> phaseFullDtos = new ArrayList<>();

            for (Phase phase : releasePhases) {
                PhaseFullDto phaseFullDto = new PhaseFullDto(phase);

                // Fetch Tasks for this Phase and recursively populate children
                // We need to ensure that the order of tasks from the phase's taskIds is maintained
                List<TaskDto> taskDtos = new ArrayList<>();
                if (phase.getTaskIds() != null) {
                    List<Task> rawTasks = taskRepository.findAllById(phase.getTaskIds());
                    rawTasks = sortTasksByLinkedList(rawTasks); // Sort tasks by linked list order

                    for (Task task : rawTasks) {
                        taskDtos.add(populateChildrenTasks(task, taskRepository)); // Recursively map and add in order
                    }
                }
                phaseFullDto.setTasks(taskDtos);
                phaseFullDtos.add(phaseFullDto);
            }
            releaseFullDto.setPhases(phaseFullDtos);
        }

        return ResponseEntity.ok(releaseFullDto);
    }

    // Helper method to recursively populate children tasks for group tasks
    private TaskDto populateChildrenTasks(Task task, TaskRepository taskRepository) {
        TaskDto taskDto = new TaskDto(task);

        if (task.getType() == Task.TaskType.PARALLEL_GROUP || task.getType() == Task.TaskType.SEQUENTIAL_GROUP) {
            // Now directly access childTaskIds from the Task entity
            List<String> childTaskIds = task.getChildTaskIds();
            if (childTaskIds != null && !childTaskIds.isEmpty()) {
                List<Task> rawChildren = taskRepository.findAllById(childTaskIds);
                List<TaskDto> childrenDtos = new ArrayList<>();

                // If it's a SEQUENTIAL_GROUP, sort children by linked list
                if (task.getType() == Task.TaskType.SEQUENTIAL_GROUP) {
                    rawChildren = sortTasksByLinkedList(rawChildren);
                }
                // For PARALLEL_GROUP, order doesn't strictly matter for execution, but maintain consistent view
                // if there's no previous/next set on parallel children, just use existing order or natural ID sort.

                for (Task childTask : rawChildren) {
                    childrenDtos.add(populateChildrenTasks(childTask, taskRepository)); // Recursive call
                }
                taskDto.setChildrenTasks(childrenDtos);
            }
        }
        return taskDto;
    }

    // Helper to sort a list of tasks based on previousTaskId/nextTaskId
    private List<Task> sortTasksByLinkedList(List<Task> tasks) {
        if (tasks == null || tasks.size() <= 1) {
            return tasks;
        }

        List<Task> sortedTasks = new ArrayList<>();
        // Find the starting task (one with no previousTaskId in the given list)
        Task currentTask = tasks.stream()
                                .filter(t -> t.getPreviousTaskId() == null || tasks.stream().noneMatch(other -> other.getId().equals(t.getPreviousTaskId())))
                                .findFirst()
                                .orElse(null);

        if (currentTask == null) {
            currentTask = tasks.get(0); // Fallback
        }

        int safetyCounter = 0;
        final int maxIterations = tasks.size() * 2; // Prevent infinite loops for bad links

        while (currentTask != null && !sortedTasks.contains(currentTask) && safetyCounter < maxIterations) {
            sortedTasks.add(currentTask);
            String nextId = currentTask.getNextTaskId();
            currentTask = tasks.stream().filter(t -> t.getId().equals(nextId)).findFirst().orElse(null);
            safetyCounter++;
        }
        // Add any tasks not found in the sequence (e.g., if there are broken links or multiple starting points)
        for (Task task : tasks) {
            if (!sortedTasks.contains(task)) {
                sortedTasks.add(task);
            }
        }

        return sortedTasks;
    }

    // Helper to sort a list of phases based on previousPhaseId/nextPhaseId
    private List<Phase> sortPhasesByLinkedList(List<Phase> phases) {
        if (phases == null || phases.size() <= 1) {
            return phases;
        }

        List<Phase> sortedPhases = new ArrayList<>();
        // Find the starting phase (one with no previousPhaseId in the given list)
        Phase currentPhase = phases.stream()
                                .filter(p -> p.getPreviousPhaseId() == null || phases.stream().noneMatch(other -> other.getId().equals(p.getPreviousPhaseId())))
                                .findFirst()
                                .orElse(null);

        if (currentPhase == null) {
            currentPhase = phases.get(0); // Fallback if no clear start
        }

        int safetyCounter = 0;
        final int maxIterations = phases.size() * 2; // Prevent infinite loops

        while (currentPhase != null && !sortedPhases.contains(currentPhase) && safetyCounter < maxIterations) {
            sortedPhases.add(currentPhase);
            String nextId = currentPhase.getNextPhaseId();
            currentPhase = phases.stream().filter(p -> p.getId().equals(nextId)).findFirst().orElse(null);
            safetyCounter++;
        }

        // Add any phases not found in the sequence
        for (Phase phase : phases) {
            if (!sortedPhases.contains(phase)) {
                sortedPhases.add(phase);
            }
        }

        return sortedPhases;
    }

    @PutMapping("/{id}")
    public ResponseEntity<Release> updateRelease(@PathVariable String id, @RequestBody Release releaseDetails) {
        Release release = releaseRepository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Release not found with ID: " + id));

        release.setName(releaseDetails.getName());
        release.setDescription(releaseDetails.getDescription());

        String newReleaseGroupId = releaseDetails.getReleaseGroupId();
        String currentReleaseGroupId = release.getReleaseGroupId();

        if (newReleaseGroupId != null && !newReleaseGroupId.trim().isEmpty()) {
            if (currentReleaseGroupId == null || currentReleaseGroupId.trim().isEmpty()) {
                ReleaseGroup targetReleaseGroup = releaseGroupRepository.findById(newReleaseGroupId)
                        .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Target Release Group not found with ID: " + newReleaseGroupId));

                if (!isActiveState(release.getState())) {
                    throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Release is not in an active state for group assignment.");
                }
                if (!isActiveState(targetReleaseGroup.getState())) {
                    throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Target Release Group is not in an active state for assignment.");
                }

                release.setReleaseGroupId(newReleaseGroupId);
                System.out.println(String.format("Release %s mapped to Release Group %s.", id, newReleaseGroupId));

                if (!targetReleaseGroup.getReleaseIds().contains(release.getId())) {
                    targetReleaseGroup.getReleaseIds().add(release.getId());
                    releaseGroupRepository.save(targetReleaseGroup);
                }

            } else if (!newReleaseGroupId.equals(currentReleaseGroupId)) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Release is already mapped to a Release Group and cannot be re-mapped.");
            }
        } else if (currentReleaseGroupId != null && !currentReleaseGroupId.trim().isEmpty() && (newReleaseGroupId == null || newReleaseGroupId.trim().isEmpty())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Release cannot be unmapped from its current Release Group.");
        }


        if (releaseDetails.getState() != null && !releaseDetails.getState().trim().isEmpty()) {
            release.setState(releaseDetails.getState());
        }

        Release updatedRelease = releaseRepository.save(release);
        return ResponseEntity.ok(updatedRelease);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteRelease(@PathVariable String id) {
        if (!releaseRepository.existsById(id)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Release not found with ID: " + id);
        }
        releaseRepository.deleteById(id);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
}

// src/main/java/com/reflow/controllers/PhaseController.java
package com.reflow.controllers;

import com.reflow.models.Phase;
import com.reflow.models.Phase.ParentType;
import com.reflow.repositories.PhaseRepository;
import com.reflow.repositories.ReleaseGroupRepository;
import com.reflow.repositories.ReleaseRepository;
import com.reflow.services.CustomIdGenerator;
import com.reflow.services.PhaseCreationService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList; // NEW: for sorting
import java.util.List;
import java.util.Map; // NEW: for sorting
import java.util.Comparator; // NEW: for sorting
import java.util.stream.Collectors; // NEW: for sorting

@RestController
@RequestMapping("/api/phases")
@RequiredArgsConstructor
public class PhaseController {

    private final PhaseRepository phaseRepository;
    private final ReleaseGroupRepository releaseGroupRepository;
    private final ReleaseRepository releaseRepository;
    private final CustomIdGenerator idGenerator;
    private final PhaseCreationService phaseCreationService;

    @PostMapping
    public ResponseEntity<Phase> createPhase(@RequestBody Phase phase) {
        if (phase.getName() == null || phase.getName().trim().isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Phase name is required.");
        }
        Phase savedPhase = phaseCreationService.createPhase(phase);
        return new ResponseEntity<>(savedPhase, HttpStatus.CREATED);
    }

    @GetMapping
    public ResponseEntity<List<Phase>> getAllPhases(
            @RequestParam(required = false) String parentId,
            @RequestParam(required = false) ParentType parentType) {
        List<Phase> phases;
        if (parentId != null && !parentId.trim().isEmpty() && parentType != null) {
            phases = phaseRepository.findByParentIdAndParentType(parentId, parentType);
            phases = sortPhasesByLinkedList(phases); // Sort phases for consistent order
        } else {
            phases = phaseRepository.findAll();
        }
        return ResponseEntity.ok(phases);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Phase> getPhaseById(@PathVariable String id) {
        Phase phase = phaseRepository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Phase not found with ID: " + id));
        return ResponseEntity.ok(phase);
    }

    @PutMapping("/{id}")
    public ResponseEntity<Phase> updatePhase(@PathVariable String id, @RequestBody Phase phaseDetails) {
        Phase phase = phaseRepository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Phase not found with ID: " + id));

        phase.setName(phaseDetails.getName());
        phase.setDescription(phaseDetails.getDescription());
        if (phaseDetails.getState() != null && !phaseDetails.getState().trim().isEmpty()) {
            phase.setState(phaseDetails.getState());
        }
        // Allow updating previousPhaseId and nextPhaseId via PUT, but creation service should manage linking
        phase.setPreviousPhaseId(phaseDetails.getPreviousPhaseId());
        phase.setNextPhaseId(phaseDetails.getNextPhaseId());

        Phase updatedPhase = phaseRepository.save(phase);
        return ResponseEntity.ok(updatedPhase);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deletePhase(@PathVariable String id) {
        // TODO: Implement proper deletion logic that also updates previous/next links
        if (!phaseRepository.existsById(id)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Phase not found with ID: " + id);
        }
        phaseRepository.deleteById(id);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

    // Helper to sort a list of phases based on previousPhaseId/nextPhaseId (duplicated from ReleaseController)
    // In a larger app, this would be a shared utility.
    private List<Phase> sortPhasesByLinkedList(List<Phase> phases) {
        if (phases == null || phases.size() <= 1) {
            return phases;
        }

        List<Phase> sortedPhases = new ArrayList<>();
        // Find the starting phase (one with no previousPhaseId in the given list)
        Phase currentPhase = phases.stream()
                                .filter(p -> p.getPreviousPhaseId() == null || phases.stream().noneMatch(other -> other.getId().equals(p.getPreviousPhaseId())))
                                .findFirst()
                                .orElse(null);

        if (currentPhase == null) {
            // Fallback: If no clear start, find the one that is NOT a 'next' of any other phase in the list
            currentPhase = phases.stream()
                .filter(p -> phases.stream().noneMatch(other -> p.getId().equals(other.getNextPhaseId())))
                .findFirst()
                .orElse(phases.get(0)); // Last resort, just pick the first
        }

        int safetyCounter = 0;
        final int maxIterations = phases.size() * 2; // Prevent infinite loops

        while (currentPhase != null && !sortedPhases.contains(currentPhase) && safetyCounter < maxIterations) {
            sortedPhases.add(currentPhase);
            String nextId = currentPhase.getNextPhaseId();
            currentPhase = phases.stream().filter(p -> p.getId().equals(nextId)).findFirst().orElse(null);
            safetyCounter++;
        }

        // Add any phases not found in the sequence (e.g., if there are broken links or multiple starting points)
        for (Phase phase : phases) {
            if (!sortedPhases.contains(phase)) {
                sortedPhases.add(phase);
            }
        }

        return sortedPhases;
    }
}

// src/main/java/com/reflow/controllers/TaskController.java
package com.reflow.controllers;

import com.reflow.models.Task;
import com.reflow.models.Task.TaskType;
import com.reflow.models.Task.GateStatus;
import com.reflow.repositories.PhaseRepository;
import com.reflow.repositories.ReleaseRepository;
import com.reflow.repositories.TaskRepository;
import com.reflow.services.ActionService;
import com.reflow.services.CustomIdGenerator;
import com.reflow.services.TaskCreationService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/tasks")
@RequiredArgsConstructor
public class TaskController {

    private final TaskRepository taskRepository;
    private final PhaseRepository phaseRepository;
    private final ReleaseRepository releaseRepository;
    private final CustomIdGenerator idGenerator;
    private final TaskCreationService taskCreationService;
    private final ActionService actionService;

    @PostMapping
    public ResponseEntity<Task> createTask(@RequestBody Task task) {
        if (task.getName() == null || task.getName().trim().isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Task name is required.");
        }
        Task savedTask = taskCreationService.createTask(task);
        return new ResponseEntity<>(savedTask, HttpStatus.CREATED);
    }

    @GetMapping
    public ResponseEntity<List<Task>> getAllTasks(
            @RequestParam(required = false) String phaseId,
            @RequestParam(required = false) String releaseId,
            @RequestParam(required = false) String state,
            @RequestParam(required = false) String groupId,
            @RequestParam(required = false) String actor) {
        List<Task> tasks;
        if (phaseId != null && !phaseId.trim().isEmpty()) {
            tasks = taskRepository.findByPhaseId(phaseId);
        } else if (releaseId != null && !releaseId.trim().isEmpty()) {
            tasks = taskRepository.findByReleaseId(releaseId);
        } else if (state != null && !state.trim().isEmpty()) {
            try {
                tasks = taskRepository.findByState(com.reflow.models.State.valueOf(state.toUpperCase()));
            } catch (IllegalArgumentException e) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid state value: " + state);
            }
        } else if (groupId != null && !groupId.trim().isEmpty()) {
            tasks = taskRepository.findByGroupId(groupId);
        } else if (actor != null && !actor.trim().isEmpty()) {
            tasks = taskRepository.findByActor(actor); // Assuming findByActor method exists in repository
        }
        else {
            tasks = taskRepository.findAll();
        }
        return ResponseEntity.ok(tasks);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Task> getTaskById(@PathVariable String id) {
        Task task = taskRepository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Task not found with ID: " + id));
        return ResponseEntity.ok(task);
    }

    @PutMapping("/{id}")
    public ResponseEntity<Task> updateTask(@PathVariable String id, @RequestBody Task taskDetails) {
        Task task = taskRepository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Task not found with ID: " + id));

        if (!actionService.isTaskModifiable(task)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                String.format("Task '%s' in state '%s' cannot be modified. Ensure it's PLANNED and its parents are not in a terminal state.",
                    id, task.getState()));
        }

        task.setName(taskDetails.getName());
        task.setDescription(taskDetails.getDescription());
        task.setType(taskDetails.getType());
        task.setPreviousTaskId(taskDetails.getPreviousTaskId());
        task.setNextTaskId(taskDetails.getNextTaskId());
        task.setGroupId(taskDetails.getGroupId());
        // NEW: Handle direct childTaskIds field
        if (taskDetails.getChildTaskIds() != null) {
            task.setChildTaskIds(taskDetails.getChildTaskIds());
        }
        task.setScheduledTime(taskDetails.getScheduledTime());
        task.setApprovers(taskDetails.getApprovers());
        task.setApprovedBy(taskDetails.getApprovedBy());
        task.setApprovalDate(taskDetails.getApprovalDate());
        task.setReason(taskDetails.getReason());
        task.setActor(taskDetails.getActor());
        
        if (taskDetails.getTaskVariables() != null) {
            task.setTaskVariables(taskDetails.getTaskVariables());
        }
        if (taskDetails.getLogs() != null) {
            task.setLogs(taskDetails.getLogs());
        }
        if (taskDetails.getTriggerInterval() != null) {
            task.setTriggerInterval(taskDetails.getTriggerInterval());
        }
        if (taskDetails.getInputVariables() != null) {
            task.setInputVariables(taskDetails.getInputVariables());
        }
        if (taskDetails.getOutputVariables() != null) {
            task.setOutputVariables(taskDetails.getOutputVariables());
        }


        if (taskDetails.getState() != null && !taskDetails.getState().trim().isEmpty()) {
            task.setState(taskDetails.getState());
        }

        task.setGate(taskDetails.isGate());
        if (task.isGate()) {
            if (taskDetails.getGateStatus() != null) {
                task.setGateStatus(taskDetails.getGateStatus());
            }
        } else {
            task.setGateStatus(null);
        }

        Task updatedTask = taskRepository.save(task);
        return ResponseEntity.ok(updatedTask);
    }

    @PostMapping("/{id}/move")
    public ResponseEntity<Task> moveTask(@PathVariable String id, @RequestBody Map<String, String> payload) {
        String newPreviousTaskId = payload.get("newPreviousTaskId");
        String newNextTaskId = payload.get("newNextTaskId");
        Task updatedTask = actionService.moveTask(id, newPreviousTaskId, newNextTaskId);
        return ResponseEntity.ok(updatedTask);
    }

    @PostMapping("/{id}/variables")
    public ResponseEntity<Task> updateTaskVariables(@PathVariable String id, @RequestBody Map<String, Object> variables) {
        Task updatedTask = actionService.updateTaskVariables(id, variables);
        return ResponseEntity.ok(updatedTask);
    }

    @PostMapping("/{id}/logs")
    public ResponseEntity<Task> appendTaskLogs(@PathVariable String id, @RequestBody Map<String, String> payload) {
        String newLogContent = payload.get("content");
        if (newLogContent == null || newLogContent.trim().isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Log content is required.");
        }
        Task updatedTask = actionService.appendTaskLogs(id, newLogContent);
        return ResponseEntity.ok(updatedTask);
    }


    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteTask(@PathVariable String id) {
        if (!taskRepository.existsById(id)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Task not found with ID: " + id);
        }
        taskRepository.deleteById(id);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
}

// src/main/java/com/reflow/controllers/ActionController.java
package com.reflow.controllers;

import com.reflow.models.BaseEntity;
import com.reflow.services.ActionService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/actions")
@RequiredArgsConstructor
public class ActionController {

    private final ActionService actionService;

    @PostMapping("/transition-state")
    public ResponseEntity<BaseEntity> transitionState(@RequestBody Map<String, String> payload) {
        String entityType = payload.get("entityType");
        String entityId = payload.get("entityId");
        String newState = payload.get("newState");
        String approvedBy = payload.get("approvedBy");
        String reason = payload.get("reason");

        BaseEntity updatedEntity = actionService.transitionState(entityType, entityId, newState, approvedBy, reason);
        return ResponseEntity.ok(updatedEntity);
    }
}
