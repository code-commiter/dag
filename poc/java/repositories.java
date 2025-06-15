// src/main/java/com/reflow/repositories/ReleaseGroupRepository.java
package com.reflow.repositories;

import com.reflow.models.ReleaseGroup;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

// Spring Data MongoDB will automatically implement this interface
@Repository
public interface ReleaseGroupRepository extends MongoRepository<ReleaseGroup, String> {
}

// src/main/java/com/reflow/repositories/ReleaseRepository.java
package com.reflow.repositories;

import com.reflow.models.Release;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ReleaseRepository extends MongoRepository<Release, String> {
    // Custom query method to find releases by releaseGroupId
    List<Release> findByReleaseGroupId(String releaseGroupId);
}

// src/main/java/com/reflow/repositories/PhaseRepository.java
package com.reflow.repositories;

import com.reflow.models.Phase;
import com.reflow.models.Phase.ParentType;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional; // NEW: Import Optional

@Repository
public interface PhaseRepository extends MongoRepository<Phase, String> {
    // Custom query method to find phases by parentId and parentType
    List<Phase> findByParentIdAndParentType(String parentId, ParentType parentType);

    // This method is primarily for finding specific phases by name, if needed by Airflow/other services
    // The gate logic is now on tasks, but phases might still need to be identifiable by name.
    List<Phase> findByParentIdAndParentTypeAndName(String parentId, ParentType parentType, String name);

    // NEW: Find a phase by its nextPhaseId (for updating previous phase when inserting)
    Optional<Phase> findByNextPhaseId(String nextPhaseId);

    // NEW: Find the starting phase for a given parent (no previous phase id)
    Optional<Phase> findByParentIdAndParentTypeAndPreviousPhaseIdIsNull(String parentId, ParentType parentType);

}

// src/main/java/com/reflow/repositories/TaskRepository.java
package com.reflow.repositories;

import com.reflow.models.State;
import com.reflow.models.Task;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional; // Import Optional

@Repository
public interface TaskRepository extends MongoRepository<Task, String> {
    // Custom query methods for tasks
    List<Task> findByPhaseId(String phaseId);
    List<Task> findByReleaseId(String releaseId);
    List<Task> findByState(State state); // For Airflow to find QUEUED tasks
    Task findByPreviousTaskId(String previousTaskId); // For finding next task
    List<Task> findByGroupId(String groupId); // New: to find tasks within a group

    // New: Find a gate task within a specific phase by its name (for cascading)
    List<Task> findByPhaseIdAndIsGateIsTrueAndName(String phaseId, String name);

    // New: Find a gate task within a specific phase by its gateCategory and name (for cascading)
    List<Task> findByPhaseIdAndIsGateIsTrueAndGateCategoryAndName(String phaseId, String gateCategory, String name);

    // New: Find a task by its nextTaskId (to break existing links during move)
    Optional<Task> findByNextTaskId(String nextTaskId); // Returns Optional to handle not found

    // New: Find the starting task within a phase (no groupId)
    List<Task> findByPhaseIdAndPreviousTaskIdIsNullAndGroupIdIsNull(String phaseId);

    // New: Find the starting task within a specific group
    List<Task> findByGroupIdAndPreviousTaskIdIsNull(String groupId);
}

// src/main/java/com/reflow/repositories/ReleaseContextRepository.java
package com.reflow.repositories;

import com.reflow.models.ReleaseContext;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ReleaseContextRepository extends MongoRepository<ReleaseContext, String> {
}
