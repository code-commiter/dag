// src/main/java/com/reflow/models/Phase.java
package com.reflow.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.ArrayList;
import java.util.List;

@Document(collection = "phases")
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class Phase extends BaseEntity {
    @Id
    private String id;
    @Field("parentType")
    public enum ParentType { // Made public
        RELEASE_GROUP,
        RELEASE
    }
    private ParentType parentType; // Enum: RELEASE_GROUP or RELEASE
    @Field("parentId")
    private String parentId; // Reference to the parent's actual ID (releaseGroupId or releaseId)
    private String name;
    private String description;
    // isGate and gateStatus moved to Task.java as per new requirement

    @Field("taskIds")
    private List<String> taskIds = new ArrayList<>(); // List of child Task IDs
}
