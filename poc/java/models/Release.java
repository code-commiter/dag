// src/main/java/com/reflow/models/Release.java
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

@Document(collection = "releases")
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class Release extends BaseEntity {
    @Id
    private String id;
    @Field("releaseGroupId")
    private String releaseGroupId; // Link to parent ReleaseGroup
    private String name;
    private String description;

    @Field("phaseIds")
    private List<String> phaseIds = new ArrayList<>(); // List of child Phase IDs

    @Field("releaseContextId")
    private String releaseContextId; // Link to its ReleaseContext document
}
