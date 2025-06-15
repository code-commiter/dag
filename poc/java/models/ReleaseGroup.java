// src/main/java/com/reflow/models/ReleaseGroup.java
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

// Defines the collection name in MongoDB
@Document(collection = "releaseGroups")
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true) // Include superclass fields in equals and hashCode
public class ReleaseGroup extends BaseEntity {
    @Id // Marks this field as the document's primary key
    private String id;
    private String name;
    private String description;

    @Field("releaseIds")
    private List<String> releaseIds = new ArrayList<>(); // List of child Release IDs

    @Field("phaseIds")
    private List<String> phaseIds = new ArrayList<>(); // List of child Phase IDs directly under this ReleaseGroup

    @Field("releaseContextId")
    private String releaseContextId; // Link to its ReleaseContext document
}
