// src/main/java/com/reflow/config/MongoConfig.java
package com.reflow.config;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

@Configuration
@EnableMongoRepositories(basePackages = "com.reflow.repositories") // Ensure your repository package is scanned
public class MongoConfig {

    // Define your MongoClient bean (if not using Spring Boot's auto-config)
    @Bean
    public MongoClient mongoClient() {
        // Replace with your actual MongoDB connection string
        return MongoClients.create("mongodb://localhost:27017");
    }

    // Define the MongoDatabaseFactory bean
    @Bean
    public MongoDatabaseFactory mongoDatabaseFactory(MongoClient mongoClient) {
        // Replace "your_database_name" with your actual database name
        return new SimpleMongoClientDatabaseFactory(mongoClient, "your_database_name");
    }

    // Define the MongoTemplate bean
    @Bean
    public MongoTemplate mongoTemplate(MongoDatabaseFactory mongoDatabaseFactory) {
        return new MongoTemplate(mongoDatabaseFactory);
    }

    // This is crucial for @Transactional to work with MongoDB
    @Bean
    MongoTransactionManager transactionManager(MongoDatabaseFactory dbFactory) {
        return new MongoTransactionManager(dbFactory);
    }
}
