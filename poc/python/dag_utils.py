# dags/reflow_dag_utils.py
# This file contains common utility functions and constants used across
# various Reflow-related Airflow DAGs.

from __future__ import annotations

import pendulum
import json

from airflow.exceptions import AirflowFailException

from confluent_kafka import Producer, KafkaException # Only Producer is used here
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

# Import configuration constants from the centralized config file
from airflow_config import (
    KAFKA_BROKER_SERVERS,
    KAFKA_TASK_EVENTS_TOPIC, # Used for publishing messages
    MONGO_HOST,
    MONGO_PORT,
    MONGO_DB_NAME,
    APPROVER_NAME_SYSTEM # Default approver name for general utilities
)

# --- Common Utility Functions ---

def get_mongo_client(approver_name: str = APPROVER_NAME_SYSTEM):
    """Establishes and returns a MongoDB client connection."""
    try:
        client = MongoClient(MONGO_HOST, MONGO_PORT)
        client.admin.command('ping') # Test connection
        print(f"[{approver_name}] Successfully connected to MongoDB at {MONGO_HOST}:{MONGO_PORT}")
        return client
    except ConnectionFailure as e:
        print(f"[{approver_name}] MongoDB connection failed: {e}")
        raise AirflowFailException(f"[{approver_name}] Failed to connect to MongoDB: {e}")
    except Exception as e:
        print(f"[{approver_name}] An unexpected error occurred during MongoDB connection: {e}")
        raise AirflowFailException(f"[{approver_name}] Failed to establish MongoDB connection: {e}")

def update_entity_state_in_mongodb(db, collection_name, entity_id, new_state, old_state_condition=None, additional_fields=None, approver_name: str = APPROVER_NAME_SYSTEM):
    """
    Generic helper to update an entity's state in MongoDB.
    Includes an optional 'old_state_condition' for optimistic locking/concurrency control.
    :param db: The MongoDB database object.
    :param collection_name: The name of the collection (e.g., "tasks", "phases").
    :param entity_id: The _id of the document to update.
    :param new_state: The new state string.
    :param old_state_condition: Optional: The state the document MUST be in for the update to succeed.
                                If None, no old state check is performed.
    :param additional_fields: Optional dictionary of other fields to set.
    :param approver_name: The name of the system/user performing the action.
    :return: True if update was successful (or no change needed), False otherwise (e.g., concurrent update).
    """
    query = {"_id": entity_id}
    if old_state_condition:
        query["state"] = old_state_condition

    update_set = {"state": new_state, "updatedAt": pendulum.now("UTC").isoformat()}
    if additional_fields:
        update_set.update(additional_fields)

    result = db[collection_name].update_one(
        query,
        {"$set": update_set}
    )
    if result.modified_count == 0 and result.matched_count == 0:
        print(f"[{approver_name}] Warning: Entity {collection_name}:{entity_id} not found or state condition '{old_state_condition}' not met. Actual state might be different.")
        return False
    elif result.modified_count == 0 and result.matched_count == 1:
        print(f"[{approver_name}] Info: Entity {collection_name}:{entity_id} state already {new_state}. No modification needed.")
        return True
    else:
        print(f"[{approver_name}] Entity {collection_name}:{entity_id} state successfully updated to: {new_state}.")
        return True

def get_terminal_states():
    """Returns a list of states considered 'terminal' for workflow progression."""
    return ["COMPLETED", "SKIPPED", "REJECTED", "ARCHIVED", "APPROVED", "FAILED"]

def publish_kafka_message(topic, message_key, message_value, approver_name: str = APPROVER_NAME_SYSTEM):
    """
    Publishes a message to a Kafka topic.
    :param topic: The Kafka topic to publish to.
    :param message_key: The key for the Kafka message (e.g., task_id).
    :param message_value: The value for the Kafka message (dict, will be JSON serialized).
    :param approver_name: The name of the system/user performing the action.
    """
    producer_conf = {
        'bootstrap.servers': KAFKA_BROKER_SERVERS,
        'acks': 'all',  # Wait for all in-sync replicas to acknowledge the write
        'retries': 3    # Retry up to 3 times on transient errors
    }
    producer = Producer(producer_conf)

    try:
        producer.produce(topic, key=message_key, value=json.dumps(message_value).encode('utf-8'))
        producer.flush(timeout=5) # Ensure the message is sent within 5 seconds
        print(f"[{approver_name}] Successfully published message to Kafka topic '{topic}' with key '{message_key}'")
    except KafkaException as e:
        print(f"[{approver_name}] Failed to deliver message to Kafka topic '{topic}': {e}")
        raise AirflowFailException(f"[{approver_name}] Failed to publish Kafka message: {e}")
    finally:
        producer.poll(0) # Serve callback (not strictly necessary with flush, but good practice)

# You can add other shared utilities here, e.g., logging configuration, API call wrappers if needed etc.

