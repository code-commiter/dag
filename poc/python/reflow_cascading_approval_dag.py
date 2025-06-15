# dags/reflow_cascading_approval_dag.py
# This Airflow DAG is dedicated to listening for Kafka events that signal
# a cascading approval (or rejection) for Release Group-level gate tasks,
# and then updating the corresponding child gate tasks in MongoDB.
# Refactored to use common utility functions from reflow_dag_utils.py
# and centralized configuration from airflow_config.py.
# UPDATED to use direct 'childTaskIds' field on Task documents.

from __future__ import annotations

import pendulum
import json
import time

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

# Import common utilities and configuration constants
from reflow_dag_utils import (
    get_mongo_client,
    update_entity_state_in_mongodb,
    get_terminal_states,
    KAFKA_BROKER_SERVERS, # Imported for Consumer config
    KAFKA_TASK_EVENTS_TOPIC,
    MONGO_DB_NAME
)

# Import specific approver name for this DAG
from airflow_config import APPROVER_NAME_CASCADER as APPROVER_NAME

# Confluent Kafka Python client - needs to be installed in Airflow environment:
# pip install confluent-kafka
from confluent_kafka import Consumer, KafkaException, OFFSET_BEGINNING


@dag(
    dag_id="reflow_cascading_approval_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="*/1 * * * *",  # Run every 1 minute to check for new Kafka events
    catchup=False,
    tags=["reflow", "kafka", "cascading-approval", "gates", "mongodb"],
    doc_md="""
    ### Reflow Cascading Approval DAG
    This DAG listens to the `reflow_task_events` Kafka topic for events related to
    Release Group-level gate tasks that have been approved or rejected.
    Upon receiving such an event, it will identify corresponding gate tasks in child
    releases and directly update their status in MongoDB. This ensures that a single
    decision at a higher level propagates efficiently down to relevant child workflows.
    Refactored to use `reflow_dag_utils.py` and `airflow_config.py`.
    **UPDATED to use direct 'childTaskIds' field on Task documents.**
    """
)
def reflow_cascading_approval_dag():

    # Kafka consumer group ID specific to this DAG
    KAFKA_CONSUMER_GROUP_ID = "airflow_reflow_cascading_approval_group"

    @task
    def consume_and_filter_cascading_events():
        """
        Consumes messages from the Kafka 'reflow_task_events' topic,
        specifically filtering for gate tasks that have been APPROVED or REJECTED.
        These are the events that signal a cascading action is needed.
        """
        conf = {
            'bootstrap.servers': KAFKA_BROKER_SERVERS,
            'group.id': KAFKA_CONSUMER_GROUP_ID,
            'auto.offset.reset': 'earliest', # Start from beginning for new consumers
            'enable.auto.commit': True,
            'session.timeout.ms': 10000,
            'heartbeat.interval.ms': 3000
        }
        consumer = Consumer(conf)
        cascading_events_for_processing = []

        try:
            consumer.subscribe([KAFKA_TASK_EVENTS_TOPIC])
            print(f"[{APPROVER_NAME}] Subscribed to Kafka topic: {KAFKA_TASK_EVENTS_TOPIC}")

            poll_duration_seconds = 30 # How long to poll for new events in this DAG run
            start_time = time.time()
            while time.time() - start_time < poll_duration_seconds:
                msg = consumer.poll(timeout=1.0) # Poll for 1 second

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().is_fatal():
                        raise KafkaException(msg.error())
                    else:
                        print(f"[{APPROVER_NAME}] Consumer error: {msg.error()}")
                        continue

                try:
                    task_data = json.loads(msg.value().decode('utf-8'))
                    task_id = task_data.get("id")
                    task_state = task_data.get("state")
                    task_name = task_data.get("name")
                    is_gate = task_data.get("isGate", False)
                    actor = task_data.get("actor") # Expected to be SYSTEM_CASCADER from the main DAG

                    print(f"[{APPROVER_NAME}] Consumed message for task: {task_name} (ID: {task_id}), State: {task_state}, IsGate: {is_gate}, Actor: {actor}")

                    # Filter for events that represent a cascaded gate decision
                    # These events are published by `airflow_kafka_task_executor_dag_v4`
                    if is_gate and actor == "SYSTEM_CASCADER" and task_state in ["APPROVED", "REJECTED"]:
                        print(f"[{APPROVER_NAME}] Adding cascaded gate event for processing: {task_name} (ID: {task_id}), State: {task_state}")
                        cascading_events_for_processing.append(task_data)
                    else:
                        print(f"[{APPROVER_NAME}] Task {task_name} (ID: {task_id}) is not a cascaded gate event, skipping.")

                except json.JSONDecodeError as e:
                    print(f"[{APPROVER_NAME}] Error decoding JSON message: {e} - Raw message: {msg.value().decode('utf-8')}")
                except Exception as e:
                    print(f"[{APPROVER_NAME}] Unexpected error processing Kafka message: {e}")

        except KafkaException as e:
            print(f"[{APPROVER_NAME}] Kafka consumer error: {e}")
            raise AirflowFailException(f"[{APPROVER_NAME}] Kafka consumer failed: {e}")
        except Exception as e:
            print(f"[{APPROVER_NAME}] An unexpected error occurred during Kafka consumption: {e}")
            raise AirflowFailException(f"[{APPROVER_NAME}] Failed during Kafka consumption: {e}")
        finally:
            consumer.close()
            print(f"[{APPROVER_NAME}] Kafka consumer closed.")

        if not cascading_events_for_processing:
            print(f"[{APPROVER_NAME}] No new cascading approval events found to process in this run.")
            return None

        return cascading_events_for_processing


    @task
    def process_cascading_approval(cascading_event_data: dict):
        """
        Processes a single cascading approval event. This task directly updates
        the target gate task's state in MongoDB.
        """
        task_id = cascading_event_data.get("id")
        task_name = cascading_event_data.get("name")
        target_state = cascading_event_data.get("state") # APPROVED or REJECTED
        approved_by = cascading_event_data.get("approvedBy", APPROVER_NAME)
        approval_date = cascading_event_data.get("approvalDate", pendulum.now("UTC").isoformat())
        reason = cascading_event_data.get("reason", f"Cascaded approval from higher-level gate by {APPROVER_NAME}")
        
        print(f"[{APPROVER_NAME}] Processing cascading approval for task: {task_name} (ID: {task_id}) to state {target_state}.")

        mongo_client = None
        try:
            mongo_client = get_mongo_client(APPROVER_NAME)
            db = mongo_client[MONGO_DB_NAME]
            
            current_task_doc = db.tasks.find_one({"_id": task_id})
            if not current_task_doc:
                print(f"[{APPROVER_NAME}] Target task {task_id} not found in DB for cascading. Skipping. It might have been deleted or archived.")
                return

            current_task_state = current_task_doc.get("state")

            # Only update if the task is not already in a terminal state
            # or if it's currently waiting for approval and the new state is different
            if current_task_state in get_terminal_states() and current_task_state != "WAITING_FOR_APPROVAL":
                print(f"[{APPROVER_NAME}] Task {task_id} is already in terminal state '{current_task_state}'. No update needed from cascading event.")
                return
            
            if current_task_state != "WAITING_FOR_APPROVAL":
                print(f"[{APPROVER_NAME}] Warning: Task {task_id} is not in 'WAITING_FOR_APPROVAL' state (current: {current_task_state}). Still attempting cascade to {target_state}.")

            # Update the task directly in MongoDB
            update_successful = update_entity_state_in_mongodb(
                db,
                "tasks",
                task_id,
                target_state, # This is the state to which it's being cascaded
                # No old_state_condition here, as we want this DAG to be authoritative for cascaded state
                additional_fields={
                    "gateStatus": target_state, # Update gateStatus to match
                    "approvedBy": approved_by,
                    "approvalDate": approval_date,
                    "reason": reason,
                    "actor": APPROVER_NAME # The actor that performed this cascade
                },
                approver_name=APPROVER_NAME
            )

            if update_successful:
                print(f"[{APPROVER_NAME}] Successfully cascaded gate status for task {task_id} to {target_state}.")
                # If the task reaches a terminal state, also check parent states
                if target_state in get_terminal_states():
                     # Re-fetch the task to get its latest phaseId and groupId
                    updated_task_doc = db.tasks.find_one({"_id": task_id})
                    if updated_task_doc:
                         # Use the same parent state checking logic from the main DAG
                        _check_and_transition_parent_states_for_cascaded_tasks(
                            db, 
                            task_id, 
                            updated_task_doc.get("phaseId"), 
                            updated_task_doc.get("groupId")
                        )
                    else:
                        print(f"[{APPROVER_NAME}] Failed to re-fetch task {task_id} after cascading update for parent state check.")
            else:
                print(f"[{APPROVER_NAME}] Failed to update task {task_id} for cascading approval. It might have been concurrently updated or is no longer eligible.")

        except (ConnectionFailure, OperationFailure) as e:
            print(f"[{APPROVER_NAME}] MongoDB connection/operation failed for task {task_id}: {e}")
            raise AirflowFailException(f"[{APPROVER_NAME}] Cascading approval failed for {task_id} due to MongoDB error: {e}")
        except Exception as e:
            print(f"[{APPROVER_NAME}] Unexpected error during cascading approval for task {task_id}: {e}")
            raise AirflowFailException(f"[{APPROVER_NAME}] Unexpected error for {task_id}: {e}")
        finally:
            if mongo_client:
                mongo_client.close()
                print(f"[{APPROVER_NAME}] MongoDB client closed.")

    # --- This is a simplified version of the main DAG's _check_and_transition_parent_states
    # It avoids re-publishing cascading events back to Kafka, preventing loops.
    # It also handles completion of phases/releases/release groups based on the cascaded task.
    def _check_and_transition_parent_states_for_cascaded_tasks(db, completed_child_id, child_phase_id, child_group_id):
        terminal_states = get_terminal_states()

        # 1. Check Phase Completion
        if child_phase_id:
            phase_tasks_cursor = db.tasks.find({"phaseId": child_phase_id})
            all_phase_tasks = list(phase_tasks_cursor)
            
            all_phase_tasks_terminal = True
            for pt in all_phase_tasks:
                if pt.get("state") not in terminal_states:
                    all_phase_tasks_terminal = False
                    break

            if all_phase_tasks_terminal:
                print(f"[{APPROVER_NAME}] All tasks in phase {child_phase_id} are terminal. Marking phase as COMPLETED.")
                update_entity_state_in_mongodb(db, "phases", child_phase_id, "COMPLETED",
                    additional_fields={"reason": f"Phase completed as all tasks are terminal by cascading DAG."},
                    approver_name=APPROVER_NAME)
                
        # 2. Check Release Completion
        if child_phase_id:
            parent_release = db.releases.find_one({"phaseIds": child_phase_id})
            if parent_release:
                release_phases_cursor = db.phases.find({"parentId": parent_release.get("_id"), "parentType": "RELEASE"})
                all_release_phases = list(release_phases_cursor)
                
                all_release_phases_terminal = True
                for rp in all_release_phases:
                    if rp.get("state") not in terminal_states:
                        all_release_phases_terminal = False
                        break

                if all_release_phases_terminal:
                    print(f"[{APPROVER_NAME}] All phases in Release {parent_release.get('_id')} are terminal. Marking Release as COMPLETED.")
                    update_entity_state_in_mongodb(db, "releases", parent_release.get("_id"), "COMPLETED",
                        additional_fields={"reason": f"Release completed as all phases are terminal by cascading DAG."},
                        approver_name=APPROVER_NAME)
                    
                    # 3. Check Release Group Completion
                    if parent_release.get("releaseGroupId"):
                        release_group_id = parent_release.get("releaseGroupId")
                        releases_in_group_cursor = db.releases.find({"releaseGroupId": release_group_id})
                        all_group_releases = list(releases_in_group_cursor)

                        all_group_releases_terminal = True
                        for gr in all_group_releases:
                            if gr.get("state") not in terminal_states:
                                all_group_releases_terminal = False
                                break

                        if all_group_releases_terminal:
                            print(f"[{APPROVER_NAME}] All releases in Release Group {release_group_id} are terminal. Marking Release Group as COMPLETED.")
                            update_entity_state_in_mongodb(db, "releaseGroups", release_group_id, "COMPLETED",
                                additional_fields={"reason": f"Release Group completed as all releases are terminal by cascading DAG."},
                                approver_name=APPROVER_NAME)

        # No need to handle groupId directly for cascading, as the cascaded task is handled.
        # The group parent will become completed by its own `_check_and_transition_parent_states` call
        # in the main executor DAG when its last child completes.

    # --- DAG Flow ---
    cascading_events = consume_and_filter_cascading_events()
    process_cascading_tasks = process_cascading_approval.partial().expand(cascading_event_data=cascading_events)

# Instantiate the DAG
reflow_cascading_approval_dag()
