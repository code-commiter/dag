# dags/reflow_scheduler_monitor_dag.py
# This Airflow DAG is responsible for monitoring SCHEDULER tasks in MongoDB
# and transitioning them from 'SCHEDULED' to 'QUEUED' when their scheduledTime is met.
# Refactored to use common utility functions from reflow_dag_utils.py
# and centralized configuration from airflow_config.py.

from __future__ import annotations

import pendulum
import time
from datetime import timedelta # Still needed for defer_after timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

# Import common utilities and configuration constants
from reflow_dag_utils import (
    get_mongo_client,
    update_entity_state_in_mongodb,
    get_terminal_states, # Not directly used in this DAG, but good to have common functions
    MONGO_DB_NAME,
    KAFKA_BROKER_SERVERS, # Imported for Consumer config
    KAFKA_TASK_EVENTS_TOPIC # Not used in consumer, but kept for consistency
)

# Import specific approver name for this DAG
from airflow_config import APPROVER_NAME_SCHEDULER as APPROVER_NAME


# Confluent Kafka Python client - needs to be installed in Airflow environment:
# pip install confluent-kafka
from confluent_kafka import Consumer, KafkaException, OFFSET_BEGINNING


@dag(
    dag_id="reflow_scheduler_monitor_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="*/1 * * * *",  # Run every 1 minute to check for overdue scheduled tasks
    catchup=False,
    tags=["reflow", "scheduler", "monitoring", "mongodb"],
    doc_md="""
    ### Reflow Scheduler Task Monitor DAG
    This DAG's sole responsibility is to monitor tasks of `type: "SCHEDULER"` in MongoDB.
    When a `SCHEDULER` task's `scheduledTime` has passed, this DAG will transition
    its state from `SCHEDULED` to `QUEUED`, making it discoverable by other Reflow
    execution DAGs (e.g., `reflow_kafka_task_executor_v4`).
    Refactored to use `reflow_dag_utils.py` and `airflow_config.py`.
    """
)
def reflow_scheduler_monitor_dag():

    # Kafka consumer group ID specific to this DAG
    KAFKA_CONSUMER_GROUP_ID = "airflow_reflow_scheduler_monitor_group"

    @task
    def check_and_queue_scheduled_tasks():
        """
        Connects to MongoDB, finds SCHEDULER tasks whose scheduledTime has passed,
        and transitions their state to QUEUED.
        """
        mongo_client = None
        try:
            mongo_client = get_mongo_client(APPROVER_NAME)
            db = mongo_client[MONGO_DB_NAME]
            tasks_collection = db.tasks

            now_utc = pendulum.now("UTC")
            print(f"[{APPROVER_NAME}] Checking for overdue SCHEDULED tasks at {now_utc.isoformat()}.")

            # Find tasks that are of type SCHEDULER, in SCHEDULED state,
            # and whose scheduledTime is in the past or current.
            query = {
                "type": "SCHEDULER",
                "state": "SCHEDULED",
                "scheduledTime": {"$lte": now_utc.replace(microsecond=0)} # Compare against current time, ignore microseconds for robust comparison
            }
            
            overdue_tasks_cursor = tasks_collection.find(query)
            overdue_tasks = list(overdue_tasks_cursor)

            if not overdue_tasks:
                print(f"[{APPROVER_NAME}] No overdue SCHEDULED tasks found.")
                return

            print(f"[{APPROVER_NAME}] Found {len(overdue_tasks)} overdue SCHEDULED tasks. Processing...")

            for task_doc in overdue_tasks:
                task_id = task_doc.get("_id")
                task_name = task_doc.get("name")
                current_state = task_doc.get("state")
                scheduled_time_str = task_doc.get("scheduledTime")
                
                print(f"[{APPROVER_NAME}] Attempting to queue task {task_name} (ID: {task_id}) from {current_state}. Scheduled for: {scheduled_time_str}")

                # Attempt to transition the task to QUEUED
                # Use optimistic locking (old_state_condition) to prevent race conditions
                update_successful = update_entity_state_in_mongodb(
                    db,
                    "tasks",
                    task_id,
                    "QUEUED",
                    old_state_condition="SCHEDULED", # Only update if still in SCHEDULED state
                    additional_fields={"reason": f"Automatically QUEUED by {APPROVER_NAME} as scheduled time ({scheduled_time_str}) passed."},
                    approver_name=APPROVER_NAME
                )

                if update_successful:
                    print(f"[{APPROVER_NAME}] Successfully queued SCHEDULER task: {task_id}")
                else:
                    print(f"[{APPROVER_NAME}] Failed to queue SCHEDULER task: {task_id}. It might have been updated concurrently or is no longer in SCHEDULED state.")

        except (ConnectionFailure, OperationFailure) as e:
            print(f"[{APPROVER_NAME}] MongoDB connection/operation failed: {e}")
            raise AirflowFailException(f"[{APPROVER_NAME}] Scheduler monitor failed due to MongoDB error: {e}")
        except Exception as e:
            print(f"[{APPROVER_NAME}] An unexpected error occurred during scheduler task monitoring: {e}")
            raise AirflowFailException(f"[{APPROVER_NAME}] Scheduler monitor failed unexpectedly: {e}")
        finally:
            if mongo_client:
                mongo_client.close()
                print(f"[{APPROVER_NAME}] MongoDB client closed.")

    # Define the task in the DAG flow
    check_and_queue_scheduled_tasks()

# Instantiate the DAG
reflow_scheduler_monitor_dag()
