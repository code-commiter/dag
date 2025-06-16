# dags/reflow_kafka_task_executor_dag_v4.py
# This Airflow DAG consumes task events from Kafka and fully orchestrates
# workflow progression by directly interacting with MongoDB for all data operations.
# Refactored to use common utility functions from reflow_dag_utils.py
# and centralized configuration from airflow_config.py.
# UPDATED to strictly separate Task.state from Task.gateStatus.
# NOW using GateStatus.APPROVED and GateStatus.REJECTED for consistency.
# TaskType string literals are replaced with constants for readability and maintainability.
# Task state string literals now also replaced with constants.
# Added QUEUE_PROCESS_TYPE to control task fetching mechanism (DB, KAFKA, MOCK).
# Removed redundant 'use_mock_kafka_data' DAG parameter.
# ENHANCED: Actual exception messages are now included in task logs on failure.
# NEW: Phase completion now directly triggers the next phase's first task within the DAG.

from __future__ import annotations

import pendulum
import json
import time
import subprocess
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException, AirflowRescheduleException
from airflow.models.param import Param # Keep Param import if you plan to add other parameters in the future, otherwise can remove it.
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Import common utilities and configuration constants
from reflow_dag_utils import (
    get_mongo_client,
    update_entity_state_in_mongodb,
    get_terminal_states,
    publish_kafka_message, # Still here, but not used for cascading directly now
    KAFKA_BROKER_SERVERS, # Imported for Consumer config
    KAFKA_TASK_EVENTS_TOPIC,
    MONGO_DB_NAME,
    APPROVER_NAME_SYSTEM # Default approver name for this DAG's actions
)

# Import JAR execution paths from centralized config
from airflow_config import (
    JAVA_EXECUTABLE_PATH,
    JAR_FILE_PATH
)

# Confluent Kafka Python client - needs to be installed in Airflow environment:
# pip install confluent-kafka
from confluent_kafka import Consumer, KafkaException, OFFSET_BEGINNING
from pymongo.errors import ConnectionFailure, OperationFailure


# Use the system approver name from config
APPROVER_NAME = APPROVER_NAME_SYSTEM

# --- QUEUE_PROCESS_TYPE Global Variable ---
# Controls how queued tasks are obtained:
# "DB": Fetches QUEUED tasks directly from MongoDB.
# "KAFKA": Consumes QUEUED task events from Kafka.
# "MOCK": Uses hardcoded mock data for testing.
QUEUE_PROCESS_TYPE = "KAFKA" # Set your desired mode here (DB, KAFKA, MOCK)

# --- TaskType Constants (Matching Java Enum for Consistency) ---
TASK_TYPE_REGULAR = "REGULAR"
TASK_TYPE_PARALLEL_GROUP = "PARALLEL_GROUP"
TASK_TYPE_SEQUENTIAL_GROUP = "SEQUENTIAL_GROUP"
TASK_TYPE_APPROVAL = "APPROVAL"
TASK_TYPE_SCHEDULER = "SCHEDULER"
TASK_TYPE_TRIGGER = "TRIGGER"

# --- GateStatus Constants (Matching Java Enum for Consistency) ---
GATE_STATUS_PENDING = "PENDING"
GATE_STATUS_APPROVED = "APPROVED"
GATE_STATUS_REJECTED = "REJECTED"

# --- Task State Constants (Matching Java Enum for Consistency) ---
STATE_PLANNED = "PLANNED"
STATE_QUEUED = "QUEUED"
STATE_IN_PROGRESS = "IN_PROGRESS"
STATE_COMPLETED = "COMPLETED"
STATE_FAILED = "FAILED"
STATE_REJECTED = "REJECTED"
STATE_BLOCKED = "BLOCKED"
STATE_WAITING_FOR_APPROVAL = "WAITING_FOR_APPROVAL"
STATE_SKIPPED = "SKIPPED"
STATE_SCHEDULED = "SCHEDULED"
STATE_RETRY = "RETRY"
STATE_ARCHIVED = "ARCHIVED"


def _append_task_logs_in_mongodb(db, task_id, new_log_content):
    """Appends new log content to the task's logs field directly in MongoDB."""
    if not new_log_content.strip():
        print(f"[{APPROVER_NAME}] No content to append for task {task_id} logs.")
        return

    timestamp = pendulum.now("UTC").isoformat()
    log_entry = f"\n--- Log Entry [{timestamp}] ---\n{new_log_content}"

    # Use aggregation pipeline with $set and $concat to atomically append to logs field
    result = db.tasks.update_one(
        {"_id": task_id},
        [{"$set": {"logs": {"$concat": ["$logs", log_entry]}, "updatedAt": timestamp}}]
    )
    if result.modified_count == 0:
        print(f"[{APPROVER_NAME}] Warning: Task {task_id} not found or logs not appended.")
    else:
        print(f"[{APPROVER_NAME}] Successfully appended logs for task {task_id}.")


@dag(
    dag_id="reflow_kafka_task_executor_v4",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="*/1 * * * *",  # Run every 1 minute to check for new Kafka events or re-evaluate deferred tasks
    catchup=False,
    tags=["reflow", "kafka", "workflow", "tasks", "orchestration", "mongodb", "group-tasks", "concurrency", "deferrable"],
    # Removed the 'params' dictionary as 'use_mock_kafka_data' is no longer needed
    doc_md="""
    ### Reflow Kafka Task Executor DAG (v4 - Centralized TRIGGER Task Handling)
    This DAG consumes tasks from Kafka and orchestrates workflow progression by directly
    interacting with MongoDB. It now uses `@task.deferrable` to handle long-running
    asynchronous operations. Refactored to use `reflow_dag_utils.py` and `airflow_config.py`.
    **UPDATED to strictly separate Task.state from Task.gateStatus.**
    **NOW using GateStatus.APPROVED and GateStatus.REJECTED for consistency.**
    **ENHANCED: Actual exception messages are now included in task logs on failure.**
    **NEW: Phase completion now directly triggers the next phase's first task within the DAG.**

    **Gate Logic Refinement (Strict Separation):**
    - `Task.state` defines the workflow progression: `PLANNED`, `QUEUED`, `IN_PROGRESS`, `WAITING_FOR_APPROVAL`, `COMPLETED`, `FAILED`, etc.
    - `Task.gateStatus` (Enum: `PENDING`, `APPROVED`, `REJECTED`) reflects the approval decision.
    - When an `APPROVAL` type task is `QUEUED`, the DAG fetches its current `gateStatus`.
    - If `gateStatus` is `APPROVED`, the DAG sets `task.state` to `COMPLETED` and queues `nextTaskId`.
    - If `gateStatus` is `REJECTED`, the DAG sets `task.state` to `FAILED` and stops workflow.
    - If `gateStatus` is `PENDING` (or null), the DAG transitions `task.state` to `WAITING_FOR_APPROVAL` and pauses.
    - UI actions for gate approval/rejection now only set `gateStatus` and appropriately update `task.state` (`QUEUED` or `FAILED`) in MongoDB, awaiting DAG progression.

    **Task Type: `TRIGGER`**
    For tasks of type `TRIGGER`, the DAG will repeatedly execute the JAR at the
    `triggerInterval` specified in the task document. The JAR itself is then responsible
    for checking the external system's status and updating the Reflow Task's state in MongoDB.
    The Airflow task will complete when the MongoDB task state becomes terminal.

    **Other Task Types (e.g., `REGULAR`, `PARALLEL_GROUP`, `SEQUENTIAL_GROUP`, `SCHEDULER`):**
    For these types, the DAG executes the associated JAR (if any `actor` is defined)
    synchronously. The JAR is expected to perform its action and complete,
    allowing the DAG to manage the task's state progression (`IN_PROGRESS` to `COMPLETED`/`FAILED`),
    chaining to the next task, and handling group task completion.
    """,
    max_active_runs=2 # Limit the number of concurrent DAG instances to 2
)
def reflow_kafka_task_executor_dag_v4():

    # Kafka consumer group ID specific to this DAG
    KAFKA_CONSUMER_GROUP_ID = "airflow_reflow_executor_group_v4"

    @task
    def consume_and_filter_queued_tasks(**kwargs):
        """
        Consumes messages from Kafka, fetches from DB, or uses mock data based on QUEUE_PROCESS_TYPE.
        Filters for tasks that need execution (only 'QUEUED' tasks now), and returns them for processing.
        """
        # Directly use QUEUE_PROCESS_TYPE global variable
        process_type = QUEUE_PROCESS_TYPE 

        queued_tasks = []

        if process_type == "MOCK":
            print(f"[{APPROVER_NAME}] USING MOCK DATA: Returning a hardcoded QUEUED task for testing.")
            # Mock task for async deployment monitored by JAR
            mock_task = {
                "_id": "mock_regular_task_id", # Changed to _id
                "name": "Synchronous Data Process",
                "description": "A regular task that runs synchronously.",
                "state": STATE_QUEUED,
                "type": TASK_TYPE_REGULAR,
                "phaseId": "P_dev_deploy_v1_0",
                "releaseId": "R_web_app_v1_0",
                "nextTaskId": None,
                "isGate": False,
                "gateStatus": None,
                "previousTaskId": None,
                "actor": "DATA_PROCESSING_SERVICE",
                "taskVariables": {},
                "logs": "",
                "childTaskIds": None
            }

            mock_sequential_group_task = {
                "_id": "mock_seq_group_task_id_1", # Changed to _id
                "name": "Mock Sequential Group",
                "description": "A group task that executes children sequentially.",
                "state": STATE_QUEUED,
                "type": TASK_TYPE_SEQUENTIAL_GROUP,
                "phaseId": "P_mock_phase_id_group",
                "releaseId": "R_mock_release_id_group",
                "childTaskIds": ["child_task_1", "child_task_2"],
                "nextTaskId": None,
                "isGate": False,
                "gateStatus": None,
                "previousTaskId": None,
                "actor": "ORCHESTRATOR",
                "taskVariables": {},
                "logs": ""
            }

            mock_parallel_group_task = {
                "_id": "mock_par_group_task_id_1", # Changed to _id
                "name": "Mock Parallel Group",
                "description": "A group task that executes children in parallel.",
                "state": STATE_QUEUED,
                "type": TASK_TYPE_PARALLEL_GROUP,
                "phaseId": "P_mock_phase_id_group_parallel",
                "releaseId": "R_mock_release_id_group_parallel",
                "childTaskIds": ["child_task_A", "child_task_B"],
                "nextTaskId": None,
                "isGate": False,
                "gateStatus": None,
                "previousTaskId": None,
                "actor": "ORCHESTRATOR",
                "taskVariables": {},
                "logs": ""
            }

            # This gate is QUEUED and its gateStatus indicates it's approved.
            mock_rg_gate_task_approved = {
                "_id": "mock_rg_gate_task_id_approved", # Changed to _id
                "name": "Suite-wide Dev Approval (Approved Gate)",
                "description": "Mock RG-level approval gate, already approved, now QUEUED.",
                "state": STATE_QUEUED,
                "type": TASK_TYPE_APPROVAL,
                "phaseId": "P_mock_rg_phase_id",
                "releaseId": "R_mock_release_id_for_rg_gate",
                "nextTaskId": "some_next_task_after_gate",
                "isGate": True,
                "gateStatus": GATE_STATUS_APPROVED,
                "gateCategory": "Development Deployment",
                "previousTaskId": None,
                "actor": "RELEASE_MANAGER",
                "taskVariables": {},
                "logs": "",
                "childTaskIds": None
            }

            mock_manual_approval_gate_pending = {
                "_id": "mock_manual_approval_gate_id_1", # Changed to _id
                "name": "Manual QA Sign-off",
                "description": "Requires manual QA team approval.",
                "state": STATE_QUEUED, # It's queued when it's its turn
                "type": TASK_TYPE_APPROVAL,
                "phaseId": "P_mock_phase_id_manual_gate",
                "releaseId": "R_mock_release_id_manual_gate",
                "nextTaskId": None,
                "isGate": True,
                "gateStatus": GATE_STATUS_PENDING,
                "previousTaskId": None,
                "actor": "QA_LEAD",
                "taskVariables": {},
                "logs": "",
                "childTaskIds": None
            }

            mock_trigger_task = {
                "_id": "mock_trigger_deploy_task_id", # Changed to _id
                "name": "Deploy Application (Trigger)",
                "description": "Continuously triggers deployment script until successful.",
                "state": STATE_QUEUED,
                "type": TASK_TYPE_TRIGGER,
                "phaseId": "P_deploy_prod",
                "releaseId": "R_prod_app_v1",
                "nextTaskId": None,
                "isGate": False,
                "gateStatus": None,
                "previousTaskId": None,
                "actor": "DEPLOYMENT_SYSTEM",
                "taskVariables": {
                    "deployment_target": "production-cluster-1"
                },
                "logs": "",
                "triggerInterval": 30,
                "childTaskIds": None
            }
            queued_tasks.append(mock_rg_gate_task_approved) # Default mock task to provide
            # queued_tasks.append(mock_trigger_task)
            # queued_tasks.append(mock_task)
            # queued_tasks.append(mock_sequential_group_task)
            # queued_tasks.append(mock_parallel_group_task)
            # queued_tasks.append(mock_manual_approval_gate_pending)


        elif process_type == "KAFKA":
            print(f"[{APPROVER_NAME}] USING KAFKA CONSUMER: Polling for QUEUED task events.")
            conf = {
                'bootstrap.servers': KAFKA_BROKER_SERVERS,
                'group.id': KAFKA_CONSUMER_GROUP_ID,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': True,
                'session.timeout.ms': 10000,
                'heartbeat.interval.ms': 3000
            }
            consumer = Consumer(conf)
            try:
                consumer.subscribe([KAFKA_TASK_EVENTS_TOPIC])
                print(f"[{APPROVER_NAME}] Subscribed to Kafka topic: {KAFKA_TASK_EVENTS_TOPIC}")

                poll_duration_seconds = 30
                start_time = time.time()
                while time.time() - start_time < poll_duration_seconds:
                    msg = consumer.poll(timeout=1.0)

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
                        
                        # Prioritize "_id", fallback to "id" if "_id" not present
                        task_id = task_data.get("_id")
                        if not task_id:
                            task_id = task_data.get("id")
                            if task_id:
                                # Ensure the dictionary uses "_id" consistently for downstream tasks
                                task_data["_id"] = task_id
                                if "id" in task_data:
                                    del task_data["id"] # Remove the "id" key if "_id" is set from it
                            else:
                                print(f"[{APPROVER_NAME}] Warning: Kafka message missing both '_id' and 'id' field. Skipping: {task_data}")
                                continue # Skip processing this malformed message


                        task_state = task_data.get("state")
                        task_name = task_data.get("name")
                        task_type = task_data.get("type")
                        is_gate = task_data.get("isGate", False)
                        actor = task_data.get("actor")

                        print(f"[{APPROVER_NAME}] Consumed message for task: {task_name} (ID: {task_id}), State: {task_state}, Type: {task_type}, IsGate: {is_gate}, Actor: {actor}")

                        if (task_state == STATE_QUEUED):
                            print(f"[{APPROVER_NAME}] Adding task for execution: {task_name} (ID: {task_id}), State: {task_state}")
                            queued_tasks.append(task_data)
                        else:
                            print(f"[{APPROVER_NAME}] Task {task_name} (ID: {task_id}) in state {task_state}, skipping direct execution from consumer.")

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

        elif process_type == "DB":
            print(f"[{APPROVER_NAME}] USING MONGODB: Fetching QUEUED tasks directly from DB.")
            mongo_client = None
            try:
                mongo_client = get_mongo_client(APPROVER_NAME)
                db = mongo_client[MONGO_DB_NAME]
                tasks_collection = db.tasks

                # Find tasks that are in the QUEUED state
                query = {"state": STATE_QUEUED}
                
                db_queued_tasks_cursor = tasks_collection.find(query)
                queued_tasks = list(db_queued_tasks_cursor)

                if not queued_tasks:
                    print(f"[{APPROVER_NAME}] No QUEUED tasks found in MongoDB.")
            except (ConnectionFailure, OperationFailure) as e:
                print(f"[{APPROVER_NAME}] MongoDB connection/operation failed: {e}")
                raise AirflowFailException(f"[{APPROVER_NAME}] Failed to fetch QUEUED tasks from DB due to MongoDB error: {e}")
            except Exception as e:
                print(f"[{APPROVER_NAME}] An unexpected error occurred during DB fetching: {e}")
                raise AirflowFailException(f"[{APPROVER_NAME}] Failed to fetch QUEUED tasks from DB: {e}")
            finally:
                if mongo_client:
                    mongo_client.close()
                    print(f"[{APPROVER_NAME}] MongoDB client closed.")
        else:
            print(f"[{APPROVER_NAME}] ERROR: Invalid QUEUE_PROCESS_TYPE '{process_type}'. Must be DB, KAFKA, or MOCK.")
            raise AirflowFailException(f"Invalid QUEUE_PROCESS_TYPE configured: {process_type}")

        if not queued_tasks:
            print(f"[{APPROVER_NAME}] No tasks found to execute in this run.")
            return None

        return queued_tasks


    @task.deferrable
    def execute_and_transition_task(task_data: dict, **kwargs):
        """
        Executes a single task. For long-running external operations, this task will defer itself.
        JAR's stdout/stderr are captured and appended to the task's logs directly in MongoDB.
        Handles state transitions and sequential/parallel group logic.
        """
        task_id = task_data.get("_id") # Use _id for consistency with MongoDB documents
        task_name = task_data.get("name")
        phase_id = task_data.get("phaseId")
        release_id = task_data.get("releaseId")

        print(f"[{APPROVER_NAME}] Processing task: {task_name} (ID: {task_id}).")

        mongo_client = None
        try:
            mongo_client = get_mongo_client(APPROVER_NAME)
            db = mongo_client[MONGO_DB_NAME]

            current_task_doc = db.tasks.find_one({"_id": task_id}) # Use _id
            if not current_task_doc:
                print(f"[{APPROVER_NAME}] Task {task_id} not found in DB. It might have been deleted or archived. Skipping execution.")
                return

            current_task_state = current_task_doc.get("state")
            current_task_is_gate = current_task_doc.get("isGate", False)
            current_task_type_db = current_task_doc.get("type")
            current_gate_status = current_task_doc.get("gateStatus")
            actor = current_task_doc.get("actor")
            
            # If the task is already in a terminal state (COMPLETED, FAILED, SKIPPED, ARCHIVED),
            # no further action is needed from this Airflow task.
            if current_task_state in get_terminal_states():
                 print(f"[{APPROVER_NAME}] Task {task_id} is already in a terminal state ({current_task_state}). Checking parent state progression.")
                 _check_and_transition_parent_states(db, task_id, current_task_doc.get("phaseId"), current_task_doc.get("groupId"))
                 return

            # --- Concurrency Control: Attempt to atomically transition from QUEUED to IN_PROGRESS ---
            if current_task_state == STATE_QUEUED:
                initial_state_update_success = update_entity_state_in_mongodb(
                    db, "tasks", task_id, STATE_IN_PROGRESS, old_state_condition=STATE_QUEUED,
                    additional_fields={"reason": f"Automatically moved to IN_PROGRESS by {APPROVER_NAME}"},
                    approver_name=APPROVER_NAME
                )
                if not initial_state_update_success:
                    print(f"[{APPROVER_NAME}] Task {task_id} was not in QUEUED state or already processed by another instance. Skipping execution.")
                    return
                current_task_state = STATE_IN_PROGRESS # Update in-memory state after successful transition

            # --- Handle Gate Tasks (APPROVAL type) that are IN_PROGRESS ---
            if current_task_is_gate and current_task_type_db == TASK_TYPE_APPROVAL:
                if current_gate_status == GATE_STATUS_APPROVED:
                    # Gate is approved, so it "completes" its execution and triggers next
                    print(f"[{APPROVER_NAME}] Gate task {task_id} (gateStatus: APPROVED). Marking as COMPLETED and proceeding to next.")
                    update_entity_state_in_mongodb(db, "tasks", task_id, STATE_COMPLETED,
                        old_state_condition=STATE_IN_PROGRESS,
                        additional_fields={"reason": f"Gate task passed and completed by {APPROVER_NAME}"}, # Added reason
                        approver_name=APPROVER_NAME)
                    
                    # After gate is COMPLETED, queue its next task
                    updated_task_doc = db.tasks.find_one({"_id": task_id}) # Use _id, Re-fetch updated state
                    if updated_task_doc and updated_task_doc.get("nextTaskId"):
                        _queue_next_task_after_completion(db, updated_task_doc)
                    
                    _check_and_transition_parent_states(db, task_id, phase_id, current_task_doc.get("groupId"))
                    return

                elif current_gate_status == GATE_STATUS_REJECTED:
                    # Gate is rejected, so it "fails" its execution and stops workflow progression
                    print(f"[{APPROVER_NAME}] Gate task {task_id} (gateStatus: REJECTED). Marking as FAILED and stopping workflow.")
                    update_entity_state_in_mongodb(db, "tasks", task_id, STATE_FAILED,
                        old_state_condition=STATE_IN_PROGRESS,
                        additional_fields={"reason": f"Gate task failed. ({APPROVER_NAME})"}, # Added reason
                        approver_name=APPROVER_NAME)
                    
                    _check_and_transition_parent_states(db, task_id, phase_id, current_task_doc.get("groupId"))
                    return
                
                else: # GateStatus is null, PENDING. Task is IN_PROGRESS, but no decision made yet.
                    print(f"[{APPROVER_NAME}] Gate task {task_id} is IN_PROGRESS but gateStatus is {current_gate_status}. Setting to WAITING_FOR_APPROVAL. Not executing JAR. Waiting for external approval.")
                    update_entity_state_in_mongodb(db, "tasks", task_id, STATE_WAITING_FOR_APPROVAL,
                        old_state_condition=STATE_IN_PROGRESS,
                        additional_fields={"reason": f"Gate task awaiting external approval. ({APPROVER_NAME})"}, # Added reason
                        approver_name=APPROVER_NAME)
                    return # Airflow task completes, will be re-queued when state changes via UI/cascading


            # --- Handle Group Tasks ---
            if current_task_type_db in [TASK_TYPE_PARALLEL_GROUP, TASK_TYPE_SEQUENTIAL_GROUP]:
                print(f"[{APPROVER_NAME}] Initiating group task {task_id} of type {current_task_type_db}.")
                
                child_task_ids = current_task_doc.get("childTaskIds", [])
                if not child_task_ids:
                    print(f"[{APPROVER_NAME}] Warning: Group task {task_id} has no childTaskIds. Marking as COMPLETED.")
                    update_entity_state_in_mongodb(db, "tasks", task_id, STATE_COMPLETED,
                        additional_fields={"reason": f"Group task completed with no children by {APPROVER_NAME}"}, # Added reason
                        approver_name=APPROVER_NAME)
                    # When a group task completes, queue its next task in the main flow
                    updated_group_task_doc = db.tasks.find_one({"_id": task_id}) # Use _id, Re-fetch to get latest state
                    if updated_group_task_doc and updated_group_task_doc.get("nextTaskId"):
                        _queue_next_task_after_completion(db, updated_group_task_doc)
                    _check_and_transition_parent_states(db, task_id, phase_id, current_task_doc.get("groupId"))
                    return

                if current_task_type_db == TASK_TYPE_PARALLEL_GROUP:
                    print(f"[{APPROVER_NAME}] Queuing all {len(child_task_ids)} child tasks for parallel group {task_id}.")
                    for child_id in child_task_ids:
                        child_doc = db.tasks.find_one({"_id": child_id}) # Use _id
                        if child_doc and child_doc.get("state") == STATE_PLANNED:
                             update_entity_state_in_mongodb(db, "tasks", child_id, STATE_QUEUED, old_state_condition=STATE_PLANNED,
                                additional_fields={"reason": f"Queued by {APPROVER_NAME} as part of parallel group {task_id}"}, # Added reason
                                approver_name=APPROVER_NAME)
                        else:
                            print(f"[{APPROVER_NAME}] Child task {child_id} for parallel group {task_id} is not in PLANNED state, skipping queueing.")

                elif current_task_type_db == TASK_TYPE_SEQUENTIAL_GROUP:
                    first_child_to_queue_id = None
                    for child_id_in_order in child_task_ids:
                        child_doc = db.tasks.find_one({"_id": child_id_in_order}) # Use _id
                        if child_doc and child_doc.get("groupId") == task_id and \
                           (child_doc.get("previousTaskId") is None or child_doc.get("previousTaskId") == ""):
                            first_child_to_queue_id = child_id_in_order
                            break
                    
                    if first_child_to_queue_id:
                        child_doc = db.tasks.find_one({"_id": first_child_to_queue_id}) # Use _id
                        if child_doc and child_doc.get("state") == STATE_PLANNED:
                            print(f"[{APPROVER_NAME}] Queuing first child {first_child_to_queue_id} for sequential group {task_id}.")
                            update_entity_state_in_mongodb(db, "tasks", first_child_to_queue_id, STATE_QUEUED, old_state_condition=STATE_PLANNED,
                                additional_fields={"reason": f"Queued by {APPROVER_NAME} as first child of sequential group {task_id}"}, # Added reason
                                approver_name=APPROVER_NAME)
                        elif child_doc and child_doc.get("state") in get_terminal_states():
                             print(f"[{APPROVER_NAME}] First child {first_child_to_queue_id} for sequential group {task_id} is already in state {child_doc.get('state')}. Checking group completion.")
                             # If the first child is already in a terminal state, check if sequential group can move forward or complete
                             # This will trigger subsequent child queuing if conditions are met
                             _check_and_transition_parent_states(db, first_child_to_queue_id, phase_id, task_id)
                        else:
                            print(f"[{APPROVER_NAME}] First child {first_child_to_queue_id} for sequential group {task_id} is not in PLANNED state or already processed. State: {child_doc.get('state') if child_doc else 'N/A'}")
                    else:
                        print(f"[{APPROVER_NAME}] Warning: No initial child (previousTaskId=null or empty) found for sequential group {task_id}. Cannot initiate children.")
                        update_entity_state_in_mongodb(db, "tasks", task_id, STATE_FAILED, additional_fields={"reason": f"No initial child task found for sequential group. ({APPROVER_NAME})"},
                            approver_name=APPROVER_NAME)
                
                # Group task remains IN_PROGRESS until its children trigger its completion.
                return

            # --- Handle TRIGGER Task Type ---
            elif current_task_type_db == TASK_TYPE_TRIGGER:
                trigger_interval = current_task_doc.get("triggerInterval")
                if not isinstance(trigger_interval, (int, float)) or trigger_interval <= 0:
                    print(f"[{APPROVER_NAME}] ERROR: Task {task_id} of type TRIGGER has invalid or missing 'triggerInterval'. Marking FAILED.")
                    update_entity_state_in_mongodb(db, "tasks", task_id, STATE_FAILED,
                        additional_fields={"reason": f"TRIGGER task has invalid triggerInterval. ({APPROVER_NAME})"}, # Added reason
                        approver_name=APPROVER_NAME)
                    raise AirflowFailException(f"TRIGGER task {task_id} has invalid triggerInterval.")

                print(f"[{APPROVER_NAME}] Executing JAR for TRIGGER task: {task_id} with Actor: {actor} (interval: {trigger_interval}s).")
                command = [
                    JAVA_EXECUTABLE_PATH,
                    "-jar",
                    JAR_FILE_PATH,
                    "--task-id", task_id,
                    "--task-name", task_name,
                    "--task-type", current_task_type_db,
                    "--phase-id", phase_id,
                    "--release-id", release_id if release_id else "null",
                    "--actor", actor,
                ]
                
                process = subprocess.run(command, capture_output=True, text=True, check=False)

                combined_output_trigger = f"STDOUT (TRIGGER):\n{process.stdout}\nSTDERR (TRIGGER):\n{process.stderr}"
                _append_task_logs_in_mongodb(db, task_id, combined_output_trigger)

                if process.returncode != 0:
                    error_message = f"JAR execution failed for TRIGGER task {task_id} with exit code {process.returncode}. Stderr: {process.stderr.strip()}"
                    print(f"[{APPROVER_NAME}] {error_message}")
                    update_entity_state_in_mongodb(db, "tasks", task_id, STATE_FAILED,
                        additional_fields={"reason": f"JAR execution for TRIGGER task failed: {error_message} ({APPROVER_NAME})"},
                        approver_name=APPROVER_NAME)
                    raise AirflowFailException(f"JAR execution failed for TRIGGER task {task_id}")
                else:
                    print(f"[{APPROVER_NAME}] JAR execution for TRIGGER task {task_id} reported success.")


                # Re-fetch task state after JAR execution to see if it completed or failed
                current_task_doc_after_jar_run = db.tasks.find_one({"_id": task_id}) # Use _id
                if not current_task_doc_after_jar_run:
                    raise AirflowFailException(f"[{APPROVER_NAME}] Task {task_id} not found in MongoDB after JAR run for TRIGGER type.")
                
                updated_state = current_task_doc_after_jar_run.get("state")
                
                if updated_state in get_terminal_states():
                    print(f"[{APPROVER_NAME}] TRIGGER task {task_id} reached terminal state {updated_state}. Completing Airflow task.")
                    _check_and_transition_parent_states(db, task_id, phase_id, current_task_doc_after_jar_run.get("groupId"))
                else:
                    print(f"[{APPROVER_NAME}] TRIGGER task {task_id} is still in state {updated_state}. Deferring for {trigger_interval} seconds.")
                    raise AirflowRescheduleException(
                        f"TRIGGER task {task_id} deferred. Polling with actor {actor} in {trigger_interval}s.",
                        defer_after=timedelta(seconds=trigger_interval)
                    )
                return 

            # --- Handle Regular Tasks ---
            elif current_task_type_db == TASK_TYPE_REGULAR:
                
                print(f"[{APPROVER_NAME}] Executing JAR: {JAR_FILE_PATH} for task_id: {task_id} with Actor: {actor}. Expected to complete synchronously.")
                command = [
                    JAVA_EXECUTABLE_PATH,
                    "-jar",
                    JAR_FILE_PATH,
                    "--task-id", task_id,
                    "--task-name", task_name,
                    "--task-type", current_task_type_db,
                    "--phase-id", phase_id,
                    "--release-id", release_id if release_id else "null",
                    "--actor", actor,
                ]
                
                process = subprocess.run(command, capture_output=True, text=True, check=False)

                combined_output_sync = f"STDOUT (SYNC):\n{process.stdout}\nSTDERR (SYNC):\n{process.stderr}"
                _append_task_logs_in_mongodb(db, task_id, combined_output_sync)

                if process.returncode != 0:
                    error_message = f"JAR execution for {task_id} reported failure (exit code {process.returncode}). Stderr: {process.stderr.strip()}"
                    print(f"[{APPROVER_NAME}] {error_message}")
                    update_entity_state_in_mongodb(db, "tasks", task_id, STATE_FAILED,
                        additional_fields={"reason": f"JAR execution failed: {error_message} ({APPROVER_NAME})"},
                        approver_name=APPROVER_NAME)
                    raise AirflowFailException(f"JAR execution failed for {task_id}")
                else:
                    print(f"[{APPROVER_NAME}] JAR execution for {task_id} reported success.")
                    updated_task_doc = db.tasks.find_one({"_id": task_id}) # Use _id, Re-fetch to get latest
                    if not updated_task_doc:
                        raise AirflowFailException(f"[{APPROVER_NAME}] Task {task_id} not found in MongoDB after JAR completion.")

                    # After synchronous JAR execution, ensure task is in a terminal state.
                    if updated_task_doc.get("state") not in get_terminal_states():
                        print(f"[{APPROVER_NAME}] Warning: JAR for task {task_id} exited successfully, but task state in DB is still '{updated_task_doc.get('state')}'. Expected a terminal state. Setting to COMPLETED by DAG.")
                        update_entity_state_in_mongodb(db, "tasks", task_id, STATE_COMPLETED,
                            additional_fields={"reason": f"JAR exited successfully but task state not terminal. Marked COMPLETED by DAG. ({APPROVER_NAME})"},
                            approver_name=APPROVER_NAME)
                        updated_task_doc = db.tasks.find_one({"_id": task_id}) # Use _id, Re-fetch to get truly updated state


                # --- Post-completion logic for synchronous tasks: queue next task in flow ---
                if updated_task_doc.get("state") == STATE_COMPLETED:
                    if updated_task_doc.get("nextTaskId"):
                        _queue_next_task_after_completion(db, updated_task_doc)

                elif updated_task_doc.get("state") == STATE_FAILED and updated_task_doc.get("nextTaskId"):
                    print(f"[{APPROVER_NAME}] Task {task_id} FAILED. Not queuing next task {updated_task_doc.get('nextTaskId')}.")
                
                # --- Group Parent Logic (if current task is a child of a group) ---
                if updated_task_doc.get("groupId"):
                    group_id = updated_task_doc.get("groupId")
                    parent_group_task = db.tasks.find_one({"_id": group_id, "type": {"$in": [TASK_TYPE_PARALLEL_GROUP, TASK_TYPE_SEQUENTIAL_GROUP]}}) # Use _id
                    if parent_group_task:
                        print(f"[{APPROVER_NAME}] Task {task_id} is part of group {group_id}. Checking group completion and sequential flow.")
                        
                        if parent_group_task.get("type") == TASK_TYPE_SEQUENTIAL_GROUP and updated_task_doc.get("state") == STATE_COMPLETED:
                            # Find the next child in the sequence
                            next_child_in_seq = db.tasks.find_one({"groupId": group_id, "previousTaskId": task_id})
                            if next_child_in_seq:
                                print(f"[{APPROVER_NAME}] Sequential group {group_id}: Queuing next child task {next_child_in_seq.get('_id')}.")
                                update_entity_state_in_mongodb(db, "tasks", next_child_in_seq.get("_id"), STATE_QUEUED, old_state_condition=STATE_PLANNED,
                                    additional_fields={"reason": f"Queued by {APPROVER_NAME} as next in sequential group {group_id}"}, # Added reason
                                    approver_name=APPROVER_NAME)
                            else:
                                print(f"[{APPROVER_NAME}] Sequential group {group_id}: No further child found after {task_id}.")

                        # After handling sequential flow, always check if all children of the group are terminal
                        child_tasks_in_group_cursor = db.tasks.find({"groupId": group_id})
                        child_tasks_in_group = list(child_tasks_in_group_cursor)

                        all_group_children_terminal = True
                        for child_t in child_tasks_in_group:
                            if child_t.get("state") not in get_terminal_states():
                                all_group_children_terminal = False
                                break
                        
                        if all_group_children_terminal:
                            print(f"[{APPROVER_NAME}] All children of group {group_id} are terminal. Marking group as COMPLETED.")
                            update_entity_state_in_mongodb(db, "tasks", group_id, STATE_COMPLETED, old_state_condition=STATE_IN_PROGRESS,
                                additional_fields={"reason": f"Group task completed as all children are terminal. ({APPROVER_NAME})"}, # Added reason
                                approver_name=APPROVER_NAME)
                            # IMPORTANT: After the group task itself is completed, queue its own nextTaskId
                            updated_group_task_doc = db.tasks.find_one({"_id": group_id}) # Use _id, Re-fetch to get latest state
                            if updated_group_task_doc and updated_group_task_doc.get("nextTaskId"):
                                _queue_next_task_after_completion(db, updated_group_task_doc)
                            _check_and_transition_parent_states(db, group_id, parent_group_task.get("phaseId"), parent_group_task.get("groupId"))
                    else:
                        print(f"[{APPROVER_NAME}] Warning: Task {task_id} has groupId {group_id} but parent group task not found or not of type 'group'.")
                else:
                    # If it's not a child of a group, or the group logic didn't queue the next,
                    # then check if its own completion affects its parent phase/release.
                    _check_and_transition_parent_states(db, task_id, phase_id, None)
            
            # --- SCHEDULER task type is handled by reflow_scheduler_monitor_dag.py
            elif current_task_type_db == TASK_TYPE_SCHEDULER:
                print(f"[{APPROVER_NAME}] SCHEDULER task {task_id} (name: {task_name}) is QUEUED. Marking as COMPLETED.")
                update_entity_state_in_mongodb(db, "tasks", task_id, STATE_COMPLETED, old_state_condition=STATE_IN_PROGRESS,
                    additional_fields={"reason": f"SCHEDULER task time reached and processed by DAG. ({APPROVER_NAME})"}, # Added reason
                    approver_name=APPROVER_NAME)
                
                # After SCHEDULER task completes, queue next task if any
                updated_task_doc = db.tasks.find_one({"_id": task_id}) # Use _id, Re-fetch to get updated state
                if updated_task_doc and updated_task_doc.get("nextTaskId"):
                    _queue_next_task_after_completion(db, updated_task_doc)
                else:
                    _check_and_transition_parent_states(db, task_id, current_task_doc.get("phaseId"), current_task_doc.get("groupId"))
                return


            else:
                print(f"[{APPROVER_NAME}] Unknown task type '{current_task_type_db}' for task {task_id}. Skipping processing.")


        except AirflowRescheduleException as e:
            # Re-raise to defer the task
            raise e
        except (ConnectionFailure, OperationFailure) as e:
            error_message = f"MongoDB connection/operation failed for task {task_id}: {e}"
            print(f"[{APPROVER_NAME}] {error_message}")
            update_entity_state_in_mongodb(db, "tasks", task_id, STATE_FAILED,
                additional_fields={"reason": f"MongoDB interaction failed: {error_message} ({APPROVER_NAME})"},
                approver_name=APPROVER_NAME)
            raise AirflowFailException(f"[{APPROVER_NAME}] Task execution failed for {task_id} due to MongoDB error: {e}")
        except subprocess.CalledProcessError as e:
            error_message = f"JAR execution failed for task {task_id} with exit code {e.returncode}. Stderr: {e.stderr.strip()}"
            print(f"[{APPROVER_NAME}] {error_message}")
            update_entity_state_in_mongodb(db, "tasks", task_id, STATE_FAILED,
                additional_fields={"reason": f"JAR execution failed: {error_message} ({APPROVER_NAME})"},
                approver_name=APPROVER_NAME)
            raise AirflowFailException(f"JAR execution failed for {task_id}")
        except FileNotFoundError as e:
            error_message = f"JAR file not found at {JAR_FILE_PATH} or Java executable not found: {e}"
            print(f"[{APPROVER_NAME}] Error: {error_message}")
            update_entity_state_in_mongodb(db, "tasks", task_id, STATE_FAILED,
                additional_fields={"reason": f"JAR file or Java executable not found: {error_message} ({APPROVER_NAME})"},
                approver_name=APPROVER_NAME)
            raise AirflowFailException(f"JAR file or Java executable not found.")
        except Exception as e:
            error_message = f"Unexpected error during task {task_id} execution: {e}"
            print(f"[{APPROVER_NAME}] {error_message}")
            update_entity_state_in_mongodb(db, "tasks", task_id, STATE_FAILED,
                additional_fields={"reason": f"Unexpected error: {error_message} ({APPROVER_NAME})"},
                approver_name=APPROVER_NAME)
            raise AirflowFailException(f"Unexpected error for {task_id}: {e}")
        finally:
            if mongo_client:
                mongo_client.close()
                print(f"[{APPROVER_NAME}] MongoDB client closed.")

    def _queue_next_task_after_completion(db, completed_task_doc):
        """
        Helper function to queue the next task in the sequence after a task completes,
        considering gate logic.
        """
        next_task_id = completed_task_doc.get("nextTaskId")
        if not next_task_id:
            return

        next_task_doc = db.tasks.find_one({"_id": next_task_id}) # Use _id
        if not next_task_doc:
            print(f"[{APPROVER_NAME}] Warning: Completed task {completed_task_doc.get('_id')} has nextTaskId {next_task_id} but it's not found.")
            return

        next_task_is_gate = next_task_doc.get("isGate", False)
        next_task_state = next_task_doc.get("state")
        next_gate_status = next_task_doc.get("gateStatus")

        # If next task is PLANNED, queue it.
        # If next task is a gate and its gateStatus is APPROVED, queue it immediately.
        # Otherwise, the flow waits.
        if next_task_state == STATE_PLANNED:
            print(f"[{APPROVER_NAME}] Task {completed_task_doc.get('_id')} completed. Queuing next task: {next_task_id}.")
            update_entity_state_in_mongodb(db, "tasks", next_task_id, STATE_QUEUED, old_state_condition=STATE_PLANNED,
                additional_fields={"reason": f"Queued by {APPROVER_NAME} after completion of {completed_task_doc.get('_id')}"}, # Added reason
                approver_name=APPROVER_NAME)
        elif next_task_is_gate and next_gate_status == GATE_STATUS_APPROVED:
            print(f"[{APPROVER_NAME}] Task {completed_task_doc.get('_id')} completed. Next task {next_task_id} is an APPROVED gate. Queuing it...")
            update_entity_state_in_mongodb(db, "tasks", next_task_id, STATE_QUEUED, old_state_condition=STATE_PLANNED,
                additional_fields={"reason": f"Queued by {APPROVER_NAME} after completion of {completed_task_doc.get('_id')} (next is approved gate)"}, # Added reason
                approver_name=APPROVER_NAME)
        elif next_task_is_gate and next_gate_status == GATE_STATUS_REJECTED:
            print(f"[{APPROVER_NAME}] Task {completed_task_doc.get('_id')} completed. Next task {next_task_id} is a REJECTED gate. Stopping workflow progression.")
            # Do not queue next task; the flow stops here.
            # The FAILED gate will be picked up by the main consumer to mark its parent as terminal.
        else:
            print(f"[{APPROVER_NAME}] Next task {next_task_id} is not PLANNED or an APPROVED gate. Current state: {next_task_state}, GateStatus: {next_gate_status}. Skipping queueing.")


    def _check_and_transition_parent_states(db, completed_child_id, child_phase_id, child_group_id):
        """
        Helper function to check if parent phase or parent group can transition to COMPLETED.
        This function is called after an atomic task or a group task completes.
        """
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
                update_entity_state_in_mongodb(db, "phases", child_phase_id, STATE_COMPLETED,
                    additional_fields={"reason": f"Phase completed as all tasks are terminal. ({APPROVER_NAME})"}, # Added reason
                    approver_name=APPROVER_NAME)
                
                # --- NEW LOGIC: Trigger next phase and its first task ---
                current_phase_doc = db.phases.find_one({"_id": child_phase_id})
                if current_phase_doc and current_phase_doc.get("nextPhaseId"):
                    next_phase_id = current_phase_doc.get("nextPhaseId")
                    next_phase_doc = db.phases.find_one({"_id": next_phase_id})
                    
                    if next_phase_doc:
                        next_phase_state = next_phase_doc.get("state")
                        if next_phase_state in [STATE_PLANNED, STATE_BLOCKED]:
                            print(f"[{APPROVER_NAME}] Current phase {child_phase_id} completed. Transitioning next phase {next_phase_id} to IN_PROGRESS.")
                            phase_transition_success = update_entity_state_in_mongodb(
                                db, "phases", next_phase_id, STATE_IN_PROGRESS,
                                old_state_condition=next_phase_state,
                                additional_fields={"reason": f"Auto-transitioned by completion of previous phase {child_phase_id} by {APPROVER_NAME}"},
                                approver_name=APPROVER_NAME
                            )
                            if phase_transition_success:
                                # Find the first task of the next phase (no previousTaskId and no groupId)
                                first_tasks_of_next_phase = db.tasks.find({
                                    "phaseId": next_phase_id,
                                    "previousTaskId": None,
                                    "groupId": None
                                }).sort("createdAt", 1).limit(1) # Sort by creation to get deterministic 'first'

                                first_task_doc_list = list(first_tasks_of_next_phase) # Convert cursor to list
                                if first_task_doc_list:
                                    first_task_of_next_phase = first_task_doc_list[0]
                                    first_task_state = first_task_of_next_phase.get("state")
                                    if first_task_state in [STATE_PLANNED, STATE_BLOCKED]:
                                        print(f"[{APPROVER_NAME}] Queuing first task {first_task_of_next_phase.get('_id')} of next phase {next_phase_id}.")
                                        update_entity_state_in_mongodb(
                                            db, "tasks", first_task_of_next_phase.get("_id"), STATE_QUEUED,
                                            old_state_condition=first_task_state,
                                            additional_fields={"reason": f"Auto-queued as first task of phase {next_phase_id} initiated by {APPROVER_NAME}"},
                                            approver_name=APPROVER_NAME
                                        )
                                    else:
                                        print(f"[{APPROVER_NAME}] First task {first_task_of_next_phase.get('_id')} of next phase {next_phase_id} is in state {first_task_state}, not queuing.")
                                else:
                                    print(f"[{APPROVER_NAME}] No direct starting task found for next phase {next_phase_id}.")
                            else:
                                print(f"[{APPROVER_NAME}] Failed to transition next phase {next_phase_id} to IN_PROGRESS (concurrent update?).")
                        else:
                            print(f"[{APPROVER_NAME}] Next phase {next_phase_id} is in state {next_phase_state}, not auto-transitioning.")
                    else:
                        print(f"[{APPROVER_NAME}] Next phase {next_phase_id} not found for phase {child_phase_id}.")
                else:
                    print(f"[{APPROVER_NAME}] Phase {child_phase_id} has no next phase configured.")


        # 2. Check Release Completion
        if child_phase_id:
            # Find parent release by checking phases that contain this child_phase_id
            # NOTE: MongoDB's find_one returns a dict, not a cursor. No need for .get("_id")
            parent_release = db.releases.find_one({"phaseIds": child_phase_id}) 
            if parent_release:
                # Fetch all phases for this parent release directly from DB
                release_phases_cursor = db.phases.find({"parentId": parent_release.get("_id"), "parentType": "RELEASE"}) 
                all_release_phases = list(release_phases_cursor)
                
                all_release_phases_terminal = True
                for rp in all_release_phases:
                    if rp.get("state") not in terminal_states:
                        all_release_phases_terminal = False
                        break

                if all_release_phases_terminal:
                    print(f"[{APPROVER_NAME}] All phases in Release {parent_release.get('_id')} are terminal. Marking Release as COMPLETED.")
                    update_entity_state_in_mongodb(db, "releases", parent_release.get("_id"), STATE_COMPLETED,
                        additional_fields={"reason": f"Release completed as all phases are terminal. ({APPROVER_NAME})"}, # Added reason
                        approver_name=APPROVER_NAME)
                    
                    # 3. Check Release Group Completion
                    if parent_release.get("releaseGroupId"):
                        release_group_id = parent_release.get("releaseGroupId")
                        # Fetch all releases in this group directly from DB
                        releases_in_group_cursor = db.releases.find({"releaseGroupId": release_group_id})
                        all_group_releases = list(releases_in_group_cursor)

                        all_group_releases_terminal = True
                        for gr in all_group_releases:
                            if gr.get("state") not in terminal_states:
                                all_group_releases_terminal = False
                                break

                        if all_group_releases_terminal:
                            print(f"[{APPROVER_NAME}] All releases in Release Group {release_group_id} are terminal. Marking Release Group as COMPLETED.")
                            update_entity_state_in_mongodb(db, "releaseGroups", release_group_id, STATE_COMPLETED,
                                additional_fields={"reason": f"Release Group completed as all releases are terminal. ({APPROVER_NAME})"}, # Added reason
                                approver_name=APPROVER_NAME)


    # --- DAG Flow ---
    queued_tasks_list = consume_and_filter_queued_tasks()
    execute_tasks = execute_and_transition_task.partial().expand(task_data=queued_tasks_list)


    # 3. Task to proactively check for and trigger new DAG runs for queued tasks
    @task
    def check_and_trigger_newly_queued_tasks(**kwargs):
        """
        This task runs after all other tasks in the current DAG run have finished.
        It simply checks for any QUEUED tasks in the database and, if found,
        triggers a new DAG run to process them immediately.
        """
        mongo_client = None
        try:
            mongo_client = get_mongo_client(APPROVER_NAME)
            db = mongo_client[MONGO_DB_NAME]

            # Simply count how many tasks are in the QUEUED state
            queued_task_count = db.tasks.count_documents({"state": STATE_QUEUED})
            
            if queued_task_count > 0:
                print(f"[{APPROVER_NAME}] Found {queued_task_count} tasks in QUEUED state. Triggering a new DAG run.")
                # Trigger a new DAG run without passing specific task_id in conf.
                # The new DAG run's 'consume_and_filter_queued_tasks' will pick up all QUEUED tasks.
                TriggerDagRunOperator(
                    task_id='trigger_general_queue_check', # A generic task ID
                    trigger_dag_id="reflow_kafka_task_executor_v4", # Trigger the same DAG
                    conf={}, # No specific trigger_task_id, rely on the next run's general queue check
                    wait_for_completion=False, # Don't wait for the triggered DAG to complete
                ).execute(context=kwargs) # Execute the operator directly
            else:
                print(f"[{APPROVER_NAME}] No tasks found in QUEUED state. No new DAG run triggered.")
            
        except Exception as e:
            print(f"[{APPROVER_NAME}] Error in check_and_trigger_newly_queued_tasks: {e}")
            raise AirflowFailException(f"Error checking/triggering new DAG run: {e}")
        finally:
            if mongo_client:
                mongo_client.close()
                print(f"[{APPROVER_NAME}] MongoDB client closed.")

    # Define the dependency: `check_and_trigger_newly_queued_tasks` runs after all
    # instances of `execute_and_transition_task` have completed.
    check_and_trigger_newly_queued_tasks() << execute_tasks_result

# Instantiate the DAG
reflow_kafka_task_executor_dag_v4()
