# dags/airflow_config.py
# This file centralizes configuration constants for Reflow Airflow DAGs.
# In a production environment, these values should typically be managed
# via Airflow Variables, Environment Variables, or a secrets manager.

# --- Kafka Configuration ---
KAFKA_BROKER_SERVERS = "localhost:9092"
KAFKA_TASK_EVENTS_TOPIC = "reflow_task_events"

# --- MongoDB Configuration ---
MONGO_HOST = "localhost"
MONGO_PORT = 27017
MONGO_DB_NAME = "reflow_db"

# --- JAR Execution Configuration ---
# IMPORTANT: Replace with the actual path to your Reflow task executor JAR file
JAVA_EXECUTABLE_PATH = "java"
JAR_FILE_PATH = "/path/to/your/reflow-task-executor.jar"

# --- Other Global Constants ---
APPROVER_NAME_SYSTEM = "SYSTEM_ORCHESTRATOR" # Generic system approver name
APPROVER_NAME_CASCADER = "SYSTEM_CASCADER_DAG" # Specific for cascading approvals
APPROVER_NAME_SCHEDULER = "SYSTEM_SCHEDULER_MONITOR" # Specific for scheduler monitor
