import unittest
from unittest.mock import patch, MagicMock, call
import pendulum
import json
from datetime import timedelta

# Assuming reflow_kafka_task_executor_v4.py is in dags/ and reflow_dag_utils.py is in dags/ as well
# For testing purposes, we'll need to adjust imports if running outside the Airflow environment
# In a real Airflow setup, you might place these tests in a `tests` folder and use a test runner.

# Import the functions directly from the DAG file
# Make sure your PYTHONPATH includes the directory containing the DAGs for this import to work
try:
    # Adjust this import based on your actual file structure for testing
    # If running from the same directory as the DAG:
    from reflow_kafka_task_executor_v4 import (
        _append_task_logs_in_mongodb,
        consume_and_filter_queued_tasks,
        execute_and_transition_task,
        _queue_next_task_after_completion,
        _check_and_transition_parent_states,
        STATE_QUEUED, STATE_IN_PROGRESS, STATE_COMPLETED, STATE_FAILED,
        STATE_WAITING_FOR_APPROVAL, STATE_PLANNED, STATE_BLOCKED,
        TASK_TYPE_REGULAR, TASK_TYPE_APPROVAL, TASK_TYPE_PARALLEL_GROUP,
        TASK_TYPE_SEQUENTIAL_GROUP, TASK_TYPE_TRIGGER, TASK_TYPE_SCHEDULER,
        GATE_STATUS_APPROVED, GATE_STATUS_REJECTED, GATE_STATUS_PENDING,
        MONGO_DB_NAME, APPROVER_NAME,
        QUEUE_PROCESS_TYPE # Need to mock this global in tests
    )
    # Also import common utilities if their logic is directly called and not mocked
    from reflow_dag_utils import (
        get_mongo_client,
        update_entity_state_in_mongodb,
        get_terminal_states
    )

except ImportError:
    # Fallback for environments where direct import from dags might be tricky
    print("Could not import directly from DAG file. Ensure it's in PYTHONPATH or mock more aggressively.")
    # You might need to copy the functions into this test file or adjust imports
    # For this example, we'll assume the direct import works for demonstration.
    pass


class TestReflowKafkaDagV4(unittest.TestCase):

    def setUp(self):
        """Set up common mocks before each test."""
        # Mock pendulum.now() to control time for timestamp-sensitive operations
        self.mock_now = pendulum.datetime(2025, 6, 15, 10, 0, 0, tzinfo=pendulum.UTC)
        self.patcher_pendulum_now = patch('reflow_kafka_task_executor_v4.pendulum.now', return_value=self.mock_now)
        self.mock_pendulum_now = self.patcher_pendulum_now.start()

        # Mock MongoDB client and database
        self.mock_mongo_client_instance = MagicMock()
        self.mock_db = MagicMock()
        self.mock_mongo_client_instance.__getitem__.return_value = self.mock_db # db = client[MONGO_DB_NAME]
        self.patcher_get_mongo_client = patch('reflow_kafka_task_executor_v4.get_mongo_client', return_value=self.mock_mongo_client_instance)
        self.mock_get_mongo_client = self.patcher_get_mongo_client.start()

        # Mock update_entity_state_in_mongodb as it's a utility function
        self.patcher_update_entity_state = patch('reflow_kafka_task_executor_v4.update_entity_state_in_mongodb', return_value=True)
        self.mock_update_entity_state = self.patcher_update_entity_state.start()

        # Mock get_terminal_states as it's a utility function
        self.patcher_get_terminal_states = patch('reflow_kafka_task_executor_v4.get_terminal_states', return_value=["COMPLETED", "SKIPPED", "REJECTED", "ARCHIVED", "APPROVED", "FAILED"])
        self.mock_get_terminal_states = self.patcher_get_terminal_states.start()

        # Mock subprocess.run for JAR execution
        self.patcher_subprocess_run = patch('reflow_kafka_task_executor_v4.subprocess.run')
        self.mock_subprocess_run = self.patcher_subprocess_run.start()
        self.mock_subprocess_run.return_value = MagicMock(returncode=0, stdout="JAR output", stderr="")

        # Mock append_task_logs_in_mongodb
        self.patcher_append_logs = patch('reflow_kafka_task_executor_v4._append_task_logs_in_mongodb')
        self.mock_append_logs = self.patcher_append_logs.start()

        # Mock the global QUEUE_PROCESS_TYPE to control test scenarios
        self.patcher_queue_process_type = patch('reflow_kafka_task_executor_v4.QUEUE_PROCESS_TYPE', new="KAFKA")
        self.mock_queue_process_type = self.patcher_queue_process_type.start()

    def tearDown(self):
        """Clean up mocks after each test."""
        self.patcher_pendulum_now.stop()
        self.patcher_get_mongo_client.stop()
        self.patcher_update_entity_state.stop()
        self.patcher_get_terminal_states.stop()
        self.patcher_subprocess_run.stop()
        self.patcher_append_logs.stop()
        self.patcher_queue_process_type.stop()


    # --- Test _append_task_logs_in_mongodb ---
    def test_append_task_logs_in_mongodb_success(self):
        self.mock_db.tasks.update_one.return_value.modified_count = 1
        _append_task_logs_in_mongodb(self.mock_db, "task123", "New log content.")
        self.mock_db.tasks.update_one.assert_called_once()
        args, _ = self.mock_db.tasks.update_one.call_args
        self.assertEqual(args[0], {"_id": "task123"})
        self.assertIn("$set", args[1])
        self.assertIn("$concat", args[1]["$set"]["logs"])
        self.assertIn("updatedAt", args[1]["$set"])

    def test_append_task_logs_in_mongodb_no_content(self):
        _append_task_logs_in_mongodb(self.mock_db, "task123", "   ")
        self.mock_db.tasks.update_one.assert_not_called()

    def test_append_task_logs_in_mongodb_task_not_found(self):
        self.mock_db.tasks.update_one.return_value.modified_count = 0
        _append_task_logs_in_mongodb(self.mock_db, "task123", "New log content.")
        self.mock_db.tasks.update_one.assert_called_once()


    # --- Test consume_and_filter_queued_tasks ---
    @patch('reflow_kafka_task_executor_v4.Consumer')
    def test_consume_and_filter_queued_tasks_kafka_success(self, MockConsumer):
        # Set QUEUE_PROCESS_TYPE for this specific test
        self.patcher_queue_process_type.stop()
        with patch('reflow_kafka_task_executor_v4.QUEUE_PROCESS_TYPE', new="KAFKA"):
            mock_msg = MagicMock()
            mock_msg.value.return_value = json.dumps({"_id": "t1", "state": "QUEUED", "name": "Test Task", "type": "REGULAR", "isGate": False, "actor": "TEST_ACTOR"}).encode('utf-8')
            mock_msg.error.return_value = None

            # Simulate one message then no more
            mock_consumer_instance = MagicMock()
            mock_consumer_instance.poll.side_effect = [mock_msg, None] # First poll returns msg, second returns None
            MockConsumer.return_value = mock_consumer_instance

            result = consume_and_filter_queued_tasks()

            MockConsumer.assert_called_once()
            mock_consumer_instance.subscribe.assert_called_once()
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["_id"], "t1")
            self.assertEqual(result[0]["state"], STATE_QUEUED)
            mock_consumer_instance.close.assert_called_once()

    @patch('reflow_kafka_task_executor_v4.Consumer')
    def test_consume_and_filter_queued_tasks_kafka_no_queued_tasks(self, MockConsumer):
        self.patcher_queue_process_type.stop()
        with patch('reflow_kafka_task_executor_v4.QUEUE_PROCESS_TYPE', new="KAFKA"):
            mock_msg = MagicMock()
            mock_msg.value.return_value = json.dumps({"_id": "t2", "state": "COMPLETED", "name": "Completed Task", "type": "REGULAR", "isGate": False, "actor": "TEST_ACTOR"}).encode('utf-8')
            mock_msg.error.return_value = None

            mock_consumer_instance = MagicMock()
            mock_consumer_instance.poll.side_effect = [mock_msg, None]
            MockConsumer.return_value = mock_consumer_instance

            result = consume_and_filter_queued_tasks()
            self.assertIsNone(result) # No QUEUED tasks found

    def test_consume_and_filter_queued_tasks_mock_success(self):
        self.patcher_queue_process_type.stop()
        with patch('reflow_kafka_task_executor_v4.QUEUE_PROCESS_TYPE', new="MOCK"):
            result = consume_and_filter_queued_tasks()
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["_id"], "mock_rg_gate_task_id_approved") # Default mock task

    def test_consume_and_filter_queued_tasks_db_success(self):
        self.patcher_queue_process_type.stop()
        with patch('reflow_kafka_task_executor_v4.QUEUE_PROCESS_TYPE', new="DB"):
            self.mock_db.tasks.find.return_value = [
                {"_id": "db_t1", "state": "QUEUED", "name": "DB Task 1", "type": "REGULAR"}
            ]
            result = consume_and_filter_queued_tasks()
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["_id"], "db_t1")
            self.mock_get_mongo_client.assert_called_once()
            self.mock_db.tasks.find.assert_called_once_with({"state": STATE_QUEUED})

    def test_consume_and_filter_queued_tasks_db_no_tasks(self):
        self.patcher_queue_process_type.stop()
        with patch('reflow_kafka_task_executor_v4.QUEUE_PROCESS_TYPE', new="DB"):
            self.mock_db.tasks.find.return_value = []
            result = consume_and_filter_queued_tasks()
            self.assertIsNone(result)

    def test_consume_and_filter_queued_tasks_invalid_type(self):
        self.patcher_queue_process_type.stop()
        with patch('reflow_kafka_task_executor_v4.QUEUE_PROCESS_TYPE', new="INVALID_TYPE"):
            with self.assertRaises(AirflowFailException) as cm:
                consume_and_filter_queued_tasks()
            self.assertIn("Invalid QUEUE_PROCESS_TYPE", str(cm.exception))


    # --- Test execute_and_transition_task ---
    def test_execute_and_transition_task_already_terminal(self):
        task_data = {"_id": "t1", "phaseId": "p1", "releaseId": "r1"}
        self.mock_db.tasks.find_one.return_value = {"_id": "t1", "state": STATE_COMPLETED, "phaseId": "p1"}
        
        # Patch the internal helper call in this function
        with patch('reflow_kafka_task_executor_v4._check_and_transition_parent_states') as mock_check_parents:
            execute_and_transition_task.function(task_data)
            self.mock_db.tasks.find_one.assert_called_once_with({"_id": "t1"})
            mock_check_parents.assert_called_once_with(self.mock_db, "t1", "p1", None)
            self.mock_update_entity_state.assert_not_called() # Should not update state if already terminal

    def test_execute_and_transition_task_queue_to_in_progress_success(self):
        task_data = {"_id": "t1", "name": "Task 1", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_REGULAR, "actor": "A1"}
        self.mock_db.tasks.find_one.side_effect = [
            {"_id": "t1", "state": STATE_QUEUED, "name": "Task 1", "type": TASK_TYPE_REGULAR, "phaseId": "p1", "releaseId": "r1", "actor": "A1"},
            {"_id": "t1", "state": STATE_IN_PROGRESS, "name": "Task 1", "type": TASK_TYPE_REGULAR, "phaseId": "p1", "releaseId": "r1", "actor": "A1"},
            {"_id": "t1", "state": STATE_COMPLETED, "name": "Task 1", "type": TASK_TYPE_REGULAR, "phaseId": "p1", "releaseId": "r1", "actor": "A1", "nextTaskId": None}
        ]
        
        with patch('reflow_kafka_task_executor_v4._check_and_transition_parent_states'):
            execute_and_transition_task.function(task_data)
            self.mock_update_entity_state.assert_has_calls([
                call(self.mock_db, "tasks", "t1", STATE_IN_PROGRESS, old_state_condition=STATE_QUEUED, additional_fields={"reason": f"Automatically moved to IN_PROGRESS by {APPROVER_NAME}"}, approver_name=APPROVER_NAME),
                call(self.mock_db, "tasks", "t1", STATE_COMPLETED, additional_fields={"reason": f"JAR exited successfully but task state not terminal. Marked COMPLETED by DAG. ({APPROVER_NAME})"}, approver_name=APPROVER_NAME)
            ])
            self.mock_subprocess_run.assert_called_once()
            self.mock_append_logs.assert_called_once()

    def test_execute_and_transition_task_queue_to_in_progress_fail_concurrency(self):
        task_data = {"_id": "t1", "name": "Task 1", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_REGULAR, "actor": "A1"}
        self.mock_db.tasks.find_one.return_value = {"_id": "t1", "state": STATE_QUEUED, "name": "Task 1", "type": TASK_TYPE_REGULAR, "phaseId": "p1", "releaseId": "r1", "actor": "A1"}
        self.mock_update_entity_state.return_value = False # Simulate failed optimistic lock for IN_PROGRESS
        
        execute_and_transition_task.function(task_data)
        self.mock_update_entity_state.assert_called_once_with(
            self.mock_db, "tasks", "t1", STATE_IN_PROGRESS, old_state_condition=STATE_QUEUED,
            additional_fields={"reason": f"Automatically moved to IN_PROGRESS by {APPROVER_NAME}"}, approver_name=APPROVER_NAME
        )
        self.mock_subprocess_run.assert_not_called() # Should not proceed if state transition fails

    def test_execute_and_transition_task_regular_success(self):
        task_data = {"_id": "t_reg", "name": "Reg Task", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_REGULAR, "actor": "ACT"}
        # Mock find_one for initial fetch and after JAR execution
        self.mock_db.tasks.find_one.side_effect = [
            {"_id": "t_reg", "state": STATE_QUEUED, "name": "Reg Task", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_REGULAR, "actor": "ACT"}, # Initial fetch
            {"_id": "t_reg", "state": STATE_IN_PROGRESS, "name": "Reg Task", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_REGULAR, "actor": "ACT"}, # After IN_PROGRESS transition
            {"_id": "t_reg", "state": STATE_COMPLETED, "name": "Reg Task", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_REGULAR, "actor": "ACT", "nextTaskId": None} # After JAR reports success (already completed in DB)
        ]
        
        with patch('reflow_kafka_task_executor_v4._check_and_transition_parent_states'):
            execute_and_transition_task.function(task_data)
            self.mock_subprocess_run.assert_called_once()
            self.mock_append_logs.assert_called_once()
            self.mock_update_entity_state.assert_has_calls([
                call(self.mock_db, "tasks", "t_reg", STATE_IN_PROGRESS, old_state_condition=STATE_QUEUED, additional_fields={"reason": f"Automatically moved to IN_PROGRESS by {APPROVER_NAME}"}, approver_name=APPROVER_NAME)
                # No call to set to COMPLETED if JAR's success already moved it to COMPLETED
            ])
            # self.mock_check_parents.assert_called_once() # This will be called by internal helper

    def test_execute_and_transition_task_regular_failed_jar(self):
        task_data = {"_id": "t_fail", "name": "Fail Task", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_REGULAR, "actor": "ACT"}
        self.mock_db.tasks.find_one.side_effect = [
            {"_id": "t_fail", "state": STATE_QUEUED, "name": "Fail Task", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_REGULAR, "actor": "ACT"},
            {"_id": "t_fail", "state": STATE_IN_PROGRESS, "name": "Fail Task", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_REGULAR, "actor": "ACT"}
        ]
        self.mock_subprocess_run.return_value = MagicMock(returncode=1, stdout="", stderr="JAR Error!")
        
        with self.assertRaises(AirflowFailException) as cm:
            execute_and_transition_task.function(task_data)
        self.assertIn("JAR execution failed", str(cm.exception))
        self.mock_update_entity_state.assert_called_with(
            self.mock_db, "tasks", "t_fail", STATE_FAILED,
            additional_fields={"reason": f"JAR execution failed: JAR execution for t_fail reported failure (exit code 1). Stderr: JAR Error! ({APPROVER_NAME})"},
            approver_name=APPROVER_NAME
        )
        self.mock_append_logs.assert_called_once()

    def test_execute_and_transition_task_approval_approved_gate(self):
        task_data = {"_id": "t_appr_gate", "name": "Appr Gate", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_APPROVAL, "isGate": True, "gateStatus": GATE_STATUS_APPROVED}
        self.mock_db.tasks.find_one.side_effect = [
            {"_id": "t_appr_gate", "state": STATE_QUEUED, "name": "Appr Gate", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_APPROVAL, "isGate": True, "gateStatus": GATE_STATUS_APPROVED},
            {"_id": "t_appr_gate", "state": STATE_IN_PROGRESS, "name": "Appr Gate", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_APPROVAL, "isGate": True, "gateStatus": GATE_STATUS_APPROVED}, # After state transition
            {"_id": "t_appr_gate", "state": STATE_COMPLETED, "name": "Appr Gate", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_APPROVAL, "isGate": True, "gateStatus": GATE_STATUS_APPROVED, "nextTaskId": "next_task_id"} # After completion
        ]
        
        with patch('reflow_kafka_task_executor_v4._queue_next_task_after_completion') as mock_queue_next:
            with patch('reflow_kafka_task_executor_v4._check_and_transition_parent_states') as mock_check_parents:
                execute_and_transition_task.function(task_data)
                self.mock_update_entity_state.assert_called_with(
                    self.mock_db, "tasks", "t_appr_gate", STATE_COMPLETED,
                    old_state_condition=STATE_IN_PROGRESS,
                    additional_fields={"reason": f"Gate task passed and completed by {APPROVER_NAME}"},
                    approver_name=APPROVER_NAME
                )
                mock_queue_next.assert_called_once() # Should try to queue next task
                mock_check_parents.assert_called_once()
                self.mock_subprocess_run.assert_not_called() # No JAR execution for approval task

    def test_execute_and_transition_task_approval_rejected_gate(self):
        task_data = {"_id": "t_rej_gate", "name": "Rej Gate", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_APPROVAL, "isGate": True, "gateStatus": GATE_STATUS_REJECTED}
        self.mock_db.tasks.find_one.side_effect = [
            {"_id": "t_rej_gate", "state": STATE_QUEUED, "name": "Rej Gate", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_APPROVAL, "isGate": True, "gateStatus": GATE_STATUS_REJECTED},
            {"_id": "t_rej_gate", "state": STATE_IN_PROGRESS, "name": "Rej Gate", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_APPROVAL, "isGate": True, "gateStatus": GATE_STATUS_REJECTED}
        ]
        
        with patch('reflow_kafka_task_executor_v4._queue_next_task_after_completion') as mock_queue_next:
            with patch('reflow_kafka_task_executor_v4._check_and_transition_parent_states') as mock_check_parents:
                execute_and_transition_task.function(task_data)
                self.mock_update_entity_state.assert_called_with(
                    self.mock_db, "tasks", "t_rej_gate", STATE_FAILED,
                    old_state_condition=STATE_IN_PROGRESS,
                    additional_fields={"reason": f"Gate task failed. ({APPROVER_NAME})"},
                    approver_name=APPROVER_NAME
                )
                mock_queue_next.assert_not_called()
                mock_check_parents.assert_called_once()
                self.mock_subprocess_run.assert_not_called()

    def test_execute_and_transition_task_approval_pending_gate(self):
        task_data = {"_id": "t_pend_gate", "name": "Pend Gate", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_APPROVAL, "isGate": True, "gateStatus": GATE_STATUS_PENDING}
        self.mock_db.tasks.find_one.side_effect = [
            {"_id": "t_pend_gate", "state": STATE_QUEUED, "name": "Pend Gate", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_APPROVAL, "isGate": True, "gateStatus": GATE_STATUS_PENDING},
            {"_id": "t_pend_gate", "state": STATE_IN_PROGRESS, "name": "Pend Gate", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_APPROVAL, "isGate": True, "gateStatus": GATE_STATUS_PENDING}
        ]
        
        execute_and_transition_task.function(task_data)
        self.mock_update_entity_state.assert_called_with(
            self.mock_db, "tasks", "t_pend_gate", STATE_WAITING_FOR_APPROVAL,
            old_state_condition=STATE_IN_PROGRESS,
            additional_fields={"reason": f"Gate task awaiting external approval. ({APPROVER_NAME})"},
            approver_name=APPROVER_NAME
        )
        self.mock_subprocess_run.assert_not_called() # No JAR execution

    def test_execute_and_transition_task_parallel_group(self):
        task_data = {"_id": "t_par_group", "name": "Par Group", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_PARALLEL_GROUP, "childTaskIds": ["child1", "child2"]}
        self.mock_db.tasks.find_one.side_effect = [
            {"_id": "t_par_group", "state": STATE_QUEUED, "name": "Par Group", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_PARALLEL_GROUP, "childTaskIds": ["child1", "child2"]},
            {"_id": "t_par_group", "state": STATE_IN_PROGRESS, "name": "Par Group", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_PARALLEL_GROUP, "childTaskIds": ["child1", "child2"]},
            {"_id": "child1", "state": STATE_PLANNED},
            {"_id": "child2", "state": STATE_PLANNED}
        ]
        
        execute_and_transition_task.function(task_data)
        self.mock_update_entity_state.assert_has_calls([
            call(self.mock_db, "tasks", "t_par_group", STATE_IN_PROGRESS, old_state_condition=STATE_QUEUED, additional_fields={"reason": f"Automatically moved to IN_PROGRESS by {APPROVER_NAME}"}, approver_name=APPROVER_NAME),
            call(self.mock_db, "tasks", "child1", STATE_QUEUED, old_state_condition=STATE_PLANNED, additional_fields={"reason": f"Queued by {APPROVER_NAME} as part of parallel group t_par_group"}, approver_name=APPROVER_NAME),
            call(self.mock_db, "tasks", "child2", STATE_QUEUED, old_state_condition=STATE_PLANNED, additional_fields={"reason": f"Queued by {APPROVER_NAME} as part of parallel group t_par_group"}, approver_name=APPROVER_NAME)
        ])
        self.mock_subprocess_run.assert_not_called()

    def test_execute_and_transition_task_sequential_group(self):
        task_data = {"_id": "t_seq_group", "name": "Seq Group", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_SEQUENTIAL_GROUP, "childTaskIds": ["childA", "childB"]}
        self.mock_db.tasks.find_one.side_effect = [
            {"_id": "t_seq_group", "state": STATE_QUEUED, "name": "Seq Group", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_SEQUENTIAL_GROUP, "childTaskIds": ["childA", "childB"]},
            {"_id": "t_seq_group", "state": STATE_IN_PROGRESS, "name": "Seq Group", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_SEQUENTIAL_GROUP, "childTaskIds": ["childA", "childB"]},
            {"_id": "childA", "state": STATE_PLANNED, "groupId": "t_seq_group", "previousTaskId": None} # First child
        ]
        
        execute_and_transition_task.function(task_data)
        self.mock_update_entity_state.assert_has_calls([
            call(self.mock_db, "tasks", "t_seq_group", STATE_IN_PROGRESS, old_state_condition=STATE_QUEUED, additional_fields={"reason": f"Automatically moved to IN_PROGRESS by {APPROVER_NAME}"}, approver_name=APPROVER_NAME),
            call(self.mock_db, "tasks", "childA", STATE_QUEUED, old_state_condition=STATE_PLANNED, additional_fields={"reason": f"Queued by {APPROVER_NAME} as first child of sequential group t_seq_group"}, approver_name=APPROVER_NAME)
        ])
        self.mock_subprocess_run.assert_not_called()

    def test_execute_and_transition_task_trigger_defer(self):
        task_data = {"_id": "t_trigger", "name": "Trigger Task", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_TRIGGER, "actor": "ACT", "triggerInterval": 10}
        self.mock_db.tasks.find_one.side_effect = [
            {"_id": "t_trigger", "state": STATE_QUEUED, "name": "Trigger Task", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_TRIGGER, "actor": "ACT", "triggerInterval": 10},
            {"_id": "t_trigger", "state": STATE_IN_PROGRESS, "name": "Trigger Task", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_TRIGGER, "actor": "ACT", "triggerInterval": 10},
            {"_id": "t_trigger", "state": STATE_IN_PROGRESS, "name": "Trigger Task", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_TRIGGER, "actor": "ACT", "triggerInterval": 10} # Still in_progress after JAR run
        ]
        
        with self.assertRaises(AirflowRescheduleException) as cm:
            execute_and_transition_task.function(task_data)
        self.assertIn("TRIGGER task t_trigger deferred", str(cm.exception))
        self.mock_subprocess_run.assert_called_once()
        self.mock_append_logs.assert_called_once()
        self.mock_update_entity_state.assert_called_once_with(
            self.mock_db, "tasks", "t_trigger", STATE_IN_PROGRESS, old_state_condition=STATE_QUEUED,
            additional_fields={"reason": f"Automatically moved to IN_PROGRESS by {APPROVER_NAME}"}, approver_name=APPROVER_NAME
        ) # Only initial transition

    def test_execute_and_transition_task_trigger_completed(self):
        task_data = {"_id": "t_trigger_comp", "name": "Trigger Complete", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_TRIGGER, "actor": "ACT", "triggerInterval": 10}
        self.mock_db.tasks.find_one.side_effect = [
            {"_id": "t_trigger_comp", "state": STATE_QUEUED, "name": "Trigger Complete", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_TRIGGER, "actor": "ACT", "triggerInterval": 10},
            {"_id": "t_trigger_comp", "state": STATE_IN_PROGRESS, "name": "Trigger Complete", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_TRIGGER, "actor": "ACT", "triggerInterval": 10},
            {"_id": "t_trigger_comp", "state": STATE_COMPLETED, "name": "Trigger Complete", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_TRIGGER, "actor": "ACT", "triggerInterval": 10} # Now COMPLETED
        ]
        
        with patch('reflow_kafka_task_executor_v4._check_and_transition_parent_states') as mock_check_parents:
            execute_and_transition_task.function(task_data)
            self.mock_subprocess_run.assert_called_once()
            self.mock_append_logs.assert_called_once()
            self.mock_update_entity_state.assert_called_once_with(
                self.mock_db, "tasks", "t_trigger_comp", STATE_IN_PROGRESS, old_state_condition=STATE_QUEUED,
                additional_fields={"reason": f"Automatically moved to IN_PROGRESS by {APPROVER_NAME}"}, approver_name=APPROVER_NAME
            )
            mock_check_parents.assert_called_once()

    def test_execute_and_transition_task_scheduler(self):
        task_data = {"_id": "t_sched", "name": "Scheduler Task", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_SCHEDULER}
        self.mock_db.tasks.find_one.side_effect = [
            {"_id": "t_sched", "state": STATE_QUEUED, "name": "Scheduler Task", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_SCHEDULER},
            {"_id": "t_sched", "state": STATE_IN_PROGRESS, "name": "Scheduler Task", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_SCHEDULER},
            {"_id": "t_sched", "state": STATE_COMPLETED, "name": "Scheduler Task", "phaseId": "p1", "releaseId": "r1", "type": TASK_TYPE_SCHEDULER, "nextTaskId": None} # After completion
        ]
        
        with patch('reflow_kafka_task_executor_v4._check_and_transition_parent_states') as mock_check_parents:
            execute_and_transition_task.function(task_data)
            self.mock_update_entity_state.assert_has_calls([
                call(self.mock_db, "tasks", "t_sched", STATE_IN_PROGRESS, old_state_condition=STATE_QUEUED, additional_fields={"reason": f"Automatically moved to IN_PROGRESS by {APPROVER_NAME}"}, approver_name=APPROVER_NAME),
                call(self.mock_db, "tasks", "t_sched", STATE_COMPLETED, old_state_condition=STATE_IN_PROGRESS, additional_fields={"reason": f"SCHEDULER task time reached and processed by DAG. ({APPROVER_NAME})"}, approver_name=APPROVER_NAME)
            ])
            self.mock_subprocess_run.assert_not_called()
            mock_check_parents.assert_called_once()

    def test_execute_and_transition_task_unknown_type(self):
        task_data = {"_id": "t_unk", "name": "Unknown Task", "phaseId": "p1", "releaseId": "r1", "type": "UNKNOWN_TYPE"}
        self.mock_db.tasks.find_one.side_effect = [
            {"_id": "t_unk", "state": STATE_QUEUED, "name": "Unknown Task", "phaseId": "p1", "releaseId": "r1", "type": "UNKNOWN_TYPE"},
            {"_id": "t_unk", "state": STATE_IN_PROGRESS, "name": "Unknown Task", "phaseId": "p1", "releaseId": "r1", "type": "UNKNOWN_TYPE"}
        ]
        
        execute_and_transition_task.function(task_data)
        self.mock_update_entity_state.assert_called_once_with( # Only initial IN_PROGRESS transition
            self.mock_db, "tasks", "t_unk", STATE_IN_PROGRESS, old_state_condition=STATE_QUEUED,
            additional_fields={"reason": f"Automatically moved to IN_PROGRESS by {APPROVER_NAME}"}, approver_name=APPROVER_NAME
        )
        self.mock_subprocess_run.assert_not_called()


    # --- Test _queue_next_task_after_completion ---
    def test_queue_next_task_after_completion_with_next_task(self):
        completed_task = {"_id": "t1", "nextTaskId": "t2"}
        next_task_doc = {"_id": "t2", "state": STATE_PLANNED, "isGate": False, "gateStatus": None}
        self.mock_db.tasks.find_one.return_value = next_task_doc
        
        _queue_next_task_after_completion(self.mock_db, completed_task)
        self.mock_db.tasks.find_one.assert_called_once_with({"_id": "t2"})
        self.mock_update_entity_state.assert_called_once_with(
            self.mock_db, "tasks", "t2", STATE_QUEUED, old_state_condition=STATE_PLANNED,
            additional_fields={"reason": f"Queued by {APPROVER_NAME} after completion of t1"}, approver_name=APPROVER_NAME
        )

    def test_queue_next_task_after_completion_no_next_task(self):
        completed_task = {"_id": "t1", "nextTaskId": None}
        _queue_next_task_after_completion(self.mock_db, completed_task)
        self.mock_db.tasks.find_one.assert_not_called()
        self.mock_update_entity_state.assert_not_called()

    def test_queue_next_task_after_completion_next_task_approved_gate(self):
        completed_task = {"_id": "t1", "nextTaskId": "t_gate"}
        next_task_doc = {"_id": "t_gate", "state": STATE_PLANNED, "isGate": True, "gateStatus": GATE_STATUS_APPROVED}
        self.mock_db.tasks.find_one.return_value = next_task_doc
        
        _queue_next_task_after_completion(self.mock_db, completed_task)
        self.mock_update_entity_state.assert_called_once_with(
            self.mock_db, "tasks", "t_gate", STATE_QUEUED, old_state_condition=STATE_PLANNED,
            additional_fields={"reason": f"Queued by {APPROVER_NAME} after completion of t1 (next is approved gate)"}, approver_name=APPROVER_NAME
        )

    def test_queue_next_task_after_completion_next_task_rejected_gate(self):
        completed_task = {"_id": "t1", "nextTaskId": "t_gate"}
        next_task_doc = {"_id": "t_gate", "state": STATE_PLANNED, "isGate": True, "gateStatus": GATE_STATUS_REJECTED}
        self.mock_db.tasks.find_one.return_value = next_task_doc
        
        _queue_next_task_after_completion(self.mock_db, completed_task)
        self.mock_update_entity_state.assert_not_called() # Should not queue a rejected gate


    # --- Test _check_and_transition_parent_states ---
    def test_check_and_transition_parent_states_phase_completion(self):
        self.mock_db.tasks.find.return_value = [
            {"_id": "t1", "state": STATE_COMPLETED},
            {"_id": "t2", "state": STATE_SKIPPED}
        ]
        self.mock_db.phases.find_one.side_effect = [
            {"_id": "p1", "nextPhaseId": None}, # current phase doc, no next phase
            {"_id": "p1", "nextPhaseId": None} # re-fetch (after phase update) for release check
        ]
        
        _check_and_transition_parent_states(self.mock_db, "t1", "p1", None)
        self.mock_db.tasks.find.assert_called_once_with({"phaseId": "p1"})
        self.mock_update_entity_state.assert_called_once_with(
            self.mock_db, "phases", "p1", STATE_COMPLETED,
            additional_fields={"reason": f"Phase completed as all tasks are terminal. ({APPROVER_NAME})"},
            approver_name=APPROVER_NAME
        )

    def test_check_and_transition_parent_states_no_phase_completion(self):
        self.mock_db.tasks.find.return_value = [
            {"_id": "t1", "state": STATE_COMPLETED},
            {"_id": "t2", "state": STATE_IN_PROGRESS}
        ]
        self.mock_db.phases.find_one.return_value = {"_id": "p1", "nextPhaseId": None}
        
        _check_and_transition_parent_states(self.mock_db, "t1", "p1", None)
        self.mock_update_entity_state.assert_not_called() # No phase state update

    def test_check_and_transition_parent_states_trigger_next_phase_and_task(self):
        # Setup mocks for checking tasks in phase
        self.mock_db.tasks.find.return_value = [
            {"_id": "t1", "state": STATE_COMPLETED},
            {"_id": "t2", "state": STATE_COMPLETED}
        ]
        # Setup mocks for finding current phase and next phase
        self.mock_db.phases.find_one.side_effect = [
            {"_id": "p1", "nextPhaseId": "p2"}, # Current phase P1, has next phase P2
            {"_id": "p2", "state": STATE_PLANNED}, # Next phase P2, is PLANNED
            {"_id": "p2", "state": STATE_IN_PROGRESS} # Next phase P2 after IN_PROGRESS transition
        ]
        # Setup mocks for finding the first task of the next phase
        self.mock_db.tasks.find.side_effect = [
            # First call for phase completion check
            [{"_id": "t1", "state": STATE_COMPLETED}, {"_id": "t2", "state": STATE_COMPLETED}],
            # Second call for finding first task of next phase (p2)
            MagicMock( # This needs to be a mock cursor that behaves like a real one
                sort=MagicMock(return_value=MagicMock(
                    limit=MagicMock(return_value=[{"_id": "t_next_phase_first", "state": STATE_PLANNED, "previousTaskId": None, "groupId": None, "phaseId": "p2"}])
                ))
            )
        ]
        self.mock_db.releases.find_one.return_value = None # No parent release for simplicity of this test
        
        _check_and_transition_parent_states(self.mock_db, "t1", "p1", None)
        
        self.mock_update_entity_state.assert_has_calls([
            # 1. Phase P1 completes
            call(self.mock_db, "phases", "p1", STATE_COMPLETED,
                additional_fields={"reason": f"Phase completed as all tasks are terminal. ({APPROVER_NAME})"},
                approver_name=APPROVER_NAME),
            # 2. Next phase P2 transitions to IN_PROGRESS
            call(self.mock_db, "phases", "p2", STATE_IN_PROGRESS,
                old_state_condition=STATE_PLANNED,
                additional_fields={"reason": f"Auto-transitioned by completion of previous phase p1 by {APPROVER_NAME}"},
                approver_name=APPROVER_NAME),
            # 3. First task of P2 transitions to QUEUED
            call(self.mock_db, "tasks", "t_next_phase_first", STATE_QUEUED,
                old_state_condition=STATE_PLANNED,
                additional_fields={"reason": f"Auto-queued as first task of phase p2 initiated by {APPROVER_NAME}"},
                approver_name=APPROVER_NAME)
        ])

    def test_check_and_transition_parent_states_release_completion(self):
        self.mock_db.tasks.find.return_value = [
            {"_id": "t1", "state": STATE_COMPLETED} # For phase completion check
        ]
        self.mock_db.phases.find_one.side_effect = [
            {"_id": "p1", "nextPhaseId": None}, # Current phase P1 (no next)
            {"_id": "p1", "nextPhaseId": None}  # Re-fetch for release check (assuming P1 completed)
        ]
        self.mock_db.releases.find_one.return_value = {"_id": "r1", "phaseIds": ["p1"], "releaseGroupId": None} # Parent release of P1
        self.mock_db.phases.find.return_value = [
            {"_id": "p1", "state": STATE_COMPLETED, "parentId": "r1", "parentType": "RELEASE"}
        ] # All phases in release R1 are terminal

        _check_and_transition_parent_states(self.mock_db, "t1", "p1", None)
        self.mock_update_entity_state.assert_has_calls([
            call(self.mock_db, "phases", "p1", STATE_COMPLETED, additional_fields={"reason": f"Phase completed as all tasks are terminal. ({APPROVER_NAME})"}, approver_name=APPROVER_NAME),
            call(self.mock_db, "releases", "r1", STATE_COMPLETED, additional_fields={"reason": f"Release completed as all phases are terminal. ({APPROVER_NAME})"}, approver_name=APPROVER_NAME)
        ])

    def test_check_and_transition_parent_states_release_group_completion(self):
        self.mock_db.tasks.find.return_value = [{"_id": "t1", "state": STATE_COMPLETED}]
        self.mock_db.phases.find_one.side_effect = [
            {"_id": "p1", "nextPhaseId": None},
            {"_id": "p1", "nextPhaseId": None}
        ]
        self.mock_db.releases.find_one.return_value = {"_id": "r1", "phaseIds": ["p1"], "releaseGroupId": "rg1"}
        self.mock_db.phases.find.return_value = [
            {"_id": "p1", "state": STATE_COMPLETED, "parentId": "r1", "parentType": "RELEASE"}
        ]
        self.mock_db.releases.find.return_value = [
            {"_id": "r1", "state": STATE_COMPLETED, "releaseGroupId": "rg1"}
        ] # All releases in RG1 are terminal

        _check_and_transition_parent_states(self.mock_db, "t1", "p1", None)
        self.mock_update_entity_state.assert_has_calls([
            call(self.mock_db, "phases", "p1", STATE_COMPLETED, additional_fields={"reason": f"Phase completed as all tasks are terminal. ({APPROVER_NAME})"}, approver_name=APPROVER_NAME),
            call(self.mock_db, "releases", "r1", STATE_COMPLETED, additional_fields={"reason": f"Release completed as all phases are terminal. ({APPROVER_NAME})"}, approver_name=APPROVER_NAME),
            call(self.mock_db, "releaseGroups", "rg1", STATE_COMPLETED, additional_fields={"reason": f"Release Group completed as all releases are terminal. ({APPROVER_NAME})"}, approver_name=APPROVER_NAME)
        ])


if __name__ == '__main__':
    unittest.main()

