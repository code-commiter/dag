# generate_test_data.py
import pendulum
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import uuid
import random

# --- MongoDB Configuration ---
MONGO_HOST = "localhost"
MONGO_PORT = 27017
MONGO_DB_NAME = "reflow_db" # Ensure this matches your Spring Boot and Airflow configs

# --- Helper for ID Generation (mimics Spring Boot's CustomIdGenerator) ---
def generate_id(prefix):
    return f"{prefix}_{uuid.uuid4().hex[:12]}" # Shortened UUID for readability

# --- MongoDB Client Connection ---
def get_mongo_client():
    try:
        client = MongoClient(MONGO_HOST, MONGO_PORT)
        client.admin.command('ping') # Test connection
        print(f"Successfully connected to MongoDB at {MONGO_HOST}:{MONGO_PORT}")
        return client
    except ConnectionFailure as e:
        print(f"MongoDB connection failed: {e}")
        raise

# --- Data Creation Functions ---

def create_release_group(db, name, description, release_context_id):
    rg_id = generate_id("RG")
    rg_doc = {
        "_id": rg_id,
        "name": name,
        "description": description,
        "state": "PLANNED",
        "releaseIds": [], # To be populated with child releases
        "phaseIds": [],   # To be populated with direct phases
        "releaseContextId": release_context_id,
        "createdAt": pendulum.now("UTC").isoformat(),
        "updatedAt": pendulum.now("UTC").isoformat(),
        "version": 1
    }
    db.releaseGroups.insert_one(rg_doc)
    print(f"Created ReleaseGroup: {rg_id} - '{name}'")
    return rg_doc

def create_release(db, name, description, release_group_id=None, release_context_id=None):
    r_id = generate_id("R")
    rel_doc = {
        "_id": r_id,
        "releaseGroupId": release_group_id,
        "name": name,
        "description": description,
        "state": "PLANNED",
        "phaseIds": [], # To be populated with child phases
        "releaseContextId": release_context_id if release_context_id else r_id, # Default to own ID
        "createdAt": pendulum.now("UTC").isoformat(),
        "updatedAt": pendulum.now("UTC").isoformat(),
        "version": 1
    }
    db.releases.insert_one(rel_doc)
    print(f"  Created Release: {r_id} - '{name}' (Group: {release_group_id if release_group_id else 'None'})")
    return rel_doc

def create_phase(db, name, description, parent_id, parent_type, previous_phase_id=None, next_phase_id=None):
    p_id = generate_id("P")
    phase_doc = {
        "_id": p_id,
        "parentType": parent_type,
        "parentId": parent_id,
        "name": name,
        "description": description,
        "state": "PLANNED",
        "taskIds": [], # To be populated with child tasks
        "previousPhaseId": previous_phase_id, # NEW
        "nextPhaseId": next_phase_id,         # NEW
        "createdAt": pendulum.now("UTC").isoformat(),
        "updatedAt": pendulum.now("UTC").isoformat(),
        "version": 1
    }
    db.phases.insert_one(phase_doc)
    print(f"    Created Phase: {p_id} - '{name}' (Parent: {parent_id})")
    return phase_doc

def create_task(db, name, description, task_type, phase_id, release_id, additional_props=None):
    t_id = generate_id("T")
    task_doc = {
        "_id": t_id,
        "name": name,
        "description": description,
        "type": task_type,
        "state": "PLANNED", # Default state
        "phaseId": phase_id,
        "releaseId": release_id,
        "previousTaskId": None,
        "nextTaskId": None,
        "groupId": None,
        "childTaskIds": None, # Only for group tasks
        "scheduledTime": None,
        "approvers": [],
        "approvedBy": None,
        "approvalDate": None,
        "reason": None,
        "isGate": False,
        "gateStatus": None,
        "gateCategory": None,
        "actor": "ORCHESTRATOR", # Default actor
        "taskVariables": {},
        "logs": "",
        "triggerInterval": None,
        "inputVariables": {},
        "outputVariables": {},
        "createdAt": pendulum.now("UTC").isoformat(),
        "updatedAt": pendulum.now("UTC").isoformat(),
        "version": 1
    }
    if additional_props:
        task_doc.update(additional_props)

    db.tasks.insert_one(task_doc)
    print(f"      Created Task: {t_id} - '{name}' (Type: {task_type})")
    return task_doc

# --- Main Data Generation Logic ---
def generate_all_data(db):
    print("\n--- Starting Data Generation ---")

    # Clear existing data for a clean run - COMMENTED OUT AS REQUESTED
    # print("\nClearing existing data...")
    # db.releaseGroups.delete_many({})
    # db.releases.delete_many({})
    # db.phases.delete_many({})
    # db.tasks.delete_many({})
    # db.releaseContexts.delete_many({})

    # Store IDs for logging at the end
    all_release_group_ids = []
    all_release_ids = []
    all_phase_ids = []
    all_task_ids = []

    # --- 1. Create a Release Group with 5 child Releases ---
    rg_name = "Core Application Suite RG"
    rg_desc = "Release group for all core application releases."
    core_app_rg = create_release_group(db, rg_name, rg_desc, generate_id("RC"))
    all_release_group_ids.append(core_app_rg["_id"])

    # Create a direct phase under the Release Group (e.g., for a suite-wide approval gate)
    rg_phase = create_phase(db, "Suite-wide Approval", "Central approval for the release group.",
                            core_app_rg["_id"], "RELEASE_GROUP")
    core_app_rg["phaseIds"].append(rg_phase["_id"])
    rg_gate_task = create_task(db, "Architecture Review Gate", "Final architecture review for the suite.",
                               "APPROVAL", rg_phase["_id"], None,
                               {"isGate": True, "gateStatus": "PENDING", "gateCategory": "Architecture", "actor": "ARCHITECT"})
    rg_phase["taskIds"].append(rg_gate_task["_id"])
    db.phases.update_one({"_id": rg_phase["_id"]}, {"$set": {"taskIds": rg_phase["taskIds"]}})


    child_releases = []
    for i in range(1, 6):
        rel = create_release(db, f"Child App {i} v1.0", f"Release for child application {i}.",
                             core_app_rg["_id"], core_app_rg["releaseContextId"])
        child_releases.append(rel)
        all_release_ids.append(rel["_id"])
        core_app_rg["releaseIds"].append(rel["_id"])

        # Populate each child release with phases and tasks
        # Phase 1: Development
        dev_phase = create_phase(db, "Development", "Development tasks for this release.", rel["_id"], "RELEASE")
        all_phase_ids.append(dev_phase["_id"])
        rel["phaseIds"].append(dev_phase["_id"])

        # Regular Task
        task1 = create_task(db, "Code Feature A", "Develop core feature A.", "REGULAR", dev_phase["_id"], rel["_id"], {"actor": "DEVELOPER"})
        dev_phase["taskIds"].append(task1["_id"])
        all_task_ids.append(task1["_id"])

        # Update phase with task IDs
        db.phases.update_one({"_id": dev_phase["_id"]}, {"$set": {"taskIds": dev_phase["taskIds"]}})

        # Phase 2: Testing & Gates
        test_phase = create_phase(db, "Testing & Gates", "Testing and approval gates for this release.", rel["_id"], "RELEASE")
        all_phase_ids.append(test_phase["_id"])
        rel["phaseIds"].append(test_phase["_id"])

        # Parallel Group Task with children
        parallel_group_task = create_task(db, "Run All Automated Tests", "Execute integration and E2E tests in parallel.",
                                          "PARALLEL_GROUP", test_phase["_id"], rel["_id"], {"actor": "TEST_ORCHESTRATOR"})
        all_task_ids.append(parallel_group_task["_id"])
        test_phase["taskIds"].append(parallel_group_task["_id"])

        parallel_child1 = create_task(db, "Run Integration Tests", "Execute API integration test suite.", "REGULAR",
                                       test_phase["_id"], rel["_id"], {"groupId": parallel_group_task["_id"], "actor": "TEST_ENGINE"})
        parallel_child2 = create_task(db, "Run E2E Tests", "Execute UI End-to-End test suite.", "REGULAR",
                                       test_phase["_id"], rel["_id"], {"groupId": parallel_group_task["_id"], "actor": "TEST_ENGINE"})
        
        parallel_group_task["childTaskIds"] = [parallel_child1["_id"], parallel_child2["_id"]]
        db.tasks.update_one({"_id": parallel_group_task["_id"]}, {"$set": {"childTaskIds": parallel_group_task["childTaskIds"]}})
        all_task_ids.extend([parallel_child1["_id"], parallel_child2["_id"]])
        test_phase["taskIds"].extend([parallel_child1["_id"], parallel_child2["_id"]])


        # Approval Gate Task
        approval_gate_task = create_task(db, "QA Sign-off Gate", "Requires manual QA team approval for release readiness.",
                                         "APPROVAL", test_phase["_id"], rel["_id"],
                                         {"isGate": True, "gateStatus": "PENDING", "approvers": ["qa_lead"], "actor": "QA_LEAD", "gateCategory": "Quality Assurance"})
        all_task_ids.append(approval_gate_task["_id"])
        test_phase["taskIds"].append(approval_gate_task["_id"])

        # Update phase with task IDs
        db.phases.update_one({"_id": test_phase["_id"]}, {"$set": {"taskIds": test_phase["taskIds"]}})

        # Phase 3: Deployment & Post-Deployment
        deploy_phase = create_phase(db, "Deployment", "Deployment and post-deployment validation tasks.", rel["_id"], "RELEASE")
        all_phase_ids.append(deploy_phase["_id"])
        rel["phaseIds"].append(deploy_phase["_id"])

        # Trigger Task (e.g., for continuous deployment monitoring)
        trigger_task = create_task(db, "Monitor Production Deployment", "Continuously checks deployment status in production.",
                                   "TRIGGER", deploy_phase["_id"], rel["_id"],
                                   {"actor": "DEPLOYMENT_MONITOR", "triggerInterval": 30, "state": "QUEUED"}) # Example initial state
        all_task_ids.append(trigger_task["_id"])
        deploy_phase["taskIds"].append(trigger_task["_id"])


        # Sequential Group Task with children
        sequential_group_task = create_task(db, "Post-Deployment Checks", "Run security scans and data validation sequentially.",
                                            "SEQUENTIAL_GROUP", deploy_phase["_id"], rel["_id"], {"actor": "ORCHESTRATOR"})
        all_task_ids.append(sequential_group_task["_id"])
        deploy_phase["taskIds"].append(sequential_group_task["_id"])

        seq_child1 = create_task(db, "Security Scan", "Perform security vulnerability scan.", "REGULAR",
                                  deploy_phase["_id"], rel["_id"], {"groupId": sequential_group_task["_id"], "actor": "SECURITY_TOOL"})
        seq_child2 = create_task(db, "Data Validation", "Verify data integrity post-deployment.", "REGULAR",
                                  deploy_phase["_id"], rel["_id"], {"groupId": sequential_group_task["_id"], "actor": "DATA_VALIDATOR"})
        
        # Link sequential children correctly (previousTaskId)
        seq_child1["nextTaskId"] = seq_child2["_id"]
        db.tasks.update_one({"_id": seq_child1["_id"]}, {"$set": {"nextTaskId": seq_child1["nextTaskId"]}})
        seq_child2["previousTaskId"] = seq_child1["_id"]
        db.tasks.update_one({"_id": seq_child2["_id"]}, {"$set": {"previousTaskId": seq_child2["previousTaskId"]}})


        sequential_group_task["childTaskIds"] = [seq_child1["_id"], seq_child2["_id"]]
        db.tasks.update_one({"_id": sequential_group_task["_id"]}, {"$set": {"childTaskIds": sequential_group_task["childTaskIds"]}})
        all_task_ids.extend([seq_child1["_id"], seq_child2["_id"]])
        deploy_phase["taskIds"].extend([seq_child1["_id"], seq_child2["_id"]])


        # Scheduler Task (e.g., for a nightly health check)
        scheduler_task_time = pendulum.now("UTC").add(minutes=random.randint(5, 15)).replace(microsecond=0) # Schedule 5-15 mins from now
        scheduler_task = create_task(db, "Nightly Health Check", "Runs a comprehensive health check at a scheduled time.",
                                     "SCHEDULER", deploy_phase["_id"], rel["_id"],
                                     {"scheduledTime": scheduler_task_time.isoformat(), "state": "SCHEDULED", "actor": "HEALTH_MONITOR"})
        all_task_ids.append(scheduler_task["_id"])
        deploy_phase["taskIds"].append(scheduler_task["_id"])

        # Update phase with task IDs
        db.phases.update_one({"_id": deploy_phase["_id"]}, {"$set": {"taskIds": deploy_phase["taskIds"]}})

    # Finally, update the Release Group with its child releases' IDs
    db.releaseGroups.update_one({"_id": core_app_rg["_id"]}, {"$set": {"releaseIds": core_app_rg["releaseIds"], "phaseIds": core_app_rg["phaseIds"]}})


    # --- 2. Create 5 Independent Releases ---
    print("\n--- Creating 5 Independent Releases ---")
    independent_releases = []
    for i in range(1, 6):
        ind_rel = create_release(db, f"Independent Service {chr(64 + i)} v1.0", f"Release for standalone service {chr(64 + i)}.", None) # No release_group_id
        independent_releases.append(ind_rel)
        all_release_ids.append(ind_rel["_id"])

        # Populate with a full set of phases and tasks, similar to grouped releases
        # Phase 1: Development
        dev_phase = create_phase(db, f"Dev Phase {chr(64+i)}", "Development tasks for this independent release.", ind_rel["_id"], "RELEASE")
        all_phase_ids.append(dev_phase["_id"])
        ind_rel["phaseIds"].append(dev_phase["_id"])

        # Regular Task
        task1 = create_task(db, f"Code Feature X for {chr(64+i)}", "Develop core feature X.", "REGULAR", dev_phase["_id"], ind_rel["_id"], {"actor": "DEVELOPER"})
        dev_phase["taskIds"].append(task1["_id"])
        all_task_ids.append(task1["_id"])

        db.phases.update_one({"_id": dev_phase["_id"]}, {"$set": {"taskIds": dev_phase["taskIds"]}})

        # Phase 2: Testing & Gates
        test_phase = create_phase(db, f"Test Phase {chr(64+i)}", "Testing and approval gates for this independent release.", ind_rel["_id"], "RELEASE")
        all_phase_ids.append(test_phase["_id"])
        ind_rel["phaseIds"].append(test_phase["_id"])

        # Parallel Group Task with children
        parallel_group_task = create_task(db, f"Parallel Tests {chr(64+i)}", "Execute tests in parallel.",
                                          "PARALLEL_GROUP", test_phase["_id"], ind_rel["_id"], {"actor": "TEST_ORCHESTRATOR"})
        all_task_ids.append(parallel_group_task["_id"])
        test_phase["taskIds"].append(parallel_group_task["_id"])

        parallel_child1 = create_task(db, f"Integration Tests {chr(64+i)}", "Run integration test suite.", "REGULAR",
                                       test_phase["_id"], ind_rel["_id"], {"groupId": parallel_group_task["_id"], "actor": "TEST_ENGINE"})
        parallel_child2 = create_task(db, f"E2E Tests {chr(64+i)}", "Run E2E test suite.", "REGULAR",
                                       test_phase["_id"], ind_rel["_id"], {"groupId": parallel_group_task["_id"], "actor": "TEST_ENGINE"})
        
        parallel_group_task["childTaskIds"] = [parallel_child1["_id"], parallel_child2["_id"]]
        db.tasks.update_one({"_id": parallel_group_task["_id"]}, {"$set": {"childTaskIds": parallel_group_task["childTaskIds"]}})
        all_task_ids.extend([parallel_child1["_id"], parallel_child2["_id"]])
        test_phase["taskIds"].extend([parallel_child1["_id"], parallel_child2["_id"]])

        # Approval Gate Task
        approval_gate_task = create_task(db, f"Security Gate {chr(64+i)}", "Security review approval.",
                                         "APPROVAL", test_phase["_id"], ind_rel["_id"],
                                         {"isGate": True, "gateStatus": "PENDING", "approvers": ["security_auditor"], "actor": "SECURITY_AUDIT_TEAM", "gateCategory": "Security Compliance"})
        all_task_ids.append(approval_gate_task["_id"])
        test_phase["taskIds"].append(approval_gate_task["_id"])

        db.phases.update_one({"_id": test_phase["_id"]}, {"$set": {"taskIds": test_phase["taskIds"]}})

        # Phase 3: Operations & Monitoring
        ops_phase = create_phase(db, f"Ops Phase {chr(64+i)}", "Operations and monitoring tasks.", ind_rel["_id"], "RELEASE")
        all_phase_ids.append(ops_phase["_id"])
        ind_rel["phaseIds"].append(ops_phase["_id"])

        # Trigger Task
        trigger_task = create_task(db, f"Monitor Service {chr(64+i)}", "Monitor service health.",
                                   "TRIGGER", ops_phase["_id"], ind_rel["_id"],
                                   {"actor": "MONITORING_SYSTEM", "triggerInterval": 60, "state": "QUEUED"})
        all_task_ids.append(trigger_task["_id"])
        ops_phase["taskIds"].append(trigger_task["_id"])

        # Sequential Group Task with children
        sequential_group_task = create_task(db, f"Service Health Checks {chr(64+i)}", "Daily health checks.",
                                            "SEQUENTIAL_GROUP", ops_phase["_id"], ind_rel["_id"], {"actor": "OPS_MANAGER"})
        all_task_ids.append(sequential_group_task["_id"])
        ops_phase["taskIds"].append(sequential_group_task["_id"])

        seq_child1 = create_task(db, f"Daily Log Review {chr(64+i)}", "Review logs for errors.", "REGULAR",
                                  ops_phase["_id"], ind_rel["_id"], {"groupId": sequential_group_task["_id"], "actor": "LOG_ANALYZER"})
        seq_child2 = create_task(db, f"Resource Utilization Check {chr(64+i)}", "Check CPU/Memory usage.", "REGULAR",
                                  ops_phase["_id"], ind_rel["_id"], {"groupId": sequential_group_task["_id"], "actor": "RESOURCE_MONITOR"})
        
        seq_child1["nextTaskId"] = seq_child2["_id"]
        db.tasks.update_one({"_id": seq_child1["_id"]}, {"$set": {"nextTaskId": seq_child1["nextTaskId"]}})
        seq_child2["previousTaskId"] = seq_child1["_id"]
        db.tasks.update_one({"_id": seq_child2["_id"]}, {"$set": {"previousTaskId": seq_child2["previousTaskId"]}})

        sequential_group_task["childTaskIds"] = [seq_child1["_id"], seq_child2["_id"]]
        db.tasks.update_one({"_id": sequential_group_task["_id"]}, {"$set": {"childTaskIds": sequential_group_task["childTaskIds"]}})
        all_task_ids.extend([seq_child1["_id"], seq_child2["_id"]])
        ops_phase["taskIds"].extend([seq_child1["_id"], seq_child2["_id"]])

        # Scheduler Task
        scheduler_task_time = pendulum.now("UTC").add(minutes=random.randint(20, 30)).replace(microsecond=0) # Schedule 20-30 mins from now
        scheduler_task = create_task(db, f"Weekly Report Gen {chr(64+i)}", "Generate weekly performance report.",
                                     "SCHEDULER", ops_phase["_id"], ind_rel["_id"],
                                     {"scheduledTime": scheduler_task_time.isoformat(), "state": "SCHEDULED", "actor": "REPORT_ENGINE"})
        all_task_ids.append(scheduler_task["_id"])
        ops_phase["taskIds"].append(scheduler_task["_id"])

        db.phases.update_one({"_id": ops_phase["_id"]}, {"$set": {"taskIds": ops_phase["taskIds"]}})

        # Update independent release with phase IDs
        db.releases.update_one({"_id": ind_rel["_id"]}, {"$set": {"phaseIds": ind_rel["phaseIds"]}})


    print("\n--- Data Generation Complete ---")

    print("\n--- Summary of Created IDs ---")
    print(f"Release Group IDs: {all_release_group_ids}")
    print(f"Total Releases Created: {len(all_release_ids)} - {all_release_ids}")
    print(f"Total Phases Created: {len(all_phase_ids)} - {all_phase_ids}")
    print(f"Total Tasks Created: {len(all_task_ids)} - {all_task_ids}")

# --- New Simple Data Generation Logic ---
def generate_simple_data(db):
    print("\n--- Starting Simple Data Generation ---")

    # Clear existing data for a clean run
    print("\nClearing existing data...")
    db.releaseGroups.delete_many({})
    db.releases.delete_many({})
    db.phases.delete_many({})
    db.tasks.delete_many({})
    db.releaseContexts.delete_many({})

    all_release_ids = []
    all_phase_ids = []
    all_task_ids = []

    # Create a single Release
    simple_release_name = "Simple App Release v1.0"
    simple_release_desc = "A basic release with sequential tasks across phases."
    simple_release = create_release(db, simple_release_name, simple_release_desc, None, generate_id("RC_Simple"))
    all_release_ids.append(simple_release["_id"])
    
    num_phases = 3
    num_tasks_per_phase = 5

    # Variable to keep track of the previously created phase ID for linking
    previous_phase_id_in_sequence = None 

    for i in range(1, num_phases + 1):
        phase_name = f"Phase {i}"
        phase_desc = f"Phase {i} for {simple_release_name}."
        
        # Pass previous_phase_id_in_sequence to create_phase for linking
        phase = create_phase(db, phase_name, phase_desc, 
                             simple_release["_id"], "RELEASE", 
                             previous_phase_id=previous_phase_id_in_sequence)
        
        all_phase_ids.append(phase["_id"])
        simple_release["phaseIds"].append(phase["_id"])
        
        # If there was a previous phase, update its nextPhaseId
        if previous_phase_id_in_sequence:
            db.phases.update_one(
                {"_id": previous_phase_id_in_sequence},
                {"$set": {"nextPhaseId": phase["_id"]}}
            )
        
        # Update previous_phase_id_in_sequence for the next iteration
        previous_phase_id_in_sequence = phase["_id"]

        previous_task_id = None
        current_phase_task_ids = []

        for j in range(1, num_tasks_per_phase + 1):
            task_name = f"Task {j} in {phase_name}"
            task_desc = f"Regular task {j} for phase {phase_name}."
            
            task_props = {"actor": "DEVELOPER_BOT"}
            if previous_task_id:
                task_props["previousTaskId"] = previous_task_id

            task = create_task(db, task_name, task_desc, "REGULAR", phase["_id"], simple_release["_id"], task_props)
            all_task_ids.append(task["_id"])
            current_phase_task_ids.append(task["_id"])

            # Update the nextTaskId of the previous task to point to the current task
            if previous_task_id:
                db.tasks.update_one(
                    {"_id": previous_task_id},
                    {"$set": {"nextTaskId": task["_id"]}}
                )
            
            previous_task_id = task["_id"] # Set current task as previous for the next iteration

        # Update the phase document with all task IDs after creating them
        db.phases.update_one({"_id": phase["_id"]}, {"$set": {"taskIds": current_phase_task_ids}})

    # Update the release document with all phase IDs
    db.releases.update_one({"_id": simple_release["_id"]}, {"$set": {"phaseIds": simple_release["phaseIds"]}})

    print("\n--- Simple Data Generation Complete ---")
    print(f"Created Release: {simple_release['_id']}")
    print(f"Total Phases Created: {len(all_phase_ids)} - {all_phase_ids}")
    print(f"Total Tasks Created: {len(all_task_ids)} - {all_task_ids}")


if __name__ == "__main__":
    try:
        client = get_mongo_client()
        db = client[MONGO_DB_NAME]
        
        # Call the new simple data generation script
        generate_simple_data(db)

        # Comment out the original data generation
        # generate_all_data(db) 

    except ConnectionFailure:
        print("Failed to connect to MongoDB. Please ensure MongoDB is running and accessible.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'client' in locals() and client:
            client.close()
            print("MongoDB client closed.")
