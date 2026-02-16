# Databricks notebook source
# =========================================================
# 01_data_generator.py — Continuous Streaming Data Generator
# =========================================================
#
# PURPOSE:
#   Generates a continuous, infinite stream of realistic JSONL
#   events using real-time timestamps. Writes small batch files
#   to the Volume landing zone every few seconds for Auto Loader
#   to pick up incrementally.
#
#   Runs FOREVER until manually interrupted (cancel the cell
#   in Databricks, or Ctrl+C locally).
#
# HOW IT WORKS:
#   - Every EMIT_INTERVAL seconds, generates a small batch of
#     3–8 noise events (metrics, logs, warnings) with NOW()
#     timestamps across all applications and hosts.
#   - Periodically (every SCENARIO_INTERVAL seconds), injects
#     a full incident scenario (deployment incident, memory leak,
#     or infra restart) on top of the noise.
#   - Scenarios rotate: cycle 1 = deployment incident,
#     cycle 2 = memory leak, cycle 3 = infra restart, repeat.
#   - Each batch is written as a separate JSONL file so
#     Auto Loader triggers a new micro-batch per file.
#
# SCENARIOS:
#   1. Deployment-Induced Incident  (payments-api, host-a)
#   2. Memory Leak with no deploy   (orders-api, host-b)
#   3. Infra Restart / noise         (auth-service, host-a)
#
# TO STOP:
#   - In Databricks: click "Cancel" on the running cell
#   - Locally: Ctrl+C
# =========================================================

import json
import uuid
import hashlib
import random
import os
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any

# COMMAND ----------

# %run ./config          # Uncomment when running in Databricks

# For standalone execution, inline the needed config values:
RAW_EVENTS_PATH = "/Volumes/akash_s_demo/ams/alert_pipeline/raw_events"
APPLICATIONS = ["payments-api", "orders-api", "auth-service"]
HOSTS = ["host-a", "host-b"]

# ---------------------------------------------------------
# TUNING PARAMETERS
# ---------------------------------------------------------
EMIT_INTERVAL = 5          # Seconds between each noise batch file
NOISE_BATCH_SIZE = 5       # Noise events per emit cycle (3–8 range)
SCENARIO_INTERVAL = 120    # Seconds between scenario injections
SCENARIO_EVENT_DELAY = 2   # Seconds between files during a scenario burst

# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------

def make_id() -> str:
    """Generate a short unique identifier."""
    return str(uuid.uuid4())[:12]


def fingerprint(alert_type: str, app: str, host: str) -> str:
    """
    Deterministic fingerprint for alert deduplication.
    Same (alert_type, app, host) always produces the same fingerprint
    so the Silver layer can group repeated firings.
    """
    raw = f"{alert_type}|{app}|{host}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def now() -> datetime:
    """Current UTC time — all events use real-time timestamps."""
    return datetime.utcnow()


def ts_iso(dt: datetime) -> str:
    """ISO-8601 timestamp string."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def jitter_seconds(low: int = 0, high: int = 3) -> timedelta:
    """Small random offset to avoid perfectly aligned timestamps."""
    return timedelta(seconds=random.randint(low, high))


# ---------------------------------------------------------
# EVENT FACTORIES
# ---------------------------------------------------------

def make_metric(ts: datetime, app: str, host: str,
                metric_name: str, value: float) -> Dict[str, Any]:
    return {
        "event_type": "metric",
        "timestamp": ts_iso(ts),
        "application_id": app,
        "host_id": host,
        "metric_name": metric_name,
        "metric_value": round(value, 2),
        "alert_id": None,
        "fingerprint": None,
        "alert_type": None,
        "severity": None,
        "threshold": None,
        "current_value": None,
        "log_level": None,
        "message": None,
        "error_code": None,
        "deployment_id": None,
        "version": None,
        "change_type": None,
    }


def make_alert(ts: datetime, app: str, host: str,
               alert_type: str, severity: str,
               threshold: float, current_value: float) -> Dict[str, Any]:
    return {
        "event_type": "alert",
        "timestamp": ts_iso(ts),
        "application_id": app,
        "host_id": host,
        "metric_name": None,
        "metric_value": None,
        "alert_id": make_id(),
        "fingerprint": fingerprint(alert_type, app, host),
        "alert_type": alert_type,
        "severity": severity,
        "threshold": threshold,
        "current_value": current_value,
        "log_level": None,
        "message": None,
        "error_code": None,
        "deployment_id": None,
        "version": None,
        "change_type": None,
    }


def make_log(ts: datetime, app: str, host: str,
             level: str, message: str,
             error_code: str = None) -> Dict[str, Any]:
    return {
        "event_type": "log",
        "timestamp": ts_iso(ts),
        "application_id": app,
        "host_id": host,
        "metric_name": None,
        "metric_value": None,
        "alert_id": None,
        "fingerprint": None,
        "alert_type": None,
        "severity": None,
        "threshold": None,
        "current_value": None,
        "log_level": level,
        "message": message,
        "error_code": error_code,
        "deployment_id": None,
        "version": None,
        "change_type": None,
    }


def make_deployment(ts: datetime, app: str, host: str,
                    version: str,
                    change_type: str = "rolling_update") -> Dict[str, Any]:
    return {
        "event_type": "deployment",
        "timestamp": ts_iso(ts),
        "application_id": app,
        "host_id": host,
        "metric_name": None,
        "metric_value": None,
        "alert_id": None,
        "fingerprint": None,
        "alert_type": None,
        "severity": None,
        "threshold": None,
        "current_value": None,
        "log_level": None,
        "message": None,
        "error_code": None,
        "deployment_id": make_id(),
        "version": version,
        "change_type": change_type,
    }


# ---------------------------------------------------------
# NOISE GENERATOR (real-time, small batch)
# ---------------------------------------------------------

def generate_noise_batch(count: int = NOISE_BATCH_SIZE) -> List[Dict]:
    """
    Generate a small batch of background noise events with
    NOW() timestamps. Called every EMIT_INTERVAL seconds.
    """
    events = []
    t = now()

    for i in range(count):
        app = random.choice(APPLICATIONS)
        host = random.choice(HOSTS)
        ts = t + jitter_seconds(0, 3)

        roll = random.random()
        if roll < 0.45:
            name = random.choice(["cpu_percent", "memory_percent",
                                   "error_rate", "request_latency_ms"])
            val = {
                "cpu_percent": random.uniform(10, 45),
                "memory_percent": random.uniform(30, 60),
                "error_rate": random.uniform(0.001, 0.02),
                "request_latency_ms": random.uniform(50, 200),
            }[name]
            events.append(make_metric(ts, app, host, name, val))
        elif roll < 0.85:
            level = random.choice(["INFO", "DEBUG"])
            msgs = [
                "Request processed successfully",
                "Cache hit ratio: 0.94",
                "Health check passed",
                "Connection pool refreshed",
                "Scheduled job completed",
                "Session token rotated",
                "Config reload completed",
            ]
            events.append(make_log(ts, app, host, level,
                                   random.choice(msgs)))
        else:
            events.append(make_log(ts, app, host, "WARN",
                                   "Transient timeout on downstream call",
                                   error_code="TRANSIENT_TIMEOUT"))

    return events


# ---------------------------------------------------------
# SCENARIO 1 — Deployment-Induced Incident (real-time)
# ---------------------------------------------------------

def scenario_deployment_incident_rt() -> List[List[Dict]]:
    """
    Returns a list of event batches to be written with delays
    between them, simulating a deployment incident unfolding
    over several minutes using real-time timestamps.

    Each inner list is one file write. The caller sleeps
    SCENARIO_EVENT_DELAY seconds between writes.
    """
    batches = []
    app, host = "payments-api", random.choice(HOSTS)
    version = f"v{random.randint(2,5)}.{random.randint(0,9)}.{random.randint(0,9)}"
    t = now()

    # Batch 1: Deployment event
    batches.append([
        make_deployment(t, app, host, version, "rolling_update"),
        make_log(t + timedelta(seconds=2), app, host,
                 "INFO", f"Deployment {version} started"),
    ])

    # Batch 2: Normal metrics post-deploy
    t2 = now() + timedelta(seconds=5)
    batches.append([
        make_metric(t2, app, host, "cpu_percent", random.uniform(25, 40)),
        make_metric(t2, app, host, "error_rate", random.uniform(0.005, 0.015)),
        make_log(t2, app, host, "INFO", "Request handled normally"),
    ])

    # Batch 3: Error rate starts climbing
    t3 = now() + timedelta(seconds=10)
    batches.append([
        make_metric(t3, app, host, "error_rate", random.uniform(0.05, 0.10)),
        make_metric(t3, app, host, "cpu_percent", random.uniform(55, 70)),
        make_log(t3, app, host, "WARN", "Elevated error rate detected"),
    ])

    # Batch 4: CPU spike + first alert
    t4 = now() + timedelta(seconds=15)
    cpu_val = random.uniform(85, 95)
    batches.append([
        make_metric(t4, app, host, "cpu_percent", cpu_val),
        make_metric(t4, app, host, "error_rate", random.uniform(0.12, 0.20)),
        make_alert(t4, app, host, "cpu_high", "CRITICAL", 80.0, cpu_val),
    ])

    # Batches 5-9: Repeated alerts + error logs (one per minute)
    for i in range(5):
        ti = now() + timedelta(seconds=20 + i * 5)
        cpu = random.uniform(85, 95)
        error_msgs = [
            ("TimeoutException", "TIMEOUT_CONN"),
            ("DBConnectionError: pool exhausted", "DB_CONN_POOL"),
            ("TimeoutException on /api/pay", "TIMEOUT_CONN"),
            ("DBConnectionError: max retries exceeded", "DB_CONN_POOL"),
            ("Unhandled exception in payment processor", "UNHANDLED"),
        ]
        msg, code = random.choice(error_msgs)
        batch = [
            make_alert(ti, app, host, "cpu_high", "CRITICAL", 80.0, cpu),
            make_metric(ti, app, host, "cpu_percent", cpu),
            make_log(ti, app, host, "ERROR", msg, error_code=code),
        ]
        batches.append(batch)

    # Batch 10: Error rate alert
    t10 = now() + timedelta(seconds=50)
    batches.append([
        make_alert(t10, app, host, "error_rate_high", "HIGH", 0.05,
                   random.uniform(0.15, 0.22)),
    ])

    return batches


# ---------------------------------------------------------
# SCENARIO 2 — Memory Leak (real-time)
# ---------------------------------------------------------

def scenario_memory_leak_rt() -> List[List[Dict]]:
    """Memory gradually climbs, alerts fire, no deployment."""
    batches = []
    app, host = "orders-api", random.choice(HOSTS)

    # Batches 1-6: Gradual memory climb
    for i in range(6):
        ti = now() + timedelta(seconds=i * 5)
        mem = 55.0 + i * 6.5 + random.uniform(-1, 1)
        batch = [
            make_metric(ti, app, host, "memory_percent", mem),
            make_metric(ti, app, host, "cpu_percent",
                        random.uniform(20, 40)),
        ]
        if i % 2 == 0:
            batch.append(make_log(ti, app, host, "INFO",
                                  f"Order queue depth: {random.randint(10, 200)}"))
        batches.append(batch)

    # Batch 7: First memory alert
    t7 = now() + timedelta(seconds=32)
    batches.append([
        make_metric(t7, app, host, "memory_percent",
                    random.uniform(87, 91)),
        make_alert(t7, app, host, "memory_high", "HIGH", 85.0,
                   random.uniform(87, 91)),
        make_log(t7, app, host, "WARN",
                 f"GC pause {random.randint(200, 800)}ms"),
    ])

    # Batches 8-12: Repeated memory alerts + degradation
    for i in range(5):
        ti = now() + timedelta(seconds=37 + i * 5)
        mem_val = 88.0 + random.uniform(0, 5)
        batches.append([
            make_alert(ti, app, host, "memory_high", "HIGH", 85.0, mem_val),
            make_metric(ti, app, host, "memory_percent", mem_val),
            make_log(ti, app, host, "WARN",
                     "Heap usage above safe threshold",
                     error_code="HEAP_PRESSURE"),
        ])

    return batches


# ---------------------------------------------------------
# SCENARIO 3 — Infra Restart (real-time)
# ---------------------------------------------------------

def scenario_infra_restart_rt() -> List[List[Dict]]:
    """Brief restart, CPU spike, single alert, self-heals."""
    batches = []
    app, host = "auth-service", random.choice(HOSTS)
    t = now()

    # Batch 1: Restart
    batches.append([
        make_log(t, app, host, "WARN",
                 "Host restarting — scheduled maintenance",
                 error_code="HOST_RESTART"),
        make_log(t + timedelta(seconds=2), app, host,
                 "INFO", "Host boot sequence initiated"),
    ])

    # Batch 2: CPU spike + alert
    t2 = now() + timedelta(seconds=8)
    cpu = random.uniform(78, 88)
    batches.append([
        make_metric(t2, app, host, "cpu_percent", cpu),
        make_alert(t2, app, host, "cpu_high", "MEDIUM", 80.0, cpu),
    ])

    # Batches 3-5: Normalization
    for i in range(3):
        ti = now() + timedelta(seconds=15 + i * 5)
        batches.append([
            make_metric(ti, app, host, "cpu_percent",
                        random.uniform(15, 35)),
            make_log(ti, app, host, "INFO",
                     "Service health check passed"),
        ])

    return batches


# ---------------------------------------------------------
# FILE WRITER
# ---------------------------------------------------------

def write_batch_to_volume(events: List[Dict],
                          output_path: str,
                          label: str = "") -> str:
    """Write a list of events as a single JSONL file to the Volume."""
    os.makedirs(output_path, exist_ok=True)
    filename = f"events_{make_id()}.jsonl"
    filepath = f"{output_path}/{filename}"

    with open(filepath, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")

    types = {}
    for e in events:
        types[e["event_type"]] = types.get(e["event_type"], 0) + 1
    type_str = ", ".join(f"{k}:{v}" for k, v in sorted(types.items()))

    prefix = f"[{label}] " if label else ""
    print(f"  {prefix}{len(events)} events [{type_str}] -> {filename}")

    return filepath


# ---------------------------------------------------------
# CONTINUOUS STREAM — Runs forever
# ---------------------------------------------------------

SCENARIOS = [
    ("Deployment Incident", scenario_deployment_incident_rt),
    ("Memory Leak", scenario_memory_leak_rt),
    ("Infra Restart", scenario_infra_restart_rt),
]


def run_continuous_stream(output_path: str = None):
    """
    Infinite streaming loop. Generates noise events every
    EMIT_INTERVAL seconds, and injects a full incident scenario
    every SCENARIO_INTERVAL seconds.

    Runs forever until interrupted.

    Timeline:
      T+0s     : noise batch
      T+5s     : noise batch
      T+10s    : noise batch
      ...
      T+120s   : SCENARIO 1 injected (deployment incident)
      T+125s   : noise resumes
      ...
      T+240s   : SCENARIO 2 injected (memory leak)
      ...
      T+360s   : SCENARIO 3 injected (infra restart)
      T+480s   : SCENARIO 1 again (cycles repeat)
    """
    if output_path is None:
        output_path = RAW_EVENTS_PATH

    os.makedirs(output_path, exist_ok=True)

    scenario_idx = 0
    total_events = 0
    total_files = 0
    last_scenario_time = time.time()
    start_time = time.time()

    print("=" * 60)
    print("CONTINUOUS STREAM STARTED")
    print(f"  Landing zone:       {output_path}")
    print(f"  Noise interval:     every {EMIT_INTERVAL}s")
    print(f"  Scenario interval:  every {SCENARIO_INTERVAL}s")
    print(f"  Scenarios rotate:   {' -> '.join(s[0] for s in SCENARIOS)}")
    print(f"  Stop with:          Cancel cell / Ctrl+C")
    print("=" * 60)

    try:
        cycle = 0
        while True:
            cycle += 1
            elapsed = int(time.time() - start_time)

            # --- Check if it's time to inject a scenario ---
            if time.time() - last_scenario_time >= SCENARIO_INTERVAL:
                name, scenario_fn = SCENARIOS[scenario_idx % len(SCENARIOS)]
                scenario_idx += 1
                last_scenario_time = time.time()

                print(f"\n>>> INJECTING SCENARIO: {name} "
                      f"(cycle {scenario_idx}, T+{elapsed}s)")

                batches = scenario_fn()
                for batch in batches:
                    write_batch_to_volume(batch, output_path,
                                          label=name)
                    total_events += len(batch)
                    total_files += 1
                    time.sleep(SCENARIO_EVENT_DELAY)

                print(f">>> SCENARIO COMPLETE: {name}\n")

            # --- Emit noise batch ---
            noise_count = random.randint(3, 8)
            noise = generate_noise_batch(noise_count)
            write_batch_to_volume(noise, output_path, label="noise")
            total_events += len(noise)
            total_files += 1

            # Periodic status line
            if cycle % 10 == 0:
                print(f"  [status] T+{elapsed}s | "
                      f"{total_events:,} events | "
                      f"{total_files} files | "
                      f"next scenario in "
                      f"{int(SCENARIO_INTERVAL - (time.time() - last_scenario_time))}s")

            time.sleep(EMIT_INTERVAL)

    except KeyboardInterrupt:
        elapsed = int(time.time() - start_time)
        print(f"\n{'=' * 60}")
        print(f"STREAM STOPPED (ran for {elapsed}s)")
        print(f"  Total events:  {total_events:,}")
        print(f"  Total files:   {total_files}")
        print(f"  Scenarios run: {scenario_idx}")
        print(f"{'=' * 60}")


# COMMAND ----------

# =========================================================
# DATABRICKS ENTRY POINT
# =========================================================
# This cell starts the continuous stream. It runs FOREVER
# until you cancel it.
#
# IMPORTANT: Start the Bronze/Silver/Gold streams FIRST
# (notebooks 02, 03, 04), then run this cell.
#
# To stop: click "Cancel" on this cell.

run_continuous_stream()

# COMMAND ----------
