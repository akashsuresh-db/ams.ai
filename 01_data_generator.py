# Databricks notebook source
# =========================================================
# 01_data_generator.py — Synthetic Streaming Data Generator
# =========================================================
#
# PURPOSE:
#   Generates 400–600 realistic, interleaved JSONL events
#   that simulate three distinct operational scenarios plus
#   background noise. The output is written to a landing
#   zone for Auto Loader to pick up.
#
# SCENARIOS:
#   1. Deployment-Induced Incident  (payments-api, host-a)
#   2. Memory Leak with no deploy   (orders-api, host-b)
#   3. Infra Restart / noise         (auth-service, host-a)
#
# DESIGN DECISIONS:
#   - Fingerprints are deterministic hashes so downstream
#     deduplication is reproducible.
#   - Timestamps are spaced 30–60 s apart globally but
#     scenario-internal timing follows the spec (e.g.,
#     repeated alerts every 60 s for 5 minutes).
#   - Events are shuffled by timestamp at the end to
#     simulate a realistic interleaved stream.
# =========================================================

import json
import uuid
import hashlib
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any

# ---------------------------------------------------------
# COMMAND ----------

# %run ./config          # Uncomment when running in Databricks

# For standalone execution, inline the needed config values:
RAW_EVENTS_PATH = "/mnt/alert_pipeline/raw_events"
APPLICATIONS = ["payments-api", "orders-api", "auth-service"]
HOSTS = ["host-a", "host-b"]

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


def ts_iso(dt: datetime) -> str:
    """ISO-8601 timestamp string."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def jitter(seconds_low: int = 30, seconds_high: int = 60) -> timedelta:
    """Random inter-event gap."""
    return timedelta(seconds=random.randint(seconds_low, seconds_high))


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
# NOISE GENERATOR
# ---------------------------------------------------------

def generate_noise(base_time: datetime, count: int) -> List[Dict]:
    """
    Background noise: normal metrics, INFO logs, occasional
    low-severity alerts that self-resolve.  Spread across
    all applications and hosts.
    """
    events = []
    t = base_time

    for _ in range(count):
        app = random.choice(APPLICATIONS)
        host = random.choice(HOSTS)
        t += jitter(30, 60)

        roll = random.random()
        if roll < 0.45:
            # Normal metric
            name = random.choice(["cpu_percent", "memory_percent",
                                   "error_rate", "request_latency_ms"])
            val = {
                "cpu_percent": random.uniform(10, 45),
                "memory_percent": random.uniform(30, 60),
                "error_rate": random.uniform(0.001, 0.02),
                "request_latency_ms": random.uniform(50, 200),
            }[name]
            events.append(make_metric(t, app, host, name, val))

        elif roll < 0.85:
            # INFO / DEBUG log
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
            events.append(make_log(t, app, host, level,
                                   random.choice(msgs)))
        else:
            # Transient warning log
            events.append(make_log(t, app, host, "WARN",
                                   "Transient timeout on downstream call",
                                   error_code="TRANSIENT_TIMEOUT"))

    return events


# ---------------------------------------------------------
# SCENARIO 1 — Deployment-Induced Incident
# ---------------------------------------------------------

def scenario_deployment_incident(base_time: datetime) -> List[Dict]:
    """
    Timeline:
      T+0        : deployment of v2.4.1 on payments-api / host-a
      T+25 min   : error_rate starts climbing
      T+30 min   : CPU spike, first alert fires
      T+31–35 min: repeated CPU alerts every 60 s (same fingerprint)
      T+30–35 min: error logs (TimeoutException, DBConnectionError)
    """
    events = []
    app, host = "payments-api", "host-a"
    t0 = base_time

    # --- Deployment ---
    events.append(make_deployment(t0, app, host, "v2.4.1",
                                  "rolling_update"))
    events.append(make_log(t0 + timedelta(seconds=5), app, host,
                           "INFO", "Deployment v2.4.1 started"))

    # --- Pre-incident normal metrics (T+1 to T+20 min) ---
    for m in range(1, 21):
        t = t0 + timedelta(minutes=m)
        events.append(make_metric(t, app, host, "cpu_percent",
                                  random.uniform(25, 40)))
        events.append(make_metric(t, app, host, "error_rate",
                                  random.uniform(0.005, 0.015)))
        if m % 3 == 0:
            events.append(make_log(t, app, host, "INFO",
                                   "Request handled normally"))

    # --- Error rate climb (T+21 to T+29 min) ---
    for m in range(21, 30):
        t = t0 + timedelta(minutes=m)
        rate = 0.02 + (m - 20) * 0.012
        events.append(make_metric(t, app, host, "error_rate",
                                  rate))
        events.append(make_metric(t, app, host, "cpu_percent",
                                  random.uniform(50, 70)))

    # --- Incident peak (T+30 min) ---
    t_peak = t0 + timedelta(minutes=30)
    events.append(make_metric(t_peak, app, host, "cpu_percent", 92.5))
    events.append(make_metric(t_peak, app, host, "error_rate", 0.18))
    events.append(make_alert(t_peak, app, host,
                             "cpu_high", "CRITICAL", 80.0, 92.5))

    # --- Repeated CPU alerts every 60 s for 5 minutes ---
    for i in range(1, 6):
        t_rep = t_peak + timedelta(minutes=i)
        cpu_val = random.uniform(85, 95)
        events.append(make_alert(t_rep, app, host,
                                 "cpu_high", "CRITICAL", 80.0, cpu_val))
        events.append(make_metric(t_rep, app, host,
                                  "cpu_percent", cpu_val))

    # --- Error logs during incident ---
    error_messages = [
        ("TimeoutException", "TIMEOUT_CONN"),
        ("DBConnectionError: pool exhausted", "DB_CONN_POOL"),
        ("TimeoutException on /api/pay", "TIMEOUT_CONN"),
        ("DBConnectionError: max retries exceeded", "DB_CONN_POOL"),
        ("TimeoutException on /api/refund", "TIMEOUT_CONN"),
        ("Unhandled exception in payment processor", "UNHANDLED"),
        ("DBConnectionError: socket closed", "DB_CONN_POOL"),
        ("TimeoutException on /api/status", "TIMEOUT_CONN"),
    ]
    for i, (msg, code) in enumerate(error_messages):
        t_err = t_peak + timedelta(seconds=30 * i)
        events.append(make_log(t_err, app, host, "ERROR", msg,
                               error_code=code))

    # --- Error rate alert ---
    events.append(make_alert(t_peak + timedelta(minutes=2), app, host,
                             "error_rate_high", "HIGH", 0.05, 0.18))

    return events


# ---------------------------------------------------------
# SCENARIO 2 — Memory Leak (No Deployment)
# ---------------------------------------------------------

def scenario_memory_leak(base_time: datetime) -> List[Dict]:
    """
    Timeline:
      T+0 to T+40 min : memory climbs from 55% to 90%+
      T+35 min         : first memory alert
      T+36–40 min      : repeated memory alerts (same fingerprint)
      No deployment event anywhere in this scenario.
    """
    events = []
    app, host = "orders-api", "host-b"
    t0 = base_time

    # --- Gradual memory increase ---
    for m in range(0, 41):
        t = t0 + timedelta(minutes=m)
        # Linear ramp from 55% to ~92%
        mem = 55.0 + (m / 40.0) * 37.0
        events.append(make_metric(t, app, host, "memory_percent",
                                  mem + random.uniform(-1, 1)))
        # CPU stays normal
        events.append(make_metric(t, app, host, "cpu_percent",
                                  random.uniform(20, 40)))
        # Periodic normal logs
        if m % 5 == 0:
            events.append(make_log(t, app, host, "INFO",
                                   "Order queue depth: "
                                   f"{random.randint(10, 200)}"))

    # --- First memory alert at T+35 ---
    t_alert = t0 + timedelta(minutes=35)
    events.append(make_alert(t_alert, app, host,
                             "memory_high", "HIGH", 85.0, 88.7))

    # --- Repeated memory alerts every 60 s for 5 minutes ---
    for i in range(1, 6):
        t_rep = t_alert + timedelta(minutes=i)
        mem_val = 88.0 + random.uniform(0, 4)
        events.append(make_alert(t_rep, app, host,
                                 "memory_high", "HIGH", 85.0, mem_val))
        events.append(make_log(t_rep, app, host, "WARN",
                               f"GC pause {random.randint(200, 800)}ms"))

    # --- Slow degradation logs ---
    for i in range(0, 8):
        t_log = t_alert + timedelta(seconds=45 * i)
        events.append(make_log(t_log, app, host, "WARN",
                               "Heap usage above safe threshold",
                               error_code="HEAP_PRESSURE"))

    return events


# ---------------------------------------------------------
# SCENARIO 3 — Infra Restart (Noise / Self-Healing)
# ---------------------------------------------------------

def scenario_infra_restart(base_time: datetime) -> List[Dict]:
    """
    Timeline:
      T+0       : host restart log
      T+1–2 min : brief CPU spike
      T+2 min   : single CPU alert
      T+3–5 min : metrics normalize
      No sustained errors.
    """
    events = []
    app, host = "auth-service", "host-a"
    t0 = base_time

    # --- Restart ---
    events.append(make_log(t0, app, host, "WARN",
                           "Host restarting — scheduled maintenance",
                           error_code="HOST_RESTART"))
    events.append(make_log(t0 + timedelta(seconds=15), app, host,
                           "INFO", "Host boot sequence initiated"))

    # --- Brief CPU spike ---
    for s in [30, 60, 90, 120]:
        t = t0 + timedelta(seconds=s)
        cpu = random.uniform(70, 88)
        events.append(make_metric(t, app, host, "cpu_percent", cpu))

    # --- Single alert at T+2 min ---
    events.append(make_alert(t0 + timedelta(minutes=2), app, host,
                             "cpu_high", "MEDIUM", 80.0, 85.3))

    # --- Normalization ---
    for m in range(3, 8):
        t = t0 + timedelta(minutes=m)
        events.append(make_metric(t, app, host, "cpu_percent",
                                  random.uniform(15, 35)))
        events.append(make_log(t, app, host, "INFO",
                               "Service health check passed"))

    return events


# ---------------------------------------------------------
# MAIN — Assemble, Interleave, Write
# ---------------------------------------------------------

def generate_all_events(output_path: str = None):
    """
    Assemble events from all three scenarios plus noise,
    sort by timestamp to interleave, and write JSONL.
    """
    random.seed(42)  # Reproducibility

    # Base time: a fixed reference point
    base = datetime(2026, 2, 14, 6, 0, 0)

    # Scenario timelines (staggered starts)
    events = []
    events += scenario_deployment_incident(base)
    events += scenario_memory_leak(base + timedelta(minutes=10))
    events += scenario_infra_restart(base + timedelta(minutes=50))

    # Noise events spread across the full 90-minute window
    noise_count = random.randint(200, 300)
    events += generate_noise(base, noise_count)

    # Sort all events by timestamp to produce an interleaved stream
    events.sort(key=lambda e: e["timestamp"])

    total = len(events)
    print(f"Generated {total} events "
          f"(target: 400–600, {'OK' if 400 <= total <= 600 else 'ADJUST NOISE'})")

    # ----- Write output -----
    if output_path is None:
        output_path = RAW_EVENTS_PATH

    # In Databricks, use dbutils.fs.put or write to a volume.
    # For local/testing, write to local filesystem.
    local_file = "/tmp/alert_events.jsonl"
    with open(local_file, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")

    print(f"Wrote {total} events to {local_file}")
    print(f"Copy to DBFS with: dbutils.fs.cp('file:{local_file}', "
          f"'{output_path}/events_{make_id()}.jsonl')")

    return events, local_file


# ---------------------------------------------------------
# COMMAND ----------

# DATABRICKS ENTRY POINT
# Uncomment the following block when running as a Databricks notebook:

# events, local_path = generate_all_events()
#
# # Copy to the Auto Loader landing zone
# import os
# batch_id = os.path.basename(local_path).replace(".jsonl", "")
# target = f"{RAW_EVENTS_PATH}/events_{batch_id}.jsonl"
# dbutils.fs.cp(f"file:{local_path}", target)
# print(f"Uploaded to {target}")

# ---------------------------------------------------------
# For local testing:
if __name__ == "__main__":
    events, path = generate_all_events()
    # Print a sample
    for e in events[:5]:
        print(json.dumps(e, indent=2))
    print(f"... ({len(events)} total events)")
