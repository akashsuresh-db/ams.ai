-- =========================================================
-- 05_llm_summarization.sql — LLM Summarization (Batch Layer)
-- =========================================================
--
-- PURPOSE:
--   Uses Databricks SQL ai_summarize() to generate human-
--   readable incident summaries, root causes, and confidence
--   scores for Gold incidents that don't yet have a summary.
--
-- DESIGN DECISIONS:
--
--   WHY ai_summarize() RUNS AS A BATCH JOB, NOT IN STREAMING:
--
--     1. COST CONTROL: Each ai_summarize() call costs tokens.
--        In a streaming DAG, every micro-batch would fire LLM
--        calls — potentially thousands per hour.  Batch mode
--        lets us process only unsummarized rows on a schedule
--        (e.g., every 5 minutes via a Databricks Job).
--
--     2. LATENCY ISOLATION: LLM round-trips take 2–10 seconds.
--        If placed inside the streaming DAG, they would block
--        the correlation pipeline, increasing end-to-end latency
--        for ALL downstream consumers (dashboards, PagerDuty).
--
--     3. RETRY SAFETY: LLM calls can fail (rate limits, timeouts).
--        In streaming, a failed micro-batch triggers a full
--        retry of ALL records in that batch.  In batch mode,
--        we simply retry the UPDATE on the next scheduled run —
--        the WHERE summary IS NULL clause is naturally idempotent.
--
--     4. DETERMINISTIC vs. STOCHASTIC SEPARATION: The Bronze →
--        Silver → Gold pipeline is deterministic: same inputs
--        always produce the same outputs.  LLM summarization is
--        inherently stochastic (different runs may produce
--        different summaries).  Keeping them separate means the
--        deterministic pipeline can be validated and tested
--        independently.
--
--   WHY THE CONTEXT BUNDLE IS PRE-BUILT:
--     The incident_context_text was assembled by the Gold
--     correlation engine using Spark (deterministic, fast).
--     We pass it directly to ai_summarize() rather than
--     having the LLM re-query raw data.  This ensures:
--       - The LLM sees exactly what the engineer sees
--       - No risk of the LLM hallucinating data it wasn't given
--       - The prompt is concise and token-efficient
--
-- SCHEDULING:
--   Run this as a Databricks SQL Task in a Workflow, triggered:
--     - Every 5 minutes (near-real-time)
--     - Or after the Gold streaming query completes a batch
-- =========================================================

-- ---------------------------------------------------------
-- STEP 1: SUMMARIZE UNSUMMARIZED INCIDENTS
-- ---------------------------------------------------------
-- ai_summarize() is a Databricks built-in SQL function that
-- calls the workspace's configured foundation model.
--
-- The prompt instructs the model to:
--   1. Summarize in 5 lines
--   2. Identify the most likely root cause
--   3. Provide a confidence score (0–1)
--
-- WHERE summary IS NULL ensures idempotency: rows that
-- already have a summary are skipped on re-runs.

UPDATE alert_ops.pipeline.gold_incidents
SET summary = ai_summarize(
    CONCAT(
        'Summarize this incident in 5 lines. ',
        'Then provide on separate labeled lines: ',
        '1. Root Cause: <most likely root cause>. ',
        '2. Confidence: <score between 0 and 1>. ',
        incident_context_text
    )
)
WHERE summary IS NULL;


-- ---------------------------------------------------------
-- STEP 2: PARSE ROOT CAUSE FROM SUMMARY
-- ---------------------------------------------------------
-- The LLM response is expected to contain lines like:
--   Root Cause: <text>
--   Confidence: <number>
--
-- We use regexp_extract to pull these into separate columns.
-- If parsing fails (LLM didn't follow the format), the
-- columns remain NULL and the full summary is still available.
--
-- This is intentionally lenient: we don't fail the pipeline
-- if the LLM output is unstructured.  The summary field
-- always has the complete response as a fallback.

UPDATE alert_ops.pipeline.gold_incidents
SET
    root_cause = COALESCE(
        regexp_extract(summary, 'Root Cause:\\s*(.+?)(?:\\n|$)', 1),
        root_cause
    ),
    confidence_score = COALESCE(
        CAST(
            regexp_extract(summary, 'Confidence:\\s*([0-9]*\\.?[0-9]+)', 1)
            AS DOUBLE
        ),
        confidence_score
    )
WHERE summary IS NOT NULL
  AND root_cause IS NULL;


-- ---------------------------------------------------------
-- STEP 3: VERIFY RESULTS
-- ---------------------------------------------------------
SELECT
    incident_id,
    alert_type,
    application_id,
    alert_timestamp,
    LEFT(summary, 200)    AS summary_preview,
    root_cause,
    confidence_score,
    prior_alert_count
FROM alert_ops.pipeline.gold_incidents
ORDER BY alert_timestamp DESC;
