-- =========================================================
-- 05_llm_summarization.sql — LLM Analysis (Batch Layer)
-- =========================================================
--
-- PURPOSE:
--   Uses Databricks SQL ai_query() to analyze Gold incidents
--   and produce structured output: a summary, detected patterns,
--   probable root cause, and confidence score.
--
-- FUNCTION USED:
--   ai_query(endpoint, prompt, responseFormat => ...)
--   Reference: https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/ai_query
--
--   ai_query() calls a foundation model endpoint and returns
--   structured JSON when responseFormat is specified. This lets
--   us parse summary, patterns, root_cause, and confidence_score
--   directly into typed columns — no regex parsing needed.
--
-- DESIGN DECISIONS:
--
--   WHY ai_query() INSTEAD OF ai_summarize():
--     - ai_query() supports responseFormat for structured JSON output,
--       eliminating brittle regex parsing of free-text responses.
--     - ai_query() lets us choose the model endpoint explicitly.
--     - ai_query() supports modelParameters (temperature, max_tokens)
--       for reproducibility and cost control.
--     - We can request specific fields (patterns, root_cause, confidence)
--       that ai_summarize() cannot produce in a structured way.
--
--   WHY ai_query() RUNS AS A BATCH JOB, NOT IN STREAMING:
--
--     1. COST CONTROL: Each ai_query() call costs tokens.
--        In a streaming DAG, every micro-batch would fire LLM
--        calls — potentially thousands per hour.  Batch mode
--        lets us process only un-analyzed rows on a schedule
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
--        always produce the same outputs.  LLM analysis is
--        inherently stochastic (different runs may produce
--        different summaries).  Keeping them separate means the
--        deterministic pipeline can be validated and tested
--        independently.
--
--   WHY THE CONTEXT BUNDLE IS PRE-BUILT:
--     The incident_context_text was assembled by the Gold
--     correlation engine using Spark (deterministic, fast).
--     We pass it directly to ai_query() rather than having
--     the LLM re-query raw data.  This ensures:
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
-- STEP 1: ANALYZE INCIDENTS WITH STRUCTURED JSON OUTPUT
-- ---------------------------------------------------------
-- ai_query() with responseFormat => json_schema forces the
-- LLM to return a JSON object matching our exact schema.
-- This gives us typed fields we can parse directly —
-- no regex, no string splitting, no brittle post-processing.
--
-- We use failOnError => false so that if one row's LLM call
-- fails (rate limit, timeout), it doesn't kill the entire
-- UPDATE. Failed rows keep summary = NULL and get retried
-- on the next scheduled run.
--
-- modelParameters:
--   temperature = 0.1  → near-deterministic output
--   max_tokens  = 1024 → enough for structured response
--
-- NOTE: Replace 'databricks-meta-llama-3-3-70b-instruct'
-- with your workspace's available model endpoint. Check
-- available endpoints in your Model Serving UI.

UPDATE akash_s_demo.ams.gold_incidents
SET summary = ai_query(
    'databricks-meta-llama-3-3-70b-instruct',
    CONCAT(
        'You are an expert SRE analyzing production incidents. ',
        'Analyze the following incident context and provide a structured assessment.\n\n',
        'Instructions:\n',
        '- summary: A concise 3-5 line summary of what happened.\n',
        '- patterns: List the observable patterns (e.g., "CPU spike after deployment", ',
        '  "memory leak without deployment", "cascading timeouts"). Be specific.\n',
        '- root_cause: The single most likely root cause based on the evidence.\n',
        '- confidence_score: Your confidence in the root cause (0.0 to 1.0).\n',
        '- recommended_action: One concrete next step for the on-call engineer.\n\n',
        'Incident Context:\n',
        incident_context_text
    ),
    modelParameters => named_struct(
        'temperature', 0.1,
        'max_tokens', 1024
    ),
    responseFormat => '{
        "type": "json_schema",
        "json_schema": {
            "name": "incident_analysis",
            "schema": {
                "type": "object",
                "properties": {
                    "summary": {
                        "type": "string",
                        "description": "3-5 line incident summary"
                    },
                    "patterns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of detected patterns"
                    },
                    "root_cause": {
                        "type": "string",
                        "description": "Most likely root cause"
                    },
                    "confidence_score": {
                        "type": "number",
                        "description": "Confidence in root cause (0.0 to 1.0)"
                    },
                    "recommended_action": {
                        "type": "string",
                        "description": "Next step for on-call engineer"
                    }
                },
                "required": ["summary", "patterns", "root_cause", "confidence_score", "recommended_action"]
            },
            "strict": true
        }
    }',
    failOnError => false
)
WHERE summary IS NULL;


-- ---------------------------------------------------------
-- STEP 2: PARSE STRUCTURED JSON INTO TYPED COLUMNS
-- ---------------------------------------------------------
-- Since ai_query() with responseFormat returns a JSON string
-- matching our schema, we use JSON path extraction (`:` operator
-- or get_json_object) to populate the individual columns.
--
-- This is much more reliable than regex on free text:
--   - JSON schema is enforced by the LLM endpoint
--   - Parsing is deterministic (same JSON → same fields)
--   - If the JSON is malformed, the columns stay NULL
--     and the raw summary is preserved as fallback

UPDATE akash_s_demo.ams.gold_incidents
SET
    root_cause = COALESCE(
        get_json_object(summary, '$.root_cause'),
        root_cause
    ),
    confidence_score = COALESCE(
        CAST(get_json_object(summary, '$.confidence_score') AS DOUBLE),
        confidence_score
    ),
    patterns = COALESCE(
        get_json_object(summary, '$.patterns'),
        patterns
    ),
    recommended_action = COALESCE(
        get_json_object(summary, '$.recommended_action'),
        recommended_action
    ),
    -- Replace the raw JSON with just the human-readable summary
    summary = COALESCE(
        get_json_object(summary, '$.summary'),
        summary
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
    summary,
    patterns,
    root_cause,
    confidence_score,
    recommended_action,
    prior_alert_count
FROM akash_s_demo.ams.gold_incidents
ORDER BY alert_timestamp DESC;
