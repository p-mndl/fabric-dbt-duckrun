-- Alerting smoke switch: deliberately errors at runtime so the escalation path (runner
-- exit-JSON status "models_failed" -> PL_Orchestration Switch -> escalated alert + Fail
-- activity) and dbt build's downstream skipping can be exercised. Disabled by default;
-- trigger via the pipeline's dbt_vars parameter: {"smoke_model_error": "true"}.
{{ config(enabled=(var('smoke_model_error', 'false') | string | lower == 'true')) }}

select cast('boom' as integer) as will_fail_at_runtime
