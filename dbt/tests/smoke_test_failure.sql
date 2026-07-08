-- Alerting smoke switch: deliberately fails an error-severity test so the full alert path
-- (runner exit-JSON status "tests_failed" -> PL_Orchestration Switch -> alert + Fail activity)
-- can be exercised end-to-end without touching real models. Disabled by default; trigger via
-- the pipeline's dbt_vars parameter: {"smoke_test_fail": "true"}.
{{ config(enabled=(var('smoke_test_fail', 'false') | string | lower == 'true')) }}

select 'deliberate smoke failure' as reason, 42 as sample_value
