-- Alerting smoke switch: deliberately fails a warn-severity test so the non-failing info
-- branch (runner exit-JSON status "tests_warned" -> PL_Orchestration Switch, pipeline stays
-- green) can be exercised. Disabled by default; trigger via dbt_vars: {"smoke_test_warn": "true"}.
{{ config(enabled=(var('smoke_test_warn', 'false') | string | lower == 'true'), severity='warn') }}

select 'deliberate smoke warning' as reason
