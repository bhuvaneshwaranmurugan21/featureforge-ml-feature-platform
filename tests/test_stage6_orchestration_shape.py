"""Offline API-shape and data-flow checks, not managed-execution evidence.

Read parameter paths from the actual Terraform source and apply them to API-shaped
responses. Botocore validates both Glue request and response shapes without any
client, credentials, network access, or Terraform invocation.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
from botocore.session import get_session
from botocore.validate import validate_parameters

ROOT = Path(__file__).resolve().parents[1]
COMPUTE = ROOT / "infra/terraform/compute.tf"


def _state(name: str) -> str:
    text = COMPUTE.read_text()
    match = re.search(rf"^      {re.escape(name)} = \{{", text, re.MULTILINE)
    assert match, f"missing state {name}"
    start = match.end() - 1
    depth = 0
    quoted = False
    escaped = False
    for index in range(start, len(text)):
        character = text[index]
        if quoted:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                quoted = False
        elif character == '"':
            quoted = True
        elif character == "{":
            depth += 1
        elif character == "}":
            depth -= 1
            if depth == 0:
                return text[start : index + 1]
    raise AssertionError(f"unbalanced state {name}")


def _expression(block: str, key: str) -> str:
    match = re.search(rf'"?{re.escape(key)}"?\s*=\s*"([^"\n]+)"', block)
    assert match, f"missing string expression {key}"
    return match.group(1)


def _resolve(document: dict[str, Any], path: str) -> Any:
    assert path.startswith("$.")
    value: Any = document
    for token in re.findall(r"([a-zA-Z_][a-zA-Z_0-9]*)|\[(\d+)\]", path[2:]):
        field, index = token
        value = value[field] if field else value[int(index)]
    return value


def _glue_response(state: str = "SUCCEEDED", run_id: str = "jr-1") -> dict[str, Any]:
    return {
        "glue_result": {"JobName": "featureforge-stage6-offline", "JobRunId": "jr-1"},
        "glue_completion": {
            "JobRun": {
                "Id": run_id,
                "JobName": "featureforge-stage6-offline",
                "JobRunState": state,
            }
        },
    }


def _glue_choice_accepts(document: dict[str, Any]) -> bool:
    block = _state("GlueCompletionDecision")
    predicates = re.findall(
        r'Variable\s*=\s*"([^"\n]+)"\s*'
        r'(StringEqualsPath|StringEquals)\s*=\s*"([^"\n]+)"',
        block,
    )
    assert len(predicates) == 3 and "And = [" in block
    return all(
        _resolve(document, left)
        == (_resolve(document, right) if operator == "StringEqualsPath" else right)
        for left, operator, right in predicates
    )


def test_glue_start_and_get_job_run_use_declared_api_shapes() -> None:
    glue = get_session().get_service_model("glue")
    start = glue.operation_model("StartJobRun")
    assert start.output_shape is not None
    assert set(start.output_shape.members) == {"JobRunId"}
    validate_parameters({"JobRunId": "jr-1"}, start.output_shape)
    assert "$.glue_result.JobRun." not in COMPUTE.read_text()

    state = _state("VerifyGlueCompletion")
    assert "states:::aws-sdk:glue:getJobRun" in state
    assert _expression(_state("BuildOfflineGeneration"), "Next") == "VerifyGlueCompletion"
    get = glue.operation_model("GetJobRun")
    assert get.input_shape is not None and get.output_shape is not None
    request = {
        "JobName": "featureforge-stage6-offline",
        "RunId": _resolve(_glue_response(), _expression(state, "RunId.$")),
        "PredecessorsIncluded": False,
    }
    validate_parameters(request, get.input_shape)
    validate_parameters(_glue_response()["glue_completion"], get.output_shape)
    assert _expression(state, "ResultPath") == "$.glue_completion"
    assert _expression(state, "Next") == "GlueCompletionDecision"
    assert "MaxAttempts     = 2" in state


@pytest.mark.parametrize("state", ["FAILED", "STOPPED", "TIMEOUT", "RUNNING", "ERROR", "WAITING"])
def test_non_successful_glue_metadata_cannot_reach_completion(state: str) -> None:
    assert not _glue_choice_accepts(_glue_response(state))
    assert _expression(_state("GlueCompletionDecision"), "Default") == "Quarantined"


def test_glue_completion_binds_exact_job_and_run() -> None:
    assert _glue_choice_accepts(_glue_response())
    assert not _glue_choice_accepts(_glue_response(run_id="jr-other"))
    wrong_job = _glue_response()
    wrong_job["glue_completion"]["JobRun"]["JobName"] = "another-project"
    assert not _glue_choice_accepts(wrong_job)
    with pytest.raises(KeyError):
        _glue_choice_accepts({"glue_result": _glue_response()["glue_result"]})
    record = _state("RecordGlueCompletion")
    assert _resolve(_glue_response(), _expression(record, "glue_job_run_id.$")) == "jr-1"
    assert _resolve(_glue_response(), _expression(record, "state.$")) == "SUCCEEDED"


def test_parity_choice_reads_worker_receipt_result_not_forged_top_level() -> None:
    path = _expression(_state("ParityDecision"), "Variable")
    assert path == "$.parity_result.Payload.result.decision"
    actual = {"parity_result": {"Payload": {"result": {"decision": "QUARANTINED"}}}}
    actual["parity_result"]["Payload"]["decision"] = "ELIGIBLE"
    assert _resolve(actual, path) == "QUARANTINED"
    with pytest.raises(KeyError):
        _resolve({"parity_result": {"Payload": {"decision": "ELIGIBLE"}}}, path)


def test_online_and_parity_take_identical_glue_provenance_and_context() -> None:
    materialize = _state("MaterializeOnlineCandidate")
    parity = _state("ParityGate")
    expected = {
        "online_payload_authority.$": "$.glue_receipt.Payload.result.online_payload_authority",
        "feature_set.$": "$.materialization_context.feature_set",
        "materialized_at.$": "$.materialization_context.materialized_at",
        "request_time.$": "$.materialization_context.request_time",
        "maximum_freshness_age.$": "$.materialization_context.maximum_freshness_age",
    }
    for key, path in expected.items():
        assert _expression(materialize, key) == _expression(parity, key) == path
    assert "$.online_payload_authority" not in materialize
    assert "$.parity_payload" not in parity
    activate = _state("ActivateGeneration")
    assert _expression(activate, "parity_receipt_digest.$") == (
        "$.parity_result.Payload.result.parity_digest"
    )
    assert "$.activation_payload" not in activate


def test_glue_arguments_cannot_override_manifest_or_managed_runtime() -> None:
    build = _state("BuildOfflineGeneration")
    assert "$.glue_arguments" not in build
    assert _expression(build, "--input-version.$") == "$.manifest.inputs[0].version_id"
    assert _expression(build, "--input-sha256.$") == "$.manifest.inputs[0].sha256"
    assert _expression(build, "--output-prefix.$") == "$.manifest.output_prefix"
    assert _expression(build, "--max-input-rows.$") == (
        "States.Format('{}', $.manifest.max_input_rows)"
    )
    assert _expression(build, "--max-output-rows.$") == (
        "States.Format('{}', $.manifest.max_output_rows)"
    )
    assert '"--kms-key-arn"           = aws_kms_key.platform.arn' in build
    assert '"--expected-bucket-owner" = data.aws_caller_identity.current.account_id' in build
    assert "--additional-python-modules" not in build


def test_completion_binds_actual_receipts_and_versioned_glue_outputs() -> None:
    complete = _state("CompleteRun")
    assert _expression(complete, "admission.$") == "$.validation_result.Payload.result"
    assert _expression(complete, "output_objects.$") == (
        "$.glue_receipt.Payload.result.output_objects"
    )
    assert _expression(complete, "task_receipts.$") == (
        "States.Array($.validation_result.Payload, $.glue_receipt.Payload, "
        "$.online_result.Payload, $.parity_result.Payload, $.activation_result.Payload)"
    )
    assert "$.completion_payload" not in complete
    assert "$.completion_context.admission" not in complete


def test_validation_binds_the_step_functions_execution_before_glue() -> None:
    validate = _state("ValidateManifest")
    assert _expression(validate, "execution_id.$") == "$$.Execution.Id"
    assert _expression(validate, "Next") == "BuildOfflineGeneration"
