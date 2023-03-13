################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Additional tests for orquestra.sdk._base._in_process_runtime. Most of the
functionality is covered inside tests/v2/test_api.py.

We need this file because test_api.py might be too coarse for some scenarios.
When adding new tests, please consider that suite first.
"""
from datetime import datetime, timedelta, timezone
from unittest.mock import create_autospec

import pytest

from orquestra import sdk
from orquestra.sdk import exceptions
from orquestra.sdk._base._in_process_runtime import InProcessRuntime
from orquestra.sdk._base._testing._example_wfs import wf_with_secrets
from orquestra.sdk.schema import ir
from orquestra.sdk.schema.workflow_run import State, WorkflowRunId
from orquestra.sdk.secrets import _client, _models

from .data.complex_serialization.workflow_defs import (
    wf_pass_callables_from_task,
    wf_pass_tuple,
)


@pytest.fixture
def runtime():
    return InProcessRuntime()


@pytest.fixture
def wf_def() -> ir.WorkflowDef:
    return wf_pass_tuple().model


@sdk.task(n_outputs=2)
def two_outputs(a, b):
    return a + b, a - b


@sdk.workflow
def wf_with_unused_outputs():
    out1, _ = two_outputs(5, 1)
    return out1


@sdk.workflow
def wf_all_used():
    out1, out2 = two_outputs(5, 1)
    return out1, out2


@pytest.fixture
def wf_def_unused_outputs() -> ir.WorkflowDef:
    return wf_with_unused_outputs().model


@pytest.fixture
def wf_def_all_used() -> ir.WorkflowDef:
    return wf_all_used().model


def test_secret_inside_ir(
    tmp_default_config_json, monkeypatch: pytest.MonkeyPatch, runtime: InProcessRuntime
):
    mocked_secret = _models.SecretDefinition(name="a-secret", value="mocked")
    get_secret = create_autospec(_client.SecretsClient.get_secret)
    get_secret.return_value = mocked_secret
    monkeypatch.setattr(_client.SecretsClient, "get_secret", get_secret)

    run_id = runtime.create_workflow_run(wf_with_secrets().model)
    result = runtime.get_workflow_run_outputs(run_id)

    assert result == "Mocked"


class TestQueriesAfterRunning:
    """
    Submits a well-known workflow and tests the "query" methods.
    """

    @staticmethod
    @pytest.fixture
    def run_id(runtime: InProcessRuntime, wf_def) -> WorkflowRunId:
        run_id = runtime.create_workflow_run(wf_def)
        return run_id

    class TestGetWorkflowRunOutputs:
        @staticmethod
        def test_output_values(runtime, run_id):
            assert runtime.get_workflow_run_outputs(run_id) == 3

        @staticmethod
        def test_matches_non_blocking(runtime, run_id):
            outputs = runtime.get_workflow_run_outputs(run_id)

            assert outputs == runtime.get_workflow_run_outputs_non_blocking(run_id)

        @staticmethod
        def test_multiple_runs(runtime):
            wf_def1 = wf_pass_tuple().model
            wf_def2 = wf_pass_callables_from_task().model

            run_id1 = runtime.create_workflow_run(wf_def1)
            run_id2 = runtime.create_workflow_run(wf_def2)

            outputs1 = runtime.get_workflow_run_outputs(run_id1)
            outputs2 = runtime.get_workflow_run_outputs(run_id2)

            assert run_id1 != run_id2
            assert outputs1 != outputs2

    class TestGetAvailableOutputs:
        @staticmethod
        def test_single_task_output(runtime, run_id):
            """
            The 'run_id' fixture is a result of submitting a workflow def with a
            single-output task.
            """
            assert runtime.get_available_outputs(run_id) == {
                "invocation-0-task-sum-tuple-numbers": (3,)
            }

        class TestMultipleTaskOutputs:
            @staticmethod
            def test_some_unused(runtime, wf_def_unused_outputs):
                run_id = runtime.create_workflow_run(wf_def_unused_outputs)

                assert runtime.get_available_outputs(run_id) == {
                    "invocation-0-task-two-outputs": (6, 4)
                }

            @staticmethod
            def test_all_used(runtime, wf_def_all_used):
                run_id = runtime.create_workflow_run(wf_def_all_used)

                assert runtime.get_available_outputs(run_id) == {
                    "invocation-0-task-two-outputs": (6, 4)
                }


class TestStop:
    @staticmethod
    def test_existing_run(runtime, wf_def):
        # Given
        run_id = runtime.create_workflow_run(wf_def)

        # When
        runtime.stop_workflow_run(run_id)

        # Then
        # Not much. We expect the above to be a noop.

    @staticmethod
    def test_non_existing_run(runtime):
        # Given
        run_id = "doesn't exist"

        # Then
        with pytest.raises(exceptions.WorkflowRunNotFoundError):
            # When
            runtime.stop_workflow_run(run_id)


class TestListWorkflowRuns:
    @staticmethod
    def test_happy_path(runtime, wf_def):
        # Given
        _ = runtime.create_workflow_run(wf_def)
        _ = runtime.create_workflow_run(wf_def)

        # When
        wf_runs = runtime.list_workflow_runs()

        # Then
        assert len(wf_runs) == 2

    @staticmethod
    def test_limit(runtime, wf_def):
        # Given
        _ = runtime.create_workflow_run(wf_def)
        _ = runtime.create_workflow_run(wf_def)

        # When
        wf_runs = runtime.list_workflow_runs(limit=1)

        # Then
        assert len(wf_runs) == 1

    @staticmethod
    def test_state_running(runtime, wf_def):
        # Given
        _ = runtime.create_workflow_run(wf_def)

        # When
        wf_runs = runtime.list_workflow_runs(state=State.RUNNING)

        # Then
        # It doesn't make sense to get a running workflow from this runtime
        assert len(wf_runs) == 0

    @staticmethod
    def test_state_succeeded(runtime, wf_def):
        # Given
        _ = runtime.create_workflow_run(wf_def)

        # When
        wf_runs = runtime.list_workflow_runs(state=State.SUCCEEDED)

        # Then
        # It doesn't make sense to get a running workflow from this runtime
        assert len(wf_runs) == 1

    @staticmethod
    def test_max_age(runtime, wf_def):
        # Given
        run_id = runtime.create_workflow_run(wf_def)
        _ = runtime.create_workflow_run(wf_def)
        runtime._start_time_store[run_id] = datetime.now(timezone.utc) - timedelta(
            days=1
        )

        # When
        wf_runs = runtime.list_workflow_runs(max_age=timedelta(minutes=1))

        # Then
        # It doesn't make sense to get a running workflow from this runtime
        assert len(wf_runs) == 1


class TestUnsupportedMethods:
    @staticmethod
    @pytest.mark.parametrize(
        "method",
        [
            InProcessRuntime.from_runtime_configuration,
            InProcessRuntime.get_all_workflow_runs_status,
        ],
    )
    def test_raises(runtime, method):
        with pytest.raises(NotImplementedError):
            method(runtime)
