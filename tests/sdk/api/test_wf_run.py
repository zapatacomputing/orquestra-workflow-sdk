################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Tests for orquestra.sdk._base._api._wf_run.
"""

import itertools
import time
import typing as t
import warnings
from contextlib import suppress as do_not_raise
from datetime import timedelta
from unittest.mock import (
    DEFAULT,
    MagicMock,
    Mock,
    PropertyMock,
    create_autospec,
    sentinel,
)

import pytest

from orquestra.sdk._base import _api, _dsl, _traversal, _workflow, serde
from orquestra.sdk._base._api import repos
from orquestra.sdk._base._api._task_run import TaskRun
from orquestra.sdk._base._env import CURRENT_PROJECT_ENV, CURRENT_WORKSPACE_ENV
from orquestra.sdk._base._in_process_runtime import InProcessRuntime
from orquestra.sdk._base._logs._interfaces import LogOutput, LogReader, WorkflowLogs
from orquestra.sdk._base._spaces._api import list_projects, list_workspaces
from orquestra.sdk._base._spaces._structs import ProjectRef, Workspace
from orquestra.sdk._base.abc import RuntimeInterface
from orquestra.sdk.exceptions import (
    ConfigFileNotFoundError,
    ConfigNameNotFoundError,
    ProjectInvalidError,
    RuntimeQuerySummaryError,
    UnauthorizedError,
    VersionMismatch,
    WorkflowRunCanNotBeTerminated,
    WorkflowRunNotFinished,
    WorkflowRunNotFoundError,
)
from orquestra.sdk.schema import ir
from orquestra.sdk.schema.configs import RuntimeName
from orquestra.sdk.schema.responses import JSONResult
from orquestra.sdk.schema.workflow_run import RunStatus, State
from orquestra.sdk.schema.workflow_run import TaskRun as TaskRunModel

from ..data.complex_serialization.workflow_defs import (
    capitalize,
    join_strings,
    wf_pass_tuple,
)


def _fake_completed_workflow(end_state: State = State.SUCCEEDED):
    # for simulating a workflow running
    run_model = Mock(name=f"{end_state.value} wf run model")

    # We need the output ids to have a length as we use this to determine how many
    # results we expect. We set this to 2 to avoid the special case where single
    # return values are unpacked.
    run_model.workflow_def.output_ids.__len__ = Mock(return_value=2)

    run_model.message = None
    run_model.status.state = end_state
    return run_model


class TestRunningInProcess:
    """
    Tests the public Python API for running workflows, using the "in-process"
    runtime.

    These tests in combination with those in
    tests/test_api_with_ray.py::TestRunningLocalInBackground exhibit the behaviour
    discussed in https://zapatacomputing.atlassian.net/browse/ORQSDK-485.
    """

    class TestTwoStepForm:
        @staticmethod
        def test_pass_builtin_config_name_no_file():
            run = wf_pass_tuple().run("in_process")
            results = run.get_results()

            assert results == 3

        @staticmethod
        @pytest.mark.usefixtures("patch_config_location")
        def test_pass_builtin_config_name_with_file():
            run = wf_pass_tuple().run("in_process")
            results = run.get_results()

            assert results == 3

        def test_single_run(self):
            run = wf_pass_tuple().run("in_process")
            results = run.get_results()

            assert results == 3

    class TestShorthand:
        def test_single_run(self):
            run = wf_pass_tuple().run("in_process")
            results = run.get_results()

            assert results == 3

    class TestWithConfig:
        @staticmethod
        def test_pass_config():
            config = _api.RuntimeConfig.in_process()
            run = wf_pass_tuple().run(config)
            results = run.get_results()

            assert results == 3


class TestWorkflowRun:
    @staticmethod
    @pytest.fixture
    def sample_wf_def() -> _workflow.WorkflowDef:
        @_workflow.workflow
        def my_wf():
            # We need at least 4 invocations to use in our tests.
            text1 = capitalize(join_strings(["hello", "there"]))
            text2 = capitalize(join_strings(["general", "kenobi"]))
            return text1, text2

        return my_wf()

    @staticmethod
    @pytest.fixture
    def sample_task_inv_ids(sample_wf_def) -> t.List[ir.TaskInvocationId]:
        wf_def_model = sample_wf_def.model
        task_invs = wf_def_model.task_invocations.values()
        return [inv.id for inv in task_invs]

    @staticmethod
    @pytest.fixture
    def mock_runtime(sample_task_inv_ids):
        runtime = create_autospec(RuntimeInterface, name="runtime")
        # For getting workflow ID
        runtime.create_workflow_run.return_value = "wf_pass_tuple-1"
        # For getting workflow outputs
        runtime.get_workflow_run_outputs_non_blocking.return_value = (
            serde.result_from_artifact("woohoo!", ir.ArtifactFormat.AUTO),
        )
        runtime.get_workflow_run_status.return_value = _fake_completed_workflow(
            State.SUCCEEDED
        )
        # Use side effects to simulate a running workflow

        running_wf_run_model = Mock(name="running wf run model")
        running_wf_run_model.status.state = State.RUNNING
        runtime.get_workflow_run_status.side_effect = itertools.chain(
            (
                running_wf_run_model,
                running_wf_run_model,
            ),
            itertools.repeat(DEFAULT),
        )

        # got getting task run artifacts
        runtime.get_available_outputs.return_value = {
            "task_run1": serde.result_from_artifact("woohoo!", ir.ArtifactFormat.AUTO),
            "task_run2": serde.result_from_artifact("another", ir.ArtifactFormat.AUTO),
            "task_run3": serde.result_from_artifact(123, ir.ArtifactFormat.AUTO),
        }
        runtime.get_workflow_project.return_value = ProjectRef(
            workspace_id="ws", project_id="proj"
        )
        invs = sample_task_inv_ids

        running_wf_run_model.task_runs = [
            TaskRunModel(
                id="task_run1",
                invocation_id=invs[0],
                status=RunStatus(state=State.SUCCEEDED),
            ),
            TaskRunModel(
                id="task_run2",
                invocation_id=invs[1],
                status=RunStatus(state=State.FAILED),
            ),
            TaskRunModel(
                id="task_run3",
                invocation_id=invs[2],
                status=RunStatus(state=State.FAILED),
            ),
            TaskRunModel(
                id="task_run4",
                invocation_id=invs[3],
                status=RunStatus(state=State.FAILED),
            ),
        ]

        return runtime

    @staticmethod
    @pytest.fixture
    def run(sample_wf_def, mock_runtime) -> _api.WorkflowRun:
        return _api.WorkflowRun._start(
            wf_def=sample_wf_def.model,
            runtime=mock_runtime,
            config=None,
            project=None,
            dry_run=False,
        )

    @staticmethod
    @pytest.fixture
    def runtime_repo_mock(monkeypatch):
        runtime_repo = create_autospec(repos.RuntimeRepo)
        monkeypatch.setattr(repos, "RuntimeRepo", Mock(return_value=runtime_repo))
        return runtime_repo

    class TestByID:
        @staticmethod
        @pytest.fixture
        def config_by_id_repo_mock(monkeypatch):
            config_repo = create_autospec(repos.ConfigByIDRepo)
            monkeypatch.setattr(repos, "ConfigByIDRepo", Mock(return_value=config_repo))
            return config_repo

        @staticmethod
        @pytest.fixture
        def wf_def_repo_mock(monkeypatch):
            wf_def_repo = create_autospec(repos.WFDefRepo)
            monkeypatch.setattr(repos, "WFDefRepo", Mock(return_value=wf_def_repo))
            return wf_def_repo

        @staticmethod
        @pytest.mark.parametrize("pass_config", [False, True])
        def test_passing_values(
            config_by_id_repo_mock,
            wf_def_repo_mock,
            runtime_repo_mock,
            pass_config: bool,
        ):
            """Verifies we pass args to the underlying repos."""
            # Given
            run_id = sentinel.wf_run_id
            config = sentinel.config
            wf_def = sentinel.wf_def
            runtime = sentinel.runtime
            resolved_config = sentinel.resolved_config

            config_by_id_repo_mock.get_config.return_value = resolved_config
            runtime_repo_mock.get_runtime.return_value = runtime
            wf_def_repo_mock.get_wf_def.return_value = wf_def

            # When
            if pass_config:
                run = _api.WorkflowRun.by_id(run_id=run_id, config=config)
            else:
                run = _api.WorkflowRun.by_id(run_id=run_id)

            # Then
            # Sets attrs appropriately
            assert run._config == resolved_config
            assert run._run_id == run_id
            assert run._wf_def == wf_def
            assert run._runtime == runtime

        class TestPassingErrors:
            @staticmethod
            @pytest.mark.parametrize(
                "exception,msg",
                [
                    (ConfigFileNotFoundError(), "None"),
                    (ConfigNameNotFoundError(), "None"),
                    (
                        RuntimeQuerySummaryError(
                            wf_run_id=sentinel.wf_run_id,
                            not_found_runtimes=[
                                RuntimeQuerySummaryError.RuntimeInfo(
                                    runtime_name=RuntimeName.CE_REMOTE,
                                    config_name="cluster1",
                                    server_uri="https://cluster1.example.com",
                                ),
                            ],
                            unauthorized_runtimes=[
                                RuntimeQuerySummaryError.RuntimeInfo(
                                    runtime_name=RuntimeName.CE_REMOTE,
                                    config_name="cluster2",
                                    server_uri="https://cluster2.example.com",
                                ),
                            ],
                            not_running_runtimes=[
                                RuntimeQuerySummaryError.RuntimeInfo(
                                    runtime_name=RuntimeName.RAY_LOCAL,
                                    config_name="ray",
                                    server_uri=None,
                                ),
                            ],
                        ),
                        (
                            "Couldn't find any runtime that knows about this workflow "
                            "run ID. Runtimes with 'not found' response: ['cluster1']. "
                            "Runtimes with 'unauthorized' response: ['cluster2']. "
                            "Runtimes that weren't up: ['ray']."
                        ),
                    ),
                ],
            )
            def test_config_repo_errors(config_by_id_repo_mock, exception, msg):
                # Given
                config_by_id_repo_mock.get_config.side_effect = exception

                with pytest.raises(type(exception)) as exc_info:
                    # When
                    _ = _api.WorkflowRun.by_id(
                        run_id=sentinel.wf_run_id, config=sentinel.config
                    )
                # Then
                assert str(exc_info.value) == msg

            @staticmethod
            @pytest.mark.usefixtures("config_by_id_repo_mock")
            @pytest.mark.usefixtures("runtime_repo_mock")
            @pytest.mark.parametrize(
                "exception",
                [
                    UnauthorizedError(),
                    WorkflowRunNotFoundError(),
                ],
            )
            def test_wf_def_repo_errors(wf_def_repo_mock, exception):
                # Given
                wf_def_repo_mock.get_wf_def.side_effect = exception

                # Then
                with pytest.raises(type(exception)):
                    # When
                    _ = _api.WorkflowRun.by_id(
                        run_id=sentinel.wf_run_id, config=sentinel.config
                    )

    class TestStartFromIR:
        @staticmethod
        @pytest.fixture
        def wf_ir_def(sample_wf_def):
            return sample_wf_def.model

        class TestIntegration:
            """Runs a real workflow with in-process runtime."""

            @staticmethod
            @pytest.mark.parametrize(
                "config", ["in_process", _api.RuntimeConfig.in_process()]
            )
            def test_happy_path(wf_ir_def, config):
                wf_run = _api.WorkflowRun.start_from_ir(wf_ir_def, config)

                assert wf_run.get_results() == ("Hellothere", "Generalkenobi")

            @staticmethod
            def test_wrong_config_type(wf_ir_def):
                with pytest.raises(TypeError):
                    _api.WorkflowRun.start_from_ir(wf_ir_def, 123)  # type: ignore

        @staticmethod
        @pytest.fixture
        def config_by_name_repo_mock(monkeypatch):
            config_repo = create_autospec(repos.ConfigByNameRepo)
            monkeypatch.setattr(
                repos, "ConfigByNameRepo", Mock(return_value=config_repo)
            )
            return config_repo

        @staticmethod
        def test_uses_mocked_runtime(
            wf_ir_def, config_by_name_repo_mock, runtime_repo_mock, mock_runtime
        ):
            # Given
            input_config = sentinel.input_config
            config_by_name_repo_mock.normalize_config.return_value = (
                sentinel.resolved_config
            )

            runtime_repo_mock.get_runtime.return_value = mock_runtime

            # When
            wf_run = _api.WorkflowRun.start_from_ir(wf_ir_def, input_config)

            # Then
            assert wf_run.run_id == "wf_pass_tuple-1"

    class TestGetStatus:
        @staticmethod
        def test_returns_status_from_runtime(run, mock_runtime):
            # Given
            # When
            state = run.get_status()
            # Then
            mock_runtime.get_workflow_run_status.assert_called()
            assert state == State.RUNNING

    class TestGetStatusModel:
        @staticmethod
        def test_matches_get_status(run):
            model = run.get_status_model()

            assert model.status.state == run.get_status()

        @staticmethod
        def test_happy_path():
            # Given
            run_id = "wf.1"
            wf_def = Mock()
            config = Mock()
            runtime = Mock()
            run = _api.WorkflowRun(
                run_id=run_id, wf_def=wf_def, runtime=runtime, config=config
            )

            # When
            run.get_status_model()

            # Then
            runtime.get_workflow_run_status.assert_called_with(run_id)

        @staticmethod
        def test_suppresses_versionmismatch_warnings():
            # Given
            run_id = "wf.1"
            wf_def = Mock()
            config = Mock()
            runtime = Mock()

            def raise_warnings(*args, **kwargs):
                warnings.warn("a warning that should not be suppressed")
                warnings.warn(VersionMismatch("foo", Mock(), None))
                warnings.warn(VersionMismatch("foo", Mock(), None))

            runtime.get_workflow_run_status.side_effect = raise_warnings

            run = _api.WorkflowRun(
                run_id=run_id, wf_def=wf_def, runtime=runtime, config=config
            )

            # When
            with pytest.warns(Warning) as record:
                run.get_status_model()

            # Then
            assert len(record) == 1
            assert str(record[0].message) == str(
                UserWarning("a warning that should not be suppressed")
            )

    class TestWaitUntilFinished:
        class TestHappyPath:
            @staticmethod
            def test_verbose(monkeypatch, run, mock_runtime, capsys):
                # Given
                monkeypatch.setattr(time, "sleep", MagicMock())

                # When
                run.wait_until_finished()

                # Then
                # We expect wait_until_finished to keep calling get_workflow_run_status
                # from the runtime until the status is in a completed state.
                # The mock runtime will return RUNNING twice before SUCCEEDED.
                # We expect 3 total calls to get_workflow_run_status
                assert mock_runtime.get_workflow_run_status.call_count == 3

                # We expect x prints to stderr.
                captured = capsys.readouterr()
                assert captured.out == ""
                assert captured.err == (
                    "wf_pass_tuple-1 is RUNNING. Sleeping for 4.0s...\n"
                    "wf_pass_tuple-1 is RUNNING. Sleeping for 4.0s...\n"
                    "wf_pass_tuple-1 is SUCCEEDED\n"
                )

            @staticmethod
            def test_verbose_with_message(monkeypatch, run, mock_runtime, capsys):
                # Given
                monkeypatch.setattr(time, "sleep", MagicMock())
                mock_runtime.get_workflow_run_status.return_value.message = (
                    "<message sentinel>"
                )

                # When
                run.wait_until_finished()

                # Then
                captured = capsys.readouterr()
                assert captured.out == ""
                assert captured.err == (
                    "wf_pass_tuple-1 is RUNNING. Sleeping for 4.0s...\n"
                    "wf_pass_tuple-1 is RUNNING. Sleeping for 4.0s...\n"
                    "wf_pass_tuple-1 is SUCCEEDED - <message sentinel>\n"
                )

            @staticmethod
            def test_quiet(monkeypatch, run, mock_runtime, capsys):
                # Given
                monkeypatch.setattr(time, "sleep", MagicMock())

                # When
                run.wait_until_finished(verbose=False)

                # Then
                # We expect wait_until_finished to keep calling get_workflow_run_status
                # from the runtime until the status is in a completed state.
                # The mock runtime will return RUNNING twice before SUCCEEDED.
                # We expect 3 total calls to get_workflow_run_status
                assert mock_runtime.get_workflow_run_status.call_count == 3

                # We expect no prints to stderr.
                captured = capsys.readouterr()
                assert captured.out == ""
                assert captured.err == ""

            @staticmethod
            @pytest.mark.parametrize(
                "completed_state",
                (State.SUCCEEDED, State.FAILED, State.TERMINATED, State.KILLED),
            )
            def test_completed_states(monkeypatch, run, mock_runtime, completed_state):
                # Given
                monkeypatch.setattr(time, "sleep", MagicMock())
                mock_runtime.get_workflow_run_status.return_value = (
                    _fake_completed_workflow(completed_state)
                )

                # When
                s = run.wait_until_finished()

                # Then
                assert mock_runtime.get_workflow_run_status.call_count == 3
                assert s == completed_state

    class TestGetResults:
        @staticmethod
        def test_raises_exception_if_workflow_not_finished(run):
            # Given
            # When
            with pytest.raises(WorkflowRunNotFinished) as exc_info:
                run.get_results()
            # Then
            assert (
                "Workflow run with id wf_pass_tuple-1 has not finished. "
                "Current state: State.RUNNING"
            ) in str(exc_info)

        @staticmethod
        @pytest.mark.slow
        def test_waits_when_wait_is_true(monkeypatch, run, mock_runtime):
            # Given
            monkeypatch.setattr(time, "sleep", MagicMock())
            # When
            results = run.get_results(wait=True)
            # Then
            assert mock_runtime.get_workflow_run_status.call_count >= 1
            assert results is not None
            assert results == "woohoo!"

        @staticmethod
        def test_waits_when_wait_is_explicitly_false(run, mock_runtime):
            # Remove RUNNING in mock
            mock_runtime.get_workflow_run_status.side_effect = None
            # Given
            # When
            results = run.get_results(wait=False)
            # Then
            assert results is not None
            assert results == "woohoo!"
            assert mock_runtime.get_workflow_run_status.call_count == 1

    class TestGetResultsSerialized:
        @staticmethod
        def test_raises_exception_if_workflow_not_finished(run):
            # Given
            # When
            with pytest.raises(WorkflowRunNotFinished) as exc_info:
                run.get_results_serialized()
            # Then
            assert (
                "Workflow run with id wf_pass_tuple-1 has not finished. "
                "Current state: State.RUNNING"
            ) in str(exc_info)

        @staticmethod
        @pytest.mark.slow
        def test_waits_when_wait_is_true(monkeypatch, run, mock_runtime):
            # Given
            monkeypatch.setattr(time, "sleep", MagicMock())
            # When
            results = run.get_results_serialized(wait=True)
            # Then
            assert mock_runtime.get_workflow_run_status.call_count >= 1
            assert results is not None
            assert isinstance(results[0], JSONResult)
            assert results[0].value == '"woohoo!"'

        @staticmethod
        def test_waits_when_wait_is_explicitly_false(run, mock_runtime):
            # Remove RUNNING in mock
            mock_runtime.get_workflow_run_status.side_effect = None
            # Given
            # When
            results = run.get_results_serialized(wait=False)
            # Then
            assert results is not None
            assert isinstance(results[0], JSONResult)
            assert results[0].value == '"woohoo!"'

    class TestGetArtifacts:
        @staticmethod
        def test_handling_n_outputs():
            """
            Some tasks return 1 value, some return multiple. The values in the
            dict returned from `sdk.WorkflowRun.get_artifacts()` is supposed
            to correspond to whatever we would get if we ran the task function
            directly.

            Test boundary::
                [sdk.WorkflowRun]->[RuntimeInterface]
                                 ->[ir.WorkflowDef]
            """
            # Given
            runtime = create_autospec(RuntimeInterface)

            # The RuntimeInterface's contract for get_available_outputs is
            # to return whatever the task function returned.
            runtime.get_available_outputs.return_value = {
                "inv1": serde.result_from_artifact(42, ir.ArtifactFormat.AUTO),
                "inv2": serde.result_from_artifact((21, 38), ir.ArtifactFormat.AUTO),
            }

            mock_inv1 = create_autospec(ir.TaskInvocation)
            mock_inv1.output_ids = ["art1"]

            mock_inv2 = create_autospec(ir.TaskInvocation)
            mock_inv2.output_ids = ["art2", "art3"]

            wf_def = create_autospec(ir.WorkflowDef)
            wf_def.task_invocations = {
                "inv1": mock_inv1,
                "inv2": mock_inv2,
            }

            wf_run = _api.WorkflowRun(
                run_id="wf.1",
                wf_def=wf_def,
                runtime=runtime,
            )

            # When
            artifacts_dict = wf_run.get_artifacts()

            # Then
            assert artifacts_dict == {
                "inv1": 42,
                "inv2": (21, 38),
            }

    class TestGetArtifactsSerialized:
        @staticmethod
        def test_serialized_artifacts():
            # Given
            runtime = create_autospec(RuntimeInterface)

            values = {
                "inv1": serde.result_from_artifact(42, ir.ArtifactFormat.AUTO),
                "inv2": serde.result_from_artifact((21, 38), ir.ArtifactFormat.AUTO),
            }

            runtime.get_available_outputs.return_value = values

            wf_def = create_autospec(ir.WorkflowDef)

            wf_run = _api.WorkflowRun(
                run_id="wf.1",
                wf_def=wf_def,
                runtime=runtime,
            )

            # When
            artifacts_dict = wf_run.get_artifacts_serialized()

            # Then
            assert artifacts_dict == values

    class TestTaskFilters:
        class TestTaskMatchesSchemaFilters:
            @pytest.fixture
            def schema_task_run(self):
                task = create_autospec(TaskRunModel)
                task.id = "<id sentinel>"
                task.invocation_id = "<inv id sentinel>"
                task.status = create_autospec(RunStatus)
                task.status.state = State.SUCCEEDED
                return task

            @staticmethod
            def test_no_filters(schema_task_run):
                assert _api.WorkflowRun._task_matches_schema_filters(schema_task_run)

            @staticmethod
            def test_matching_state(schema_task_run):
                assert _api.WorkflowRun._task_matches_schema_filters(
                    schema_task_run, state=State.SUCCEEDED
                )

            @staticmethod
            @pytest.mark.parametrize(
                "state", [state for state in State if state != State.SUCCEEDED]
            )
            def test_conflicting_state(schema_task_run, state):
                assert not _api.WorkflowRun._task_matches_schema_filters(
                    schema_task_run, state=state
                )

            @staticmethod
            @pytest.mark.parametrize("task_run_id", ["<id sentinel>", ".*"])
            def test_matching_task_run_id(schema_task_run, task_run_id):
                assert _api.WorkflowRun._task_matches_schema_filters(
                    schema_task_run, task_run_id=task_run_id
                )

            @staticmethod
            @pytest.mark.parametrize("task_run_id", ["foo", "<id sentinel"])
            def test_conflicting_task_run_id(schema_task_run, task_run_id):
                assert not _api.WorkflowRun._task_matches_schema_filters(
                    schema_task_run, task_run_id=task_run_id
                )

            @staticmethod
            @pytest.mark.parametrize("task_inv_id", ["<inv id sentinel>", ".*"])
            def test_matching_task_inv_id(schema_task_run, task_inv_id):
                assert _api.WorkflowRun._task_matches_schema_filters(
                    schema_task_run, task_invocation_id=task_inv_id
                )

            @staticmethod
            @pytest.mark.parametrize("task_inv_id", ["foo", "<inv id sentinel"])
            def test_conflicting_task_inv_id(schema_task_run, task_inv_id):
                assert not _api.WorkflowRun._task_matches_schema_filters(
                    schema_task_run, task_invocation_id=task_inv_id
                )

            @staticmethod
            @pytest.mark.parametrize("state", [None, State.SUCCEEDED])
            @pytest.mark.parametrize("task_run_id", [None, "<id sentinel>", ".*"])
            @pytest.mark.parametrize(
                "task_invocation_id", [None, "<inv id sentinel>", ".*"]
            )
            def test_multiple_matching_filters(
                schema_task_run, state, task_run_id, task_invocation_id
            ):
                assert _api.WorkflowRun._task_matches_schema_filters(
                    schema_task_run,
                    state=state,
                    task_run_id=task_run_id,
                    task_invocation_id=task_invocation_id,
                )

            @staticmethod
            @pytest.mark.parametrize(
                "state", [state for state in State if state != State.SUCCEEDED]
            )
            @pytest.mark.parametrize("task_run_id", [None, "foo", "<id sentinel"])
            @pytest.mark.parametrize(
                "task_invocation_id", [None, "foo", "<inv id sentinel"]
            )
            def test_multiple_conflicting_filters(
                schema_task_run, state, task_run_id, task_invocation_id
            ):
                assert not _api.WorkflowRun._task_matches_schema_filters(
                    schema_task_run,
                    state=state,
                    task_run_id=task_run_id,
                    task_invocation_id=task_invocation_id,
                )

            @staticmethod
            @pytest.mark.parametrize(
                "state, task_run_id, task_invocation_id",
                [
                    (State.SUCCEEDED, "<id sentinel>", "foo"),
                    (State.SUCCEEDED, "foo", "<inv id sentinel>"),
                    (State.WAITING, "<id sentinel>", "<inv id sentinel>"),
                ],
            )
            def test_mix_of_matching_and_conflicting_filters(
                schema_task_run, state, task_run_id, task_invocation_id
            ):
                assert not _api.WorkflowRun._task_matches_schema_filters(
                    schema_task_run,
                    state=state,
                    task_run_id=task_run_id,
                    task_invocation_id=task_invocation_id,
                )

        class TestTaskMatchesAPIFilters:
            @pytest.fixture
            def api_task_run(self):
                task = create_autospec(TaskRun)
                task.fn_name = "<function name sentinel>"
                return task

            @staticmethod
            def test_no_filters(api_task_run):
                assert _api.WorkflowRun._task_matches_api_filters(api_task_run)

            @staticmethod
            @pytest.mark.parametrize(
                "function_name", ["<function name sentinel>", ".*"]
            )
            def test_matching_function_name(api_task_run, function_name):
                assert _api.WorkflowRun._task_matches_api_filters(
                    api_task_run, task_fn_name=function_name
                )

            @staticmethod
            @pytest.mark.parametrize(
                "function_name", ["<function name sentinel", "foo"]
            )
            def test_conflicting_function_name(api_task_run, function_name):
                assert not _api.WorkflowRun._task_matches_api_filters(
                    api_task_run, task_fn_name=function_name
                )

    class TestGetTasks:
        @staticmethod
        def test_get_tasks_from_started_workflow(run):
            # Given
            # When
            tasks = run.get_tasks()

            # Then
            assert len(tasks) == 4

            wf_def_model = run._wf_def
            for task in tasks:
                assert task.workflow_run_id == run.run_id
                assert task._runtime == run._runtime
                assert task._wf_def == run._wf_def
                assert task.task_invocation_id in wf_def_model.task_invocations

        class TestFiltering:
            @staticmethod
            @pytest.mark.parametrize("state", [None, "<state sentinel>"])
            @pytest.mark.parametrize("task_run_id", [None, "<task run id sentinel>"])
            @pytest.mark.parametrize(
                "task_invocation_id", [None, "<task inv id sentinel>"]
            )
            @pytest.mark.parametrize(
                "task_function_name", [None, "<task fn name sentinel>"]
            )
            def test_argument_passing(
                run, state, task_run_id, task_invocation_id, task_function_name
            ):
                # Given
                run._task_matches_schema_filters = Mock(return_value=True)
                run._task_matches_api_filters = Mock(return_value=True)

                # When
                _ = run.get_tasks(
                    state=state,
                    task_run_id=task_run_id,
                    task_invocation_id=task_invocation_id,
                    function_name=task_function_name,
                )

                # Then
                assert run._task_matches_schema_filters.call_count == 4
                for mock_call in run._task_matches_schema_filters.call_args_list:
                    assert mock_call[1]["state"] == state
                    assert mock_call[1]["task_run_id"] == task_run_id
                    assert mock_call[1]["task_invocation_id"] == task_invocation_id
                assert run._task_matches_api_filters.call_count == 4
                for mock_call in run._task_matches_api_filters.call_args_list:
                    assert mock_call[1]["task_fn_name"] == task_function_name

            @staticmethod
            @pytest.mark.parametrize(
                "schema_filter, api_filter, n_expected_tasks",
                [([True, False, True, True], [False, True, True], 2)],
            )
            def test_filters_tasks(run, schema_filter, api_filter, n_expected_tasks):
                # Given
                run._task_matches_schema_filters = Mock(side_effect=schema_filter)
                run._task_matches_api_filters = Mock(side_effect=api_filter)

                # When
                tasks = run.get_tasks()

                # Then
                assert len(tasks) == n_expected_tasks

            @staticmethod
            def test_returns_empty_set_for_no_matching_tasks(run):
                run._task_matches_schema_filters = Mock(return_value=False)
                run._task_matches_api_filters = Mock(return_value=False)

                tasks = run.get_tasks()

                # Then
                assert tasks == []

        class TestTasksSorted:
            @staticmethod
            @_dsl.task
            def a(*_):
                return 1

            @staticmethod
            @_dsl.task
            def b(*_):
                return 2

            @staticmethod
            @_dsl.task
            def c(*_):
                return 3

            @staticmethod
            @_dsl.task
            def d(*_):
                return 4

            def test_linear_graph(self):
                """
                tests wf with graph
                [a]
                 |
                [b]
                 |
                [c]
                """

                @_workflow.workflow
                def simple_wf():
                    a = self.a()
                    b = self.b(a)
                    return self.c(b)

                wf_run = simple_wf().run("in_process")

                tasks = wf_run.get_tasks()

                assert tasks[0].fn_name == "a"
                assert tasks[1].fn_name == "b"
                assert tasks[2].fn_name == "c"

            def test_non_linear_graph(self):
                """
                tests wf with graph
                    [a]
                  ___|___
                 |       |
                [b]     [c]
                 |_______|
                     |
                    [d]
                """

                @_workflow.workflow
                def simple_wf():
                    a = self.a()
                    b = self.b(a)
                    c = self.c(a)
                    return self.d(b, c)

                wf_run = simple_wf().run("in_process")

                tasks = wf_run.get_tasks()

                assert tasks[0].fn_name == "a"
                assert tasks[1].fn_name in ["b", "c"]
                assert tasks[2].fn_name in ["b", "c"]
                assert tasks[3].fn_name == "d"

            def test_deterministic_graph(self):
                """
                We don't care what order graph is traversed, as long as it is
                <deterministic> - meaning for the same wf_def we always get the same
                order.
                As we base our order of traversal on the deterministic logic of
                artifacts in wf function, this test proves that changing order of
                arguments for one function will change the order of task objects,
                not binding us to one particular order
                """

                @_workflow.workflow
                def wf_1():
                    return self.a(), self.b()

                @_workflow.workflow
                def wf_2():
                    return self.b(), self.a()

                tasks_1 = wf_1().run("in_process").get_tasks()
                tasks_2 = wf_2().run("in_process").get_tasks()

                assert tasks_1[0].fn_name != tasks_2[0].fn_name
                assert tasks_1[1].fn_name != tasks_2[1].fn_name

                assert tasks_1[0].fn_name == tasks_2[1].fn_name
                assert tasks_1[1].fn_name == tasks_2[0].fn_name

    class TestGetLogs:
        @staticmethod
        def test_happy_path(run: _api.WorkflowRun, sample_task_inv_ids):
            # Given
            invs = sample_task_inv_ids
            log_reader = create_autospec(LogReader)
            log_reader.get_workflow_logs.return_value = WorkflowLogs(
                per_task={
                    invs[0]: LogOutput(out=["woohoo!\n"], err=[]),
                    invs[1]: LogOutput(out=["another\n", "line\n"], err=[]),
                    # This task invocation was executed, but it produced no logs.
                    invs[2]: LogOutput(out=[], err=[]),
                    # There's also 4th task invocation in the workflow def, it wasn't
                    # executed yet, so we don't return it.
                },
                env_setup=LogOutput(out=[], err=[]),
                system=LogOutput(
                    out=[
                        "<sys log sentinel 1>",
                        "<sys log sentinel 2>",
                        "<sys log sentinel 3>",
                    ],
                    err=[],
                ),
                other=LogOutput(out=[], err=[]),
            )

            run._runtime = log_reader

            # When
            logs = run.get_logs()

            # Then
            assert len(logs.per_task) == 3

            expected_inv = "invocation-0-task-capitalize"
            assert expected_inv in logs.per_task
            assert len(logs.per_task[expected_inv]) == 1
            assert logs.per_task[expected_inv].out[0] == "woohoo!\n"

            assert len(logs.system.out) == 3

            assert logs.system.out == [
                "<sys log sentinel 1>",
                "<sys log sentinel 2>",
                "<sys log sentinel 3>",
            ]

    class TestGetConfig:
        @staticmethod
        def test_happy_path():
            config = _api.RuntimeConfig.in_process()
            wf = wf_pass_tuple().run(config=config)

            assert wf.config == config

    class TestStop:
        @staticmethod
        def test_happy_path():
            # Given
            run_id = "wf.1"
            wf_def = Mock()
            runtime = Mock()
            config = Mock()
            run = _api.WorkflowRun(
                run_id=run_id, wf_def=wf_def, runtime=runtime, config=config
            )

            # When
            run.stop()

            # Then
            runtime.stop_workflow_run.assert_called_with(run_id, force=None)

        @staticmethod
        @pytest.mark.parametrize(
            "force",
            (True, False),
        )
        def test_force_stop(force):
            # Given
            run_id = "wf.1"
            wf_def = Mock()
            runtime = Mock()
            config = Mock()
            run = _api.WorkflowRun(
                run_id=run_id, wf_def=wf_def, runtime=runtime, config=config
            )

            # When
            run.stop(force=force)

            # Then
            runtime.stop_workflow_run.assert_called_with(run_id, force=force)

        @staticmethod
        @pytest.mark.parametrize(
            "exc",
            [
                UnauthorizedError(),
                WorkflowRunCanNotBeTerminated(),
                WorkflowRunNotFoundError(),
            ],
        )
        def test_error_from_runtime(exc):
            # Given
            run_id = "wf.1"
            wf_def = Mock()

            runtime = Mock()
            runtime.stop_workflow_run.side_effect = exc

            config = Mock()
            run = _api.WorkflowRun(
                run_id=run_id, wf_def=wf_def, runtime=runtime, config=config
            )

            # Then
            with pytest.raises(type(exc)):
                # When
                run.stop()

    class TestProject:
        def test_get_project(self, run):
            project = run.project
            assert project.workspace_id == "ws"
            assert project.project_id == "proj"

        def test_value_get_cached(self, run):
            _ = run.project
            _ = run.project

            run._runtime.get_workflow_project.assert_called_once()


class TestListWorkflows:
    @staticmethod
    @pytest.fixture
    def mock_config_runtime(monkeypatch):
        run = MagicMock()
        type(run).id = PropertyMock(side_effect=["wf0", "wf1", "wf2"])
        runtime = Mock(RuntimeInterface)
        # For getting workflow ID
        runtime.list_workflow_runs.return_value = [run, run, run]
        mock_config = MagicMock(_api.RuntimeConfig)
        mock_config._get_runtime.return_value = runtime
        monkeypatch.setattr(
            _api.RuntimeConfig, "load", MagicMock(return_value=mock_config)
        )

        return runtime

    def test_get_all_wfs(self, mock_config_runtime):
        # Given
        # When
        runs = _api.list_workflow_runs("mocked_config")
        # Then
        assert len(runs) == 3
        assert runs[0].run_id == "wf0"
        assert runs[1].run_id == "wf1"
        assert runs[2].run_id == "wf2"

    def test_invalid_max_age(self, mock_config_runtime):
        # Given
        # When
        with pytest.raises(ValueError) as exc_info:
            _ = _api.list_workflow_runs("mocked_config", max_age="hello")
        assert exc_info.match("Time strings must")

    @pytest.mark.parametrize(
        "max_age, delta",
        [
            ("1d", timedelta(days=1)),
            ("2h", timedelta(hours=2)),
            ("3m", timedelta(minutes=3)),
            ("4s", timedelta(seconds=4)),
            ("1d2h3m4s", timedelta(days=1, seconds=7384)),
        ],
    )
    def test_with_max_age(self, mock_config_runtime, max_age, delta):
        # Given
        # When
        _ = _api.list_workflow_runs("mocked_config", max_age=max_age)
        # Then
        mock_config_runtime.list_workflow_runs.assert_called_once_with(
            limit=None,
            max_age=delta,
            state=None,
            workspace=None,
        )

    def test_with_limit(self, mock_config_runtime):
        # Given
        # When
        _ = _api.list_workflow_runs("mocked_config", limit=10)
        # Then
        mock_config_runtime.list_workflow_runs.assert_called_once_with(
            limit=10,
            max_age=None,
            state=None,
            workspace=None,
        )

    def test_with_state(self, mock_config_runtime):
        # Given
        # When
        _ = _api.list_workflow_runs("mocked_config", state=State.SUCCEEDED)
        # Then
        mock_config_runtime.list_workflow_runs.assert_called_once_with(
            limit=None,
            max_age=None,
            state=State.SUCCEEDED,
            workspace=None,
        )

    def test_with_workspace(self, mock_config_runtime):
        # GIVEN
        # WHEN
        _ = _api.list_workflow_runs(
            "mocked_config", workspace="<workspace ID sentinel>"
        )

        # THEN
        mock_config_runtime.list_workflow_runs.assert_called_once_with(
            limit=None,
            max_age=None,
            state=None,
            workspace="<workspace ID sentinel>",
        )

    def test_raises_exception_with_project_and_no_workspace(self, mock_config_runtime):
        # GIVEN
        # WHEN
        with pytest.raises(ProjectInvalidError) as e:
            _ = _api.list_workflow_runs(
                "mocked_config", project="<project ID sentinel>"
            )

        # THEN
        assert e.exconly() == (
            "orquestra.sdk.exceptions.ProjectInvalidError: The project "
            "`<project ID sentinel>` cannot be uniquely identified without a workspace "
            "parameter."
        )

    def test_in_studio_passed_arguments(self, monkeypatch, mock_config_runtime):
        # GIVEN
        monkeypatch.setenv("ORQ_CURRENT_WORKSPACE", "env_workspace")
        monkeypatch.setenv("ORQ_CURRENT_PROJECT", "env_project")

        # WHEN
        _ = _api.list_workflow_runs(
            "mocked_config",
            workspace="<workspace ID sentinel>",
        )

        # THEN
        mock_config_runtime.list_workflow_runs.assert_called_once_with(
            limit=None,
            max_age=None,
            state=None,
            workspace="<workspace ID sentinel>",
        )

    def test_in_studio_no_arguments(self, monkeypatch, mock_config_runtime):
        # GIVEN
        monkeypatch.setenv("ORQ_CURRENT_WORKSPACE", "env_workspace")
        monkeypatch.setenv("ORQ_CURRENT_PROJECT", "env_project")

        # overwrite config name
        mock_config = MagicMock(_api.RuntimeConfig)
        mock_config._get_runtime.return_value = mock_config_runtime
        mock_config.name = "auto"
        monkeypatch.setattr(
            _api.RuntimeConfig, "load", MagicMock(return_value=mock_config)
        )

        # WHEN
        _ = _api.list_workflow_runs(
            "mock_config",
        )

        # THEN
        mock_config_runtime.list_workflow_runs.assert_called_once_with(
            limit=None,
            max_age=None,
            state=None,
            workspace="env_workspace",
        )

    @staticmethod
    def test_suppresses_versionmismatch_warnings(mock_config_runtime):
        # Given
        def raise_warnings(*args, **kwargs):
            warnings.warn("a warning that should not be suppressed")
            warnings.warn(VersionMismatch("foo", Mock(), None))
            warnings.warn(VersionMismatch("foo", Mock(), None))
            return [Mock(), Mock(), Mock()]

        mock_config_runtime.list_workflow_runs.side_effect = raise_warnings

        # When
        with pytest.warns(Warning) as record:
            _ = _api.list_workflow_runs("mocked_config")

        # Then
        assert len(record) == 1
        assert str(record[0].message) == str(
            UserWarning("a warning that should not be suppressed")
        )


class TestListWorkflowSummaries:
    @staticmethod
    @pytest.fixture
    def mock_config_runtime(monkeypatch):
        run = MagicMock()
        type(run).id = PropertyMock(side_effect=["wf0", "wf1", "wf2"])
        runtime = Mock(RuntimeInterface)
        # For getting workflow ID
        runtime.list_workflow_run_summaries.return_value = [run, run, run]
        mock_config = MagicMock(_api.RuntimeConfig)
        mock_config._get_runtime.return_value = runtime
        monkeypatch.setattr(
            _api.RuntimeConfig, "load", MagicMock(return_value=mock_config)
        )

        return runtime

    def test_get_all_wfs(self, mock_config_runtime):
        # Given
        # When
        runs = _api.list_workflow_run_summaries("mocked_config")
        # Then
        assert len(runs) == 3
        assert runs[0].id == "wf0"
        assert runs[1].id == "wf1"
        assert runs[2].id == "wf2"

    def test_invalid_max_age(self, mock_config_runtime):
        # Given
        # When
        with pytest.raises(ValueError) as exc_info:
            _ = _api.list_workflow_run_summaries("mocked_config", max_age="hello")
        assert exc_info.match("Time strings must")

    @pytest.mark.parametrize(
        "max_age, delta",
        [
            ("1d", timedelta(days=1)),
            ("2h", timedelta(hours=2)),
            ("3m", timedelta(minutes=3)),
            ("4s", timedelta(seconds=4)),
            ("1d2h3m4s", timedelta(days=1, seconds=7384)),
        ],
    )
    def test_with_max_age(self, mock_config_runtime, max_age, delta):
        # Given
        # When
        _ = _api.list_workflow_run_summaries("mocked_config", max_age=max_age)
        # Then
        mock_config_runtime.list_workflow_run_summaries.assert_called_once_with(
            limit=None,
            max_age=delta,
            state=None,
            workspace=None,
        )

    def test_with_limit(self, mock_config_runtime):
        # Given
        # When
        _ = _api.list_workflow_run_summaries("mocked_config", limit=10)
        # Then
        mock_config_runtime.list_workflow_run_summaries.assert_called_once_with(
            limit=10,
            max_age=None,
            state=None,
            workspace=None,
        )

    def test_with_state(self, mock_config_runtime):
        # Given
        # When
        _ = _api.list_workflow_run_summaries("mocked_config", state=State.SUCCEEDED)
        # Then
        mock_config_runtime.list_workflow_run_summaries.assert_called_once_with(
            limit=None,
            max_age=None,
            state=State.SUCCEEDED,
            workspace=None,
        )

    def test_with_workspace(self, mock_config_runtime):
        # GIVEN
        # WHEN
        _ = _api.list_workflow_run_summaries(
            "mocked_config", workspace="<workspace ID sentinel>"
        )

        # THEN
        mock_config_runtime.list_workflow_run_summaries.assert_called_once_with(
            limit=None,
            max_age=None,
            state=None,
            workspace="<workspace ID sentinel>",
        )

    def test_raises_exception_with_project_and_no_workspace(self, mock_config_runtime):
        # GIVEN
        # WHEN
        with pytest.raises(ProjectInvalidError) as e:
            _ = _api.list_workflow_run_summaries(
                "mocked_config", project="<project ID sentinel>"
            )

        # THEN
        assert e.exconly() == (
            "orquestra.sdk.exceptions.ProjectInvalidError: The project "
            "`<project ID sentinel>` cannot be uniquely identified without a workspace "
            "parameter."
        )

    def test_in_studio_passed_arguments(self, monkeypatch, mock_config_runtime):
        # GIVEN
        monkeypatch.setenv("ORQ_CURRENT_WORKSPACE", "env_workspace")
        monkeypatch.setenv("ORQ_CURRENT_PROJECT", "env_project")

        # WHEN
        _ = _api.list_workflow_run_summaries(
            "mocked_config",
            workspace="<workspace ID sentinel>",
        )

        # THEN
        mock_config_runtime.list_workflow_run_summaries.assert_called_once_with(
            limit=None,
            max_age=None,
            state=None,
            workspace="<workspace ID sentinel>",
        )

    def test_in_studio_no_arguments(self, monkeypatch, mock_config_runtime):
        # GIVEN
        monkeypatch.setenv("ORQ_CURRENT_WORKSPACE", "env_workspace")
        monkeypatch.setenv("ORQ_CURRENT_PROJECT", "env_project")

        # overwrite config name
        mock_config = MagicMock(_api.RuntimeConfig)
        mock_config._get_runtime.return_value = mock_config_runtime
        mock_config.name = "auto"
        monkeypatch.setattr(
            _api.RuntimeConfig, "load", MagicMock(return_value=mock_config)
        )

        # WHEN
        _ = _api.list_workflow_run_summaries(
            "mock_config",
        )

        # THEN
        mock_config_runtime.list_workflow_run_summaries.assert_called_once_with(
            limit=None,
            max_age=None,
            state=None,
            workspace="env_workspace",
        )

    @staticmethod
    def test_suppresses_versionmismatch_warnings(mock_config_runtime):
        # Given
        def raise_warnings(*args, **kwargs):
            warnings.warn("a warning that should not be suppressed")
            warnings.warn(VersionMismatch("foo", Mock(), None))
            warnings.warn(VersionMismatch("foo", Mock(), None))
            return [Mock(), Mock(), Mock()]

        mock_config_runtime.list_workflow_run_summaries.side_effect = raise_warnings

        # When
        with pytest.warns(Warning) as record:
            _ = _api.list_workflow_run_summaries("mocked_config")

        # Then
        assert len(record) == 1
        assert str(record[0].message) == str(
            UserWarning("a warning that should not be suppressed")
        )


@pytest.mark.parametrize(
    "workspace_id, project_id, workspace_env, project_env, raises, expected",
    [
        (
            "a",
            "b",
            None,
            None,
            do_not_raise(),
            ProjectRef(workspace_id="a", project_id="b"),
        ),
        ("a", None, None, None, pytest.raises(ProjectInvalidError), None),
        (None, "b", None, None, pytest.raises(ProjectInvalidError), None),
        (None, None, None, None, do_not_raise(), None),
        (
            "a",
            "b",
            "env_ws",
            "env_proj",
            do_not_raise(),
            ProjectRef(workspace_id="a", project_id="b"),
        ),
        (
            None,
            None,
            "env_ws",
            "env_proj",
            do_not_raise(),
            ProjectRef(workspace_id="env_ws", project_id="env_proj"),
        ),
        (None, None, "env_ws", None, pytest.raises(ProjectInvalidError), None),
        (None, None, None, "env_proj", pytest.raises(ProjectInvalidError), None),
    ],
)
class TestProjectId:
    def test_run(
        self,
        workspace_id,
        project_id,
        workspace_env,
        project_env,
        raises,
        expected,
        monkeypatch,
    ):
        monkeypatch.setattr(_traversal, "_global_inline_import_identifier", lambda: 0)
        workflow_create_mock = Mock()
        monkeypatch.setattr(
            InProcessRuntime, "create_workflow_run", workflow_create_mock
        )
        wf_def = wf_pass_tuple()

        if workspace_env:
            monkeypatch.setenv(name=CURRENT_WORKSPACE_ENV, value=workspace_env)
        if project_env:
            monkeypatch.setenv(name=CURRENT_PROJECT_ENV, value=project_env)

        monkeypatch.setattr(
            InProcessRuntime, "create_workflow_run", workflow_create_mock
        )
        monkeypatch.setattr(_api._config.RuntimeConfig, "name", "auto")
        with raises:
            wf_def.run("in_process", workspace_id=workspace_id, project_id=project_id)
            workflow_create_mock.assert_called_once_with(wf_def.model, expected, False)


class TestListWorkspaces:
    @staticmethod
    @pytest.fixture
    def mock_config_runtime(monkeypatch):
        ws = MagicMock()
        type(ws).workspace_id = PropertyMock(
            side_effect=[
                "ws1",
                "ws2",
            ]
        )
        runtime = Mock(RuntimeInterface)
        # For getting workflow ID
        runtime.list_workspaces.return_value = [ws, ws]
        mock_config = MagicMock(_api.RuntimeConfig)
        mock_config._get_runtime.return_value = runtime
        monkeypatch.setattr(
            _api.RuntimeConfig, "load", MagicMock(return_value=mock_config)
        )

        return runtime

    def test_list_workspaces(self, mock_config_runtime):
        # Given
        # When
        runs = list_workspaces("mocked_config")
        # Then
        assert len(runs) == 2
        assert runs[0].workspace_id == "ws1"
        assert runs[1].workspace_id == "ws2"
        mock_config_runtime.list_workspaces.assert_called_once()


class TestListProjects:
    @staticmethod
    @pytest.fixture
    def mock_config_runtime(monkeypatch):
        ws = MagicMock()
        type(ws).project_id = PropertyMock(
            side_effect=[
                "p1",
                "p2",
            ]
        )
        runtime = Mock(RuntimeInterface)
        # For getting workflow ID
        runtime.list_projects.return_value = [ws, ws]
        mock_config = MagicMock(_api.RuntimeConfig)
        mock_config._get_runtime.return_value = runtime
        monkeypatch.setattr(
            _api.RuntimeConfig, "load", MagicMock(return_value=mock_config)
        )

        return runtime

    @pytest.mark.parametrize(
        "workspace, expected_argument",
        [
            ("string_workspace_id", "string_workspace_id"),
            (Workspace(workspace_id="id", name="name"), "id"),
        ],
    )
    def test_list_projects(self, mock_config_runtime, workspace, expected_argument):
        # Given
        # When
        runs = list_projects("mocked_config", workspace)
        # Then
        assert len(runs) == 2
        assert runs[0].project_id == "p1"
        assert runs[1].project_id == "p2"
        mock_config_runtime.list_projects.assert_called_with(expected_argument)
