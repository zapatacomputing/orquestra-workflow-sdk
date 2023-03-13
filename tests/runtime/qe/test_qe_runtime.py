# © Copyright 2022 Zapata Computing Inc.
################################################################################
import base64
import copy
import datetime
import gzip
import io
import json
import tarfile
import typing as t
from pathlib import Path
from unittest.mock import MagicMock, Mock, PropertyMock

import pytest
import responses

import orquestra.sdk as sdk
import orquestra.sdk._base._db as _db
import orquestra.sdk._base._qe._qe_runtime as _qe_runtime
from orquestra.sdk import exceptions
from orquestra.sdk._base._conversions._yaml_exporter import (
    pydantic_to_yaml,
    workflow_to_yaml,
)
from orquestra.sdk._base._testing._example_wfs import my_workflow
from orquestra.sdk._base.serde import result_from_artifact
from orquestra.sdk.schema.configs import RuntimeConfiguration, RuntimeName
from orquestra.sdk.schema.ir import ArtifactNodeId, TaskInvocationId
from orquestra.sdk.schema.local_database import StoredWorkflowRun
from orquestra.sdk.schema.responses import JSONResult
from orquestra.sdk.schema.workflow_run import (
    RunStatus,
    State,
    TaskRun,
    WorkflowRun,
    WorkflowRunId,
)

QE_MINIMAL_CURRENT_REPRESENTATION: t.Dict[str, t.Any] = {
    "status": {
        "phase": "Succeeded",
        "startedAt": "1989-12-13T09:03:49Z",
        "finishedAt": "1989-12-13T09:05:14Z",
        "nodes": {
            "hello-there-abc123-r000": {
                "id": "hello-there-abc123-r000",
                "name": "hello-there-abc123-r000",
                "displayName": "hello-there-abc123-r000",
                "type": "DAG",
                "templateName": "qeDagWorkflow",
                "phase": "Succeeded",
                "startedAt": "1989-12-13T09:03:49Z",
                "finishedAt": "1989-12-13T09:05:09Z",
                "children": ["hello-there-abc123-r000-2738763496"],
                "outboundNodes": ["hello-there-abc123-r000-3825957270"],
            },
            "wf-3fmte-r000": {
                "id": "wf-3fmte-r000",
                "name": "wf-3fmte-r000",
                "displayName": "wf-3fmte-r000",
                "type": "DAG",
                "templateName": "qeDagWorkflow",
                "phase": "Failed",
                "startedAt": "1989-12-13T09:03:49Z",
                "finishedAt": "1989-12-13T09:05:09Z",
                "children": ["hello-there-abc123-r000-2738763496"],
                "outboundNodes": ["hello-there-abc123-r000-3825957270"],
            },
            "hello-there-abc123-r000-2738763496": {
                "id": "hello-there-abc123-r000-2738763496",
                "name": "hello-there-abc123-r000.invocation-1-task-multi-output-test",  # noqa: E501
                "displayName": "invocation-1-task-multi-output-test",
                "type": "Pod",
                "templateName": "invocation-1-task-multi-output-test",
                "phase": "Succeeded",
                "boundaryID": "hello-there-abc123-r000",
                "startedAt": "1989-12-13T09:03:49Z",
                "finishedAt": "1989-12-13T09:04:28Z",
                "children": ["hello-there-abc123-r000-3825957270"],
            },
            "hello-there-abc123-r000-2752640860": {
                "id": "hello-there-abc123-r000-2752640860",
                "name": "hello-there-abc123-r000.onExit",
                "displayName": "hello-there-abc123-r000.onExit",
                "type": "DAG",
                "templateName": "orquestra",
                "phase": "Succeeded",
                "startedAt": "1989-12-13T09:05:09Z",
                "finishedAt": "1989-12-13T09:05:16Z",
                "children": ["hello-there-abc123-r000-48533308"],
                "outboundNodes": ["hello-there-abc123-r000-48533308"],
            },
            "hello-there-abc123-r000-3825957270": {
                "id": "hello-there-abc123-r000-3825957270",
                "name": "hello-there-abc123-r000.invocation-0-task-make-greeting-message",  # noqa: E501
                "displayName": "invocation-0-task-make-greeting-message",
                "type": "Pod",
                "templateName": "invocation-0-task-make-greeting-message",
                "phase": "Succeeded",
                "boundaryID": "hello-there-abc123-r000",
                "startedAt": "1989-12-13T09:04:29Z",
                "finishedAt": "1989-12-13T09:05:08Z",
            },
            "hello-there-abc123-r000-48533308": {
                "id": "hello-there-abc123-r000-48533308",
                "name": "hello-there-abc123-r000.onExit.Finalizing-your-data",
                "displayName": "Finalizing-your-data",
                "type": "Pod",
                "templateName": "Finalizing-your-data",
                "phase": "Succeeded",
                "boundaryID": "hello-there-abc123-r000-2752640860",
                "startedAt": "1989-12-13T09:05:09Z",
                "finishedAt": "1989-12-13T09:05:14Z",
            },
        },
    },
}

QE_MINIMAL_CURRENT_REPRESENTATION_COMPRESSED_NODES = {
    "status": {
        "phase": QE_MINIMAL_CURRENT_REPRESENTATION["status"]["phase"],
        "startedAt": QE_MINIMAL_CURRENT_REPRESENTATION["status"]["startedAt"],
        "finishedAt": QE_MINIMAL_CURRENT_REPRESENTATION["status"]["finishedAt"],
        "compressedNodes": base64.standard_b64encode(
            gzip.compress(
                json.dumps(
                    QE_MINIMAL_CURRENT_REPRESENTATION["status"]["nodes"]
                ).encode(),
            )
        ).decode(),
    },
}

QE_MINIMAL_CURRENT_REPRESENTATION_RUNNING_WF = {
    "status": {
        "phase": "Running",
        "startedAt": "1989-12-13T09:03:49Z",
        "finishedAt": None,
        "nodes": {
            "hello-there-abc123-r000": {
                "id": "hello-there-abc123-r000",
                "name": "hello-there-abc123-r000",
                "displayName": "hello-there-abc123-r000",
                "type": "DAG",
                "templateName": "qeDagWorkflow",
                "phase": "Running",
                "startedAt": "1989-12-13T09:03:49Z",
                "finishedAt": "1989-12-13T09:05:09Z",
                "children": ["hello-there-abc123-r000-2738763496"],
                "outboundNodes": ["hello-there-abc123-r000-3825957270"],
            },
        },
    }
}

QE_MINIMAL_CURRENT_REPRESENTATION_WAITING_TASK = copy.deepcopy(
    QE_MINIMAL_CURRENT_REPRESENTATION
)
QE_MINIMAL_CURRENT_REPRESENTATION_WAITING_TASK["status"]["nodes"][
    "hello-there-abc123-r000-3825957270"
] = {
    "id": "hello-there-abc123-r000-3825957270",
    "name": "hello-there-abc123-r000.invocation-0-task-make-greeting-message",  # noqa: E501
    "displayName": "invocation-0-task-make-greeting-message",
    "type": "Pod",
    "templateName": "invocation-0-task-make-greeting-message",
    "phase": "Pending",
    "boundaryID": "hello-there-abc123-r000",
    "message": "Unschedulable: 0/8 nodes are available: 8 node(s) didn't match Pod's node affinity/selector.",  # noqa: E501
    "startedAt": "2022-07-18T11:48:12Z",
    "finishedAt": None,
}

QE_WORKFLOW_RESULT_JSON_DICT = {
    "hello-there-abc123-r000-2738763496": {
        "artifact-1-multi-output-test": {
            "serialization_format": "JSON",
            "value": '"there"',
        },
        "inputs": {
            "__sdk_fn_ref_dict": {
                "type": "sdk-metadata",
                "value": {
                    "file_path": "git-1871174d6e_github_com_zapatacomputing_orquestra_sdk/src/orquestra/sdk/examples/exportable_wf.py",  # noqa: E501
                    "function_name": "multi_output_test",
                    "line_number": 29,
                    "type": "FILE_FUNCTION_REF",
                },
            },
            "__sdk_output_node_dicts": {
                "type": "sdk-metadata",
                "value": [
                    {
                        "artifact_index": 1,
                        "custom_name": None,
                        "id": "artifact-1-multi-output-test",
                        "serialization_format": "AUTO",
                    }
                ],
            },
            "__sdk_positional_args_ids": {"type": "sdk-metadata", "value": []},
        },
        "stepID": "hello-there-abc123-r000-2738763496",
        "stepName": "invocation-1-task-multi-output-test",
        "workflowId": "hello-there-abc123-r000",
    },
    "hello-there-abc123-r000-3825957270": {
        "artifact-0-make-greeting-message": {
            "serialization_format": "JSON",
            "value": '"hello, alex zapata!there"',
        },
        "inputs": {
            "__sdk_fn_ref_dict": {
                "type": "sdk-metadata",
                "value": {
                    "file_path": "git-1871174d6e_github_com_zapatacomputing_orquestra_sdk/src/orquestra/sdk/examples/exportable_wf.py",  # noqa: E501
                    "function_name": "make_greeting",
                    "line_number": 19,
                    "type": "FILE_FUNCTION_REF",
                },
            },
            "__sdk_output_node_dicts": {
                "type": "sdk-metadata",
                "value": [
                    {
                        "artifact_index": None,
                        "custom_name": None,
                        "id": "artifact-0-make-greeting-message",
                        "serialization_format": "AUTO",
                    }
                ],
            },
            "__sdk_positional_args_ids": {"type": "sdk-metadata", "value": []},
            "additional_message": {
                "sourceArtifactName": "artifact-1-multi-output-test",
                "sourceStepID": "hello-there-abc123-r000-2738763496",
            },
            "first": {
                "type": "workflow-result-dict",
                "value": {"serialization_format": "JSON", "value": '"alex"'},
            },
            "last": {
                "type": "workflow-result-dict",
                "value": {"serialization_format": "JSON", "value": '"zapata"'},
            },
        },
        "stepID": "hello-there-abc123-r000-3825957270",
        "stepName": "invocation-0-task-make-greeting-message",
        "workflowId": "hello-there-abc123-r000",
    },
}


def _make_result_bytes(results_dict: t.Dict[str, t.Any]) -> bytes:
    results_file_bytes = json.dumps(results_dict).encode()

    tar_buf = io.BytesIO()
    with tarfile.open(mode="w:gz", fileobj=tar_buf) as tar:
        # TODO: verify that this mock reflects reality.

        # See this for creating tars in memory:
        # https://github.com/python/cpython/issues/66404#issuecomment-1093662423
        tar_info = tarfile.TarInfo("results.json")
        tar_info.size = len(results_file_bytes)
        tar.addfile(tar_info, fileobj=io.BytesIO(results_file_bytes))

    tar_buf.seek(0)
    return tar_buf.read()


QE_WORKFLOW_RESULT_BYTES = _make_result_bytes(QE_WORKFLOW_RESULT_JSON_DICT)

QE_STATUS_RESPONSE = {
    "id": "hello-there-abc123-r000",
    "status": "Succeeded",
    "currentRepresentation": base64.standard_b64encode(
        json.dumps(QE_MINIMAL_CURRENT_REPRESENTATION).encode()
    ).decode(),
    "completed": True,
    "retry": "",
    "lastModified": "1989-12-13T09:10:04.14422796Z",
    "created": "1989-12-13T09:03:49.39478764Z",
}


QE_STATUS_RESPONSE_COMPRESSED = {
    "id": "hello-there-abc123",
    "status": "Succeeded",
    "currentRepresentation": base64.standard_b64encode(
        json.dumps(QE_MINIMAL_CURRENT_REPRESENTATION_COMPRESSED_NODES).encode()
    ).decode(),
    "completed": True,
    "retry": "",
    "lastModified": "1989-12-13T09:10:04.14422796Z",
    "created": "1989-12-13T09:03:49.39478764Z",
}

QE_STATUS_RESPONSE_NOT_READY = {
    "id": "hello-there-abc123-r000",
    "status": "Pending",
    "currentRepresentation": base64.standard_b64encode(
        json.dumps({"status": {"startedAt": None, "finishedAt": None}}).encode()
    ).decode(),
    "completed": False,
    "retry": "",
    "lastModified": "1989-12-13T09:10:04.14422796Z",
    "created": "1989-12-13T09:03:49.39478764Z",
}

QE_STATUS_RESPONSE_RUNNING = {
    "id": "hello-there-abc123-r000",
    "status": "Running",
    "currentRepresentation": base64.standard_b64encode(
        json.dumps(QE_MINIMAL_CURRENT_REPRESENTATION_RUNNING_WF).encode()
    ).decode(),
    "completed": False,
    "retry": "",
    "lastModified": "1989-12-13T09:10:04.14422796Z",
    "created": "1989-12-13T09:03:49.39478764Z",
}

QE_STATUS_RESPONSE_WAITING_TASK = {
    "id": "hello-there-abc123-r000",
    "status": "Running",
    "currentRepresentation": base64.standard_b64encode(
        json.dumps(QE_MINIMAL_CURRENT_REPRESENTATION_WAITING_TASK).encode()
    ).decode(),
    "completed": False,
    "retry": "",
    "lastModified": "1989-12-13T09:10:04.14422796Z",
    "created": "1989-12-13T09:03:49.39478764Z",
}

QE_STATUS_RESPONSE_TERMINATED = copy.deepcopy(QE_STATUS_RESPONSE_WAITING_TASK)
QE_STATUS_RESPONSE_TERMINATED["status"] = "Terminated"

QE_RESPONSES = {
    "version": {
        "serverVersion": "patched",
        "serverBuild": "patched",
        "cliBuild": "patched",
        "importsBuild": "patched",
    },
    "submit": "workflow-id",
    "status": QE_STATUS_RESPONSE,
    "status_compressed": QE_STATUS_RESPONSE_COMPRESSED,
    "status_not_ready": QE_STATUS_RESPONSE_NOT_READY,
    "status_running": QE_STATUS_RESPONSE_RUNNING,
    "status_waiting_task": QE_STATUS_RESPONSE_WAITING_TASK,
    "status_terminated": QE_STATUS_RESPONSE_TERMINATED,
    "stop": " ",
    "logs": {"logs": "a couple of mocked out lines\njoined with newline separator"},
    "logs_empty": {"logs": ""},
    "logs_jsonl": {
        "logs": "\n".join(
            [
                json.dumps({"log": line})
                for line in ["sample log line", "another log entry in JSONL format"]
            ]
        )
    },
    "list": [
        {
            "_ulid": "01FAQVX4390WK7EAW2XC6VR5JX",
            "completed": True,
            "created": "1989-12-13T09:03:49.39478764Z",
            "currentRepresentation": json.dumps(QE_MINIMAL_CURRENT_REPRESENTATION),
            "id": "hello-there-abc123-r000",
            "lastModified": "1989-12-13T09:10:04.14422796Z",
            "owner": "philip.carinhas@zapatacomputing.com",
            "retry": "",
            "status": "Succeeded",
            "target": "argo",
            "version": "io.orquestra.workflow/1.0.0",
        },
    ],
    "results_bytes": QE_WORKFLOW_RESULT_BYTES,
}


TEST_WORKFLOW = my_workflow.model


@pytest.fixture
def mocked_responses():
    # This is the place to tap into response mocking machinery, if needed.
    with responses.RequestsMock() as mocked_responses:
        yield mocked_responses


@pytest.fixture
def runtime(tmp_path, mock_workflow_db_location):
    (tmp_path / ".orquestra").mkdir(exist_ok=True)
    # Fake QE configuration
    config = RuntimeConfiguration(
        config_name="hello",
        runtime_name=RuntimeName.QE_REMOTE,
        runtime_options={"uri": "http://localhost", "token": "blah"},
    )
    # Return a runtime object
    return _qe_runtime.QERuntime(config, tmp_path)


@pytest.fixture
def runtime_verbose(tmp_path):
    (tmp_path / ".orquestra").mkdir(exist_ok=True)
    # Fake QE configuration
    config = RuntimeConfiguration(
        config_name="hello",
        runtime_name=RuntimeName.QE_REMOTE,
        runtime_options={"uri": "http://localhost", "token": "blah"},
    )
    # Return a runtime object
    return _qe_runtime.QERuntime(config, tmp_path, True)


class TestInitialization:
    @pytest.mark.parametrize("proj_dir", [".", Path(".")])
    @pytest.mark.parametrize("verbose", [True, False])
    def test_passing_project_dir_and_config_obj(self, proj_dir, verbose):
        """
        - GIVEN user is calling QERuntime()
        - WHEN project_dir and config arguments are provided
        - THEN a RuntimeConfiguration object is returned whose properties match
          those of the configuration returned from calling
          from_runtime_configuration() with the same parameters
        """
        config = RuntimeConfiguration(
            config_name="hello",
            runtime_name=RuntimeName.QE_REMOTE,
            runtime_options={"uri": "http://localhost", "token": "blah"},
        )

        # when
        rt = _qe_runtime.QERuntime(config=config, project_dir=proj_dir, verbose=verbose)

        # then
        rt2 = _qe_runtime.QERuntime.from_runtime_configuration(
            project_dir=proj_dir, config=config, verbose=verbose
        )
        assert rt._config == rt2._config
        assert rt._project_dir == rt2._project_dir
        assert rt._verbose == rt2._verbose


HELLO_CONFIG = RuntimeConfiguration(
    config_name="hello",
    runtime_name=RuntimeName.QE_REMOTE,
    runtime_options={"uri": "http://localhost", "token": "blah"},
)


class TestGeneral:
    def test_invalid_config(self, monkeypatch):
        config = RuntimeConfiguration(
            config_name="hello",
            runtime_name=RuntimeName.QE_REMOTE,
            runtime_options={},
        )
        with pytest.raises(exceptions.RuntimeConfigError):
            _qe_runtime.QERuntime(config, Path("shouldnt_matter"))


class TestCreateWorkflowRun:
    def test_happy_path(self, monkeypatch, runtime, mocked_responses):
        _save_workflow_run = Mock()
        monkeypatch.setattr(_db.WorkflowDB, "save_workflow_run", _save_workflow_run)
        mocked_responses.add(
            responses.POST,
            "http://localhost/v1/workflows",
            body=QE_RESPONSES["submit"],
        )

        result = runtime.create_workflow_run(TEST_WORKFLOW)

        _save_workflow_run.assert_called_once()
        assert result == "workflow-id"

    @staticmethod
    def test_exception_on_bad_request_too_large(monkeypatch, runtime, mocked_responses):
        _save_workflow_run = Mock()
        monkeypatch.setattr(_db.WorkflowDB, "save_workflow_run", _save_workflow_run)
        mocked_responses.add(
            responses.POST,
            "http://localhost/v1/workflows",
            status=400,
            body="Failed to run workflow : Request entity too large: limit is 3145728",
        )

        with pytest.raises(exceptions.WorkflowTooLargeError) as exc_info:
            _ = runtime.create_workflow_run(TEST_WORKFLOW)

        _save_workflow_run.assert_not_called()
        assert "The submitted workflow is too large to be run on this cluster." in str(
            exc_info
        )

    @staticmethod
    def test_exception_on_bad_request_passthrough(
        monkeypatch, runtime, mocked_responses
    ):
        _save_workflow_run = Mock()
        monkeypatch.setattr(_db.WorkflowDB, "save_workflow_run", _save_workflow_run)
        mocked_responses.add(
            responses.POST,
            "http://localhost/v1/workflows",
            status=400,
            body="some other submission failure",
        )

        with pytest.raises(exceptions.WorkflowSyntaxError) as exc_info:
            _ = runtime.create_workflow_run(TEST_WORKFLOW)

        _save_workflow_run.assert_not_called()
        assert "some other submission failure" in str(exc_info)

    @staticmethod
    @pytest.mark.parametrize(
        "workflow_name",
        [
            "ContainsUppercaseCharacters",
            "-starts-with.non-alphanumeric",
            "ends-with-non.alphanumeric-",
        ],
    )
    def test_raises_exception_if_worflow_name_does_not_match_qe_reqs(
        workflow_name,
        runtime,
    ):
        @sdk.workflow(custom_name=workflow_name)
        def BadNameWorkflow():
            return [None]

        workflow = BadNameWorkflow.model

        with pytest.raises(exceptions.InvalidWorkflowDefinitionError) as exc_info:
            runtime.create_workflow_run(workflow)

        assert f'Workflow name "{workflow_name}" is invalid' in str(exc_info)

    def test_yaml_to_stderr(
        self, monkeypatch, runtime_verbose, runtime, mocked_responses, capsys
    ):
        _save_workflow_run = Mock()
        monkeypatch.setattr(_db.WorkflowDB, "save_workflow_run", _save_workflow_run)
        mocked_responses.add(
            responses.POST,
            "http://localhost/v1/workflows",
            body=QE_RESPONSES["submit"],
        )
        _ = runtime.create_workflow_run(TEST_WORKFLOW)
        captured_without_yaml = capsys.readouterr()
        _ = runtime_verbose.create_workflow_run(TEST_WORKFLOW)
        captured_with_yaml = capsys.readouterr()
        yaml_wf = pydantic_to_yaml(workflow_to_yaml(TEST_WORKFLOW))
        assert yaml_wf in captured_with_yaml.err
        assert yaml_wf not in captured_without_yaml.err


class TestGetAllWorkflowRunsStatus:
    def test_empty_db(self, monkeypatch, runtime, mocked_responses):
        # Testing an empty local DB. We should ignore workflows from QE
        _get_workflow_runs_list = Mock(return_value=[])
        monkeypatch.setattr(
            _db.WorkflowDB, "get_workflow_runs_list", _get_workflow_runs_list
        )
        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/workflowlist",
            json=QE_RESPONSES["list"],
        )

        result = runtime.get_all_workflow_runs_status()

        assert result == []

    def test_with_local_runs(self, monkeypatch, runtime, mocked_responses):
        # Mock the local DB with a local run
        _get_workflow_runs_list = Mock(
            return_value=[
                StoredWorkflowRun(
                    workflow_run_id="hello-there-abc123-r000",  # noqa: E501
                    config_name="hello",
                    workflow_def=TEST_WORKFLOW,
                )
            ]
        )
        monkeypatch.setattr(
            _db.WorkflowDB, "get_workflow_runs_list", _get_workflow_runs_list
        )
        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/workflowlist",
            json=QE_RESPONSES["list"],
        )

        result = runtime.get_all_workflow_runs_status()

        assert result == [
            WorkflowRun(
                id="hello-there-abc123-r000",
                workflow_def=TEST_WORKFLOW,
                task_runs=[
                    TaskRun(
                        id="hello-there-abc123-r000-2738763496",
                        invocation_id="invocation-1-task-multi-output-test",
                        status=RunStatus(
                            state=State.SUCCEEDED,
                            start_time=datetime.datetime(
                                1989, 12, 13, 9, 3, 49, tzinfo=datetime.timezone.utc
                            ),
                            end_time=datetime.datetime(
                                1989, 12, 13, 9, 4, 28, tzinfo=datetime.timezone.utc
                            ),
                        ),
                    ),
                    TaskRun(
                        id="hello-there-abc123-r000-3825957270",
                        invocation_id=("invocation-0-task-make-greeting-message"),
                        status=RunStatus(
                            state=State.SUCCEEDED,
                            start_time=datetime.datetime(
                                1989, 12, 13, 9, 4, 29, tzinfo=datetime.timezone.utc
                            ),
                            end_time=datetime.datetime(
                                1989, 12, 13, 9, 5, 8, tzinfo=datetime.timezone.utc
                            ),
                        ),
                    ),
                ],
                status=RunStatus(
                    state=State.SUCCEEDED,
                    start_time=datetime.datetime(
                        1989, 12, 13, 9, 3, 49, tzinfo=datetime.timezone.utc
                    ),
                    end_time=datetime.datetime(
                        1989, 12, 13, 9, 5, 14, tzinfo=datetime.timezone.utc
                    ),
                ),
            )
        ]


def _mock_artifact_resp(
    responses,
    wf_run_id: WorkflowRunId,
    inv_id: TaskInvocationId,
    art_id: ArtifactNodeId,
    result_model: JSONResult,
):
    responses.add(
        responses.GET,
        f"http://localhost/v2/workflows/{wf_run_id}/step/{inv_id}/artifact/{art_id}",
        content_type="application/x-gtar-compressed",
        body=_make_result_bytes(dict(result_model)),
    )


class TestGetAvailableOutputs:
    def test_successful_workflow(self, monkeypatch, runtime, mocked_responses):
        wf_run_id = "hello-there-abc123-r000"
        _get_workflow_run = Mock(
            return_value=StoredWorkflowRun(
                workflow_run_id=wf_run_id,
                config_name="hello",
                workflow_def=TEST_WORKFLOW,
            )
        )
        monkeypatch.setattr(_db.WorkflowDB, "get_workflow_run", _get_workflow_run)

        _mock_artifact_resp(
            mocked_responses,
            result_model=JSONResult(value='"hello, alex zapata!there"'),
            wf_run_id=wf_run_id,
            inv_id="invocation-0-task-make-greeting-message",
            art_id="artifact-0-make-greeting-message",
        )
        _mock_artifact_resp(
            mocked_responses,
            result_model=JSONResult(value='"hello"'),
            wf_run_id=wf_run_id,
            inv_id="invocation-1-task-multi-output-test",
            art_id="artifact-1-multi-output-test",
        )
        _mock_artifact_resp(
            mocked_responses,
            result_model=JSONResult(value='"there"'),
            wf_run_id=wf_run_id,
            inv_id="invocation-1-task-multi-output-test",
            art_id="artifact-2-multi-output-test",
        )

        result = runtime.get_available_outputs("hello-there-abc123-r000")

        assert result == {
            "invocation-0-task-make-greeting-message": ("hello, alex zapata!there",),
            "invocation-1-task-multi-output-test": ("hello", "there"),
        }

    def test_failed_workflow(self, monkeypatch, runtime, mocked_responses):
        wf_run_id = "wf-3fmte-r000"
        _get_workflow_run = Mock(
            return_value=StoredWorkflowRun(
                workflow_run_id=wf_run_id,
                config_name="hello",
                workflow_def=TEST_WORKFLOW,
            )
        )
        monkeypatch.setattr(_db.WorkflowDB, "get_workflow_run", _get_workflow_run)

        _mock_artifact_resp(
            mocked_responses,
            wf_run_id=wf_run_id,
            inv_id="invocation-0-task-make-greeting-message",
            art_id="artifact-0-make-greeting-message",
            result_model=JSONResult(value='"hello, alex zapata!there"'),
        )
        mocked_responses.add(
            responses.GET,
            "http://localhost/v2/workflows/wf-3fmte-r000/step/invocation-1-task-multi-output-test/artifact/artifact-1-multi-output-test",  # noqa
            status=404,
            json={
                "meta": {},
                "message": "not found",
            },
        )

        result = runtime.get_available_outputs("wf-3fmte-r000")
        # There should be a result for 1 task that finish successfully
        assert len(result) == 1
        assert result["invocation-0-task-make-greeting-message"] == (
            "hello, alex zapata!there",
        )


class TestGetWorkflowRunStatus:
    def test_happy_path(self, monkeypatch, runtime, mocked_responses):
        _get_workflow_run = Mock(
            return_value=StoredWorkflowRun(
                workflow_run_id="hello-there-abc123-r000",
                config_name="hello",
                workflow_def=TEST_WORKFLOW,
            )
        )
        monkeypatch.setattr(_db.WorkflowDB, "get_workflow_run", _get_workflow_run)
        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/workflow",
            json=QE_RESPONSES["status"],
        )

        result = runtime.get_workflow_run_status("hello-there-abc123-r000")

        assert result == WorkflowRun(
            id="hello-there-abc123-r000",
            workflow_def=TEST_WORKFLOW,
            task_runs=[
                TaskRun(
                    id="hello-there-abc123-r000-2738763496",
                    invocation_id="invocation-1-task-multi-output-test",
                    status=RunStatus(
                        state=State.SUCCEEDED,
                        start_time=datetime.datetime(
                            1989, 12, 13, 9, 3, 49, tzinfo=datetime.timezone.utc
                        ),
                        end_time=datetime.datetime(
                            1989, 12, 13, 9, 4, 28, tzinfo=datetime.timezone.utc
                        ),
                    ),
                ),
                TaskRun(
                    id="hello-there-abc123-r000-3825957270",
                    invocation_id="invocation-0-task-make-greeting-message",
                    status=RunStatus(
                        state=State.SUCCEEDED,
                        start_time=datetime.datetime(
                            1989, 12, 13, 9, 4, 29, tzinfo=datetime.timezone.utc
                        ),
                        end_time=datetime.datetime(
                            1989, 12, 13, 9, 5, 8, tzinfo=datetime.timezone.utc
                        ),
                    ),
                ),
            ],
            status=RunStatus(
                state=State.SUCCEEDED,
                start_time=datetime.datetime(
                    1989, 12, 13, 9, 3, 49, tzinfo=datetime.timezone.utc
                ),
                end_time=datetime.datetime(
                    1989, 12, 13, 9, 5, 14, tzinfo=datetime.timezone.utc
                ),
            ),
        )

    def test_get_workflow_run_status_fails(self, mock_workflow_db_location):
        # Fake QE configuration
        config = RuntimeConfiguration(
            config_name="hello",
            runtime_name=RuntimeName.QE_REMOTE,
            runtime_options={"uri": "http://localhost", "token": "blah"},
        )
        # Return a runtime object
        runtime = _qe_runtime.QERuntime(config, Path("Nothing"))

        stub_runID = "hello-there-abc123-r000"
        with pytest.raises(exceptions.WorkflowNotFoundError) as exc_info:
            _ = runtime.get_workflow_run_status(stub_runID)

        assert stub_runID in str(exc_info)

    def test_task_waiting_with_message(self, monkeypatch, runtime, mocked_responses):
        _get_workflow_run = Mock(
            return_value=StoredWorkflowRun(
                workflow_run_id="hello-there-abc123-r000",
                config_name="hello",
                workflow_def=TEST_WORKFLOW,
            )
        )
        monkeypatch.setattr(_db.WorkflowDB, "get_workflow_run", _get_workflow_run)
        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/workflow",
            json=QE_RESPONSES["status_waiting_task"],
        )

        result = runtime.get_workflow_run_status("hello-there-abc123-r000")

        assert len(result.task_runs) == 2
        assert "Unschedulable: 0/8 nodes are available" in result.task_runs[1].message

    def test_workflow_terminated(self, monkeypatch, runtime, mocked_responses):
        """This tests for when QE and Argo statuses get out of sync"""
        # Given
        _get_workflow_run = Mock(
            return_value=StoredWorkflowRun(
                workflow_run_id="hello-there-abc123-r000",
                config_name="hello",
                workflow_def=TEST_WORKFLOW,
            )
        )
        monkeypatch.setattr(_db.WorkflowDB, "get_workflow_run", _get_workflow_run)
        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/workflow",
            json=QE_RESPONSES["status_terminated"],
        )

        # When
        result = runtime.get_workflow_run_status("hello-there-abc123-r000")

        # Then
        task_run_states = [task.status.state for task in result.task_runs]
        # There should be two task runs
        assert len(result.task_runs) == 2
        # One task run completed and should be SUCCEEDED
        assert State.SUCCEEDED in task_run_states
        # One task run was running and should be TERMINATED
        assert State.TERMINATED in task_run_states
        # The overall workflow state should be TERMINATED
        assert result.status.state == State.TERMINATED

    def test_compressedNodes_is_nodes(self, monkeypatch, runtime, mocked_responses):
        _get_workflow_run = Mock(
            return_value=StoredWorkflowRun(
                workflow_run_id="hello-there-abc123-r000",
                config_name="hello",
                workflow_def=TEST_WORKFLOW,
            )
        )
        monkeypatch.setattr(_db.WorkflowDB, "get_workflow_run", _get_workflow_run)
        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/workflow",
            json=QE_RESPONSES["status_compressed"],
        )

        compressed_result = runtime.get_workflow_run_status("hello-there-abc123-r000")

        assert compressed_result == WorkflowRun(
            id="hello-there-abc123-r000",
            workflow_def=TEST_WORKFLOW,
            task_runs=[
                TaskRun(
                    id="hello-there-abc123-r000-2738763496",
                    invocation_id="invocation-1-task-multi-output-test",
                    status=RunStatus(
                        state=State.SUCCEEDED,
                        start_time=datetime.datetime(
                            1989, 12, 13, 9, 3, 49, tzinfo=datetime.timezone.utc
                        ),
                        end_time=datetime.datetime(
                            1989, 12, 13, 9, 4, 28, tzinfo=datetime.timezone.utc
                        ),
                    ),
                ),
                TaskRun(
                    id="hello-there-abc123-r000-3825957270",
                    invocation_id="invocation-0-task-make-greeting-message",
                    status=RunStatus(
                        state=State.SUCCEEDED,
                        start_time=datetime.datetime(
                            1989, 12, 13, 9, 4, 29, tzinfo=datetime.timezone.utc
                        ),
                        end_time=datetime.datetime(
                            1989, 12, 13, 9, 5, 8, tzinfo=datetime.timezone.utc
                        ),
                    ),
                ),
            ],
            status=RunStatus(
                state=State.SUCCEEDED,
                start_time=datetime.datetime(
                    1989, 12, 13, 9, 3, 49, tzinfo=datetime.timezone.utc
                ),
                end_time=datetime.datetime(
                    1989, 12, 13, 9, 5, 14, tzinfo=datetime.timezone.utc
                ),
            ),
        )

    def test_not_ready(self, monkeypatch, runtime, mocked_responses):
        _get_workflow_run = Mock(
            return_value=StoredWorkflowRun(
                workflow_run_id="hello-there-abc123-r000",
                config_name="hello",
                workflow_def=TEST_WORKFLOW,
            )
        )
        monkeypatch.setattr(_db.WorkflowDB, "get_workflow_run", _get_workflow_run)
        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/workflow",
            json=QE_RESPONSES["status_not_ready"],
        )

        result = runtime.get_workflow_run_status("hello-there-abc123-r000")

        assert result == WorkflowRun(
            id="hello-there-abc123-r000",
            workflow_def=TEST_WORKFLOW,
            task_runs=[],
            status=RunStatus(
                state=State.WAITING,
                start_time=None,
                end_time=None,
            ),
        )


class TestGetWorkflowRunOutputs:
    def test_raises(self, runtime):
        with pytest.raises(NotImplementedError):
            runtime.get_workflow_run_outputs("hello-there-abc123-r000")


class TestGetWorkflowRunOutputsNonBlocking:
    def test_happy_path(self, monkeypatch, runtime, mocked_responses):
        _get_workflow_run = Mock(
            return_value=StoredWorkflowRun(
                workflow_run_id="hello-there-abc123-r000",
                config_name="hello",
                workflow_def=TEST_WORKFLOW,
            )
        )
        monkeypatch.setattr(_db.WorkflowDB, "get_workflow_run", _get_workflow_run)
        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/workflow",
            json=QE_RESPONSES["status"],
        )
        mocked_responses.add(
            responses.GET,
            "http://localhost/v2/workflows/hello-there-abc123-r000/result",
            content_type="application/x-gtar-compressed",
            body=QE_RESPONSES["results_bytes"],
        )

        result = runtime.get_workflow_run_outputs_non_blocking(
            "hello-there-abc123-r000"
        )

        assert result == ("hello, alex zapata!there",)

    def test_get_workflow_run_outputs_non_blocking_fails(
        self, mock_workflow_db_location
    ):
        # Fake QE configuration
        config = RuntimeConfiguration(
            config_name="hello",
            runtime_name=RuntimeName.QE_REMOTE,
            runtime_options={"uri": "http://localhost", "token": "blah"},
        )
        # Return a runtime object
        runtime = _qe_runtime.QERuntime(config, Path("Nothing"))

        stub_runID = "hello-there-abc123-r000"
        with pytest.raises(exceptions.WorkflowNotFoundError) as exc_info:
            _ = runtime.get_workflow_run_outputs_non_blocking(stub_runID)
        assert stub_runID in str(exc_info)

    def test_still_running(self, monkeypatch, runtime, mocked_responses):
        _get_workflow_run = Mock(
            return_value=StoredWorkflowRun(
                workflow_run_id="hello-there-abc123-r000",
                config_name="hello",
                workflow_def=TEST_WORKFLOW,
            )
        )
        monkeypatch.setattr(_db.WorkflowDB, "get_workflow_run", _get_workflow_run)

        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/workflow",
            json=QE_RESPONSES["status_running"],
        )

        with pytest.raises(exceptions.WorkflowRunNotSucceeded):
            _ = runtime.get_workflow_run_outputs_non_blocking("hello-there-abc123-r000")

    def test_empty(self, monkeypatch, runtime, mocked_responses):
        _get_workflow_run = Mock(
            return_value=StoredWorkflowRun(
                workflow_run_id="hello-there-abc123-r000",
                config_name="hello",
                workflow_def=TEST_WORKFLOW,
            )
        )
        _empty_tgz = Mock(side_effect=IndexError)
        monkeypatch.setattr(_db.WorkflowDB, "get_workflow_run", _get_workflow_run)
        monkeypatch.setattr(_qe_runtime, "extract_result_json", _empty_tgz)
        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/workflow",
            json=QE_RESPONSES["status"],
        )
        mocked_responses.add(
            responses.GET,
            "http://localhost/v2/workflows/hello-there-abc123-r000/result",
            content_type="application/x-gtar-compressed",
        )
        with pytest.raises(exceptions.NotFoundError):
            _ = runtime.get_workflow_run_outputs_non_blocking("hello-there-abc123-r000")

    def test_wrong_format(self, monkeypatch, runtime, mocked_responses):
        _get_workflow_run = Mock(
            return_value=StoredWorkflowRun(
                workflow_run_id="hello-there-abc123-r000",
                config_name="hello",
                workflow_def=TEST_WORKFLOW,
            )
        )
        monkeypatch.setattr(_db.WorkflowDB, "get_workflow_run", _get_workflow_run)
        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/workflow",
            json=QE_RESPONSES["status"],
        )
        mocked_responses.add(
            responses.GET,
            "http://localhost/v2/workflows/hello-there-abc123-r000/result",
            body="oops, all text!",
        )
        with pytest.raises(ValueError):
            _ = runtime.get_workflow_run_outputs_non_blocking("hello-there-abc123-r000")

    def test_not_found(self, monkeypatch, runtime, mocked_responses):
        _get_workflow_run = Mock(
            return_value=StoredWorkflowRun(
                workflow_run_id="hello-there-abc123-r000",
                config_name="hello",
                workflow_def=TEST_WORKFLOW,
            )
        )
        monkeypatch.setattr(_db.WorkflowDB, "get_workflow_run", _get_workflow_run)
        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/workflow",
            json=QE_RESPONSES["status"],
        )
        mocked_responses.add(
            responses.GET,
            "http://localhost/v2/workflows/hello-there-abc123-r000/result",
            status=404,
            json={
                "meta": {},
                "message": "not found",
            },
        )
        with pytest.raises(exceptions.NotFoundError):
            _ = runtime.get_workflow_run_outputs_non_blocking("hello-there-abc123-r000")


class TestGetWorkflowLogs:
    def test_happy_flow(self, monkeypatch, runtime, mocked_responses):
        # Given
        wf_run_id = "hello-there-abc123-r000"
        _get_workflow_run = Mock(
            return_value=StoredWorkflowRun(
                workflow_run_id=wf_run_id,
                config_name="hello",
                workflow_def=TEST_WORKFLOW,
            )
        )
        monkeypatch.setattr(_db.WorkflowDB, "get_workflow_run", _get_workflow_run)
        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/workflow",
            json=QE_RESPONSES["status"],
        )
        mocked_responses.add(
            responses.GET,
            f"http://localhost/v1/logs/{wf_run_id}",
            json=QE_RESPONSES["logs"],
        )

        # When
        logs = runtime.get_workflow_logs(wf_run_id)

        # Then
        assert logs == {
            "invocation-1-task-multi-output-test": [
                "a couple of mocked out lines",
                "joined with newline separator",
            ],
            "invocation-0-task-make-greeting-message": [
                "a couple of mocked out lines",
                "joined with newline separator",
            ],
        }

    def test_invalid_id(self, monkeypatch, runtime, mocked_responses):
        _get_workflow_run = Mock(side_effect=exceptions.NotFoundError)
        monkeypatch.setattr(_db.WorkflowDB, "get_workflow_run", _get_workflow_run)

        with pytest.raises(exceptions.NotFoundError):
            runtime.get_workflow_logs("notvalidqetask")


class TestGetTaskLogs:
    def test_plain_string_logs(self, monkeypatch, runtime, mocked_responses):
        # Given
        wf_run_id = "hello-there-abc123-r000"
        task_run_id = f"{wf_run_id}-3825957270"
        task_inv_id = "invocation-0-task-make-greeting-message"

        _get_workflow_run = Mock(
            return_value=StoredWorkflowRun(
                workflow_run_id=wf_run_id,
                config_name="hello",
                workflow_def=TEST_WORKFLOW,
            )
        )
        monkeypatch.setattr(_db.WorkflowDB, "get_workflow_run", _get_workflow_run)
        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/workflow",
            json=QE_RESPONSES["status"],
        )
        mocked_responses.add(
            responses.GET,
            f"http://localhost/v1/logs/{wf_run_id}?stepname={task_run_id}",
            json=QE_RESPONSES["logs_jsonl"],
        )

        # When
        logs = runtime.get_task_logs(wf_run_id, task_inv_id)

        # Then
        assert logs == [
            "sample log line",
            "another log entry in JSONL format",
        ]

    def test_json_logs(self, monkeypatch, runtime, mocked_responses):
        wf_run_id = "hello-there-abc123-r000"
        task_run_id = f"{wf_run_id}-3825957270"
        task_inv_id = "invocation-0-task-make-greeting-message"

        _get_workflow_run = Mock(
            return_value=StoredWorkflowRun(
                workflow_run_id=wf_run_id,
                config_name="hello",
                workflow_def=TEST_WORKFLOW,
            )
        )
        monkeypatch.setattr(_db.WorkflowDB, "get_workflow_run", _get_workflow_run)
        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/workflow",
            json=QE_RESPONSES["status"],
        )
        mocked_responses.add(
            responses.GET,
            f"http://localhost/v1/logs/{wf_run_id}?stepname={task_run_id}",
            json=QE_RESPONSES["logs"],
        )

        # When
        logs = runtime.get_task_logs(wf_run_id, task_inv_id)

        # Then
        assert logs == [
            "a couple of mocked out lines",
            "joined with newline separator",
        ]

    def test_wf_missing_from_db(self, monkeypatch, runtime, mocked_responses):
        # Given
        wf_run_id = "shouldn't matter"
        task_inv_id = "shouldn't matter either"

        _get_workflow_run = Mock(side_effect=exceptions.WorkflowNotFoundError)
        monkeypatch.setattr(_db.WorkflowDB, "get_workflow_run", _get_workflow_run)

        # Then
        with pytest.raises(exceptions.WorkflowNotFoundError):
            # When
            _ = runtime.get_task_logs(wf_run_id, task_inv_id)

    def test_task_inv_missing_from_wf(self, monkeypatch, runtime, mocked_responses):
        wf_run_id = "hello-there-abc123-r000"
        task_inv_id = "doesn't exist"

        _get_workflow_run = Mock(
            return_value=StoredWorkflowRun(
                workflow_run_id=wf_run_id,
                config_name="hello",
                workflow_def=TEST_WORKFLOW,
            )
        )
        monkeypatch.setattr(_db.WorkflowDB, "get_workflow_run", _get_workflow_run)
        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/workflow",
            json=QE_RESPONSES["status"],
        )

        with pytest.raises(exceptions.TaskInvocationNotFoundError):
            _ = runtime.get_task_logs(wf_run_id, task_inv_id)

    def test_wf_missing_on_qe(self, monkeypatch, runtime, mocked_responses):
        wf_run_id = "hello-there-abc123-r000"
        task_run_id = "hello-there-abc123-r000-3825957270"
        task_inv_id = "invocation-0-task-make-greeting-message"

        _get_workflow_run = Mock(
            return_value=StoredWorkflowRun(
                workflow_run_id=wf_run_id,
                config_name="hello",
                workflow_def=TEST_WORKFLOW,
            )
        )
        monkeypatch.setattr(_db.WorkflowDB, "get_workflow_run", _get_workflow_run)
        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/workflow",
            json=QE_RESPONSES["status"],
        )
        mocked_responses.add(
            responses.GET,
            f"http://localhost/v1/logs/{wf_run_id}?stepname={task_run_id}",
            status=400,
        )

        with pytest.raises(exceptions.NotFoundError):
            _ = runtime.get_task_logs(wf_run_id, task_inv_id)


class TestStopWorkflowRun:
    def test_invalid_run_id(self, monkeypatch, runtime, mocked_responses):
        with pytest.raises(exceptions.WorkflowRunCanNotBeTerminated):
            runtime.stop_workflow_run("notvalidqeworkflow")

    def test_cannot_be_stopped(self, monkeypatch, runtime, mocked_responses):
        with pytest.raises(exceptions.WorkflowRunCanNotBeTerminated):
            runtime.stop_workflow_run("hello-there-abc123-r000")

    def test_happy_path(self, monkeypatch, runtime, mocked_responses):
        mocked_responses.add(
            responses.DELETE,
            "http://localhost/v1/workflows/hello-there-abc123-r000",
            json=QE_RESPONSES["stop"],
        )

        response = runtime.stop_workflow_run("hello-there-abc123-r000")

        assert response is None


class TestListWorkflowRuns:
    @pytest.fixture
    def mock_local_db(self, monkeypatch):
        get_workflow_runs_list = Mock(
            return_value=[
                StoredWorkflowRun(
                    workflow_run_id="hello-there-abc123-r000",
                    config_name="hello",
                    workflow_def=TEST_WORKFLOW,
                ),
                StoredWorkflowRun(
                    workflow_run_id="hello-there-abc123-r000",
                    config_name="hello",
                    workflow_def=TEST_WORKFLOW,
                ),
                StoredWorkflowRun(
                    workflow_run_id="hello-there-abc123-r000",
                    config_name="hello",
                    workflow_def=TEST_WORKFLOW,
                ),
                StoredWorkflowRun(
                    workflow_run_id="hello-there-abc123-r000",
                    config_name="hello",
                    workflow_def=TEST_WORKFLOW,
                ),
            ]
        )
        monkeypatch.setattr(
            _db.WorkflowDB, "get_workflow_runs_list", get_workflow_runs_list
        )
        return get_workflow_runs_list

    def test_happy_path(
        self, runtime, mock_workflow_db_location, mock_local_db, monkeypatch
    ):
        # Given
        mock_status = MagicMock()
        monkeypatch.setattr(
            runtime, "get_workflow_run_status", MagicMock(return_value=mock_status)
        )
        # When
        runs = runtime.list_workflow_runs()
        # Then
        assert len(runs) == 4

    def test_missing_wf_in_db(self, runtime, mock_workflow_db_location, monkeypatch):
        # Given
        get_workflow_runs_list = Mock(return_value=[])
        monkeypatch.setattr(
            _db.WorkflowDB, "get_workflow_runs_list", get_workflow_runs_list
        )
        # When
        runs = runtime.list_workflow_runs()
        # Then
        assert len(runs) == 0

    def test_missing_wf_in_runtime(
        self, runtime, mock_workflow_db_location, mock_local_db, monkeypatch
    ):
        # Given
        monkeypatch.setattr(
            runtime,
            "get_workflow_run_status",
            MagicMock(side_effect=exceptions.WorkflowRunNotFoundError),
        )
        # When
        runs = runtime.list_workflow_runs()
        # Then
        assert len(runs) == 0

    def test_with_state(
        self, runtime, mock_workflow_db_location, mock_local_db, monkeypatch
    ):
        # Given
        mock_status = MagicMock()
        type(mock_status.status).state = PropertyMock(
            side_effect=[
                State.RUNNING,
                State.SUCCEEDED,
                State.SUCCEEDED,
                State.RUNNING,
            ]
        )
        monkeypatch.setattr(
            runtime, "get_workflow_run_status", MagicMock(return_value=mock_status)
        )
        # When
        runs = runtime.list_workflow_runs(state=State.RUNNING)
        # Then
        assert len(runs) == 2

    def test_with_max_age(
        self, runtime, mock_workflow_db_location, mock_local_db, monkeypatch
    ):
        # Given
        mock_status = MagicMock()
        type(mock_status.status).start_time = PropertyMock(
            side_effect=[
                None,
                datetime.datetime.now(datetime.timezone.utc)
                - datetime.timedelta(seconds=5),
                datetime.datetime.now(datetime.timezone.utc)
                - datetime.timedelta(seconds=5),
                datetime.datetime.now(datetime.timezone.utc)
                - datetime.timedelta(days=4),
            ]
        )
        monkeypatch.setattr(
            runtime, "get_workflow_run_status", MagicMock(return_value=mock_status)
        )
        # When
        runs = runtime.list_workflow_runs(max_age=datetime.timedelta(minutes=2))
        # Then
        assert len(runs) == 3

    def test_with_limit(
        self, runtime, mock_workflow_db_location, mock_local_db, monkeypatch
    ):
        # Given
        mock_status = MagicMock()
        type(mock_status.status).start_time = PropertyMock(
            side_effect=[
                None,
                datetime.datetime.now(datetime.timezone.utc)
                - datetime.timedelta(seconds=5),
                datetime.datetime.now(datetime.timezone.utc)
                - datetime.timedelta(seconds=5),
                datetime.datetime.now(datetime.timezone.utc)
                - datetime.timedelta(days=4),
            ]
        )
        monkeypatch.setattr(
            runtime, "get_workflow_run_status", MagicMock(return_value=mock_status)
        )
        # When
        runs = runtime.list_workflow_runs(limit=2)
        # Then
        assert len(runs) == 2


@pytest.mark.parametrize(
    "error_code, expected_exception, telltales",
    [
        (401, exceptions.UnauthorizedError, []),
        (
            413,
            exceptions.WorkflowTooLargeError,
            ["The submitted workflow is too large to be run on this cluster."],
        ),
    ],
)
class TestHTTPErrors:
    def test_create_workflow_run(
        self,
        error_code,
        expected_exception,
        telltales,
        monkeypatch,
        runtime,
        mocked_responses,
    ):
        _save_workflow_run = Mock()
        monkeypatch.setattr(_db.WorkflowDB, "save_workflow_run", _save_workflow_run)
        mocked_responses.add(
            responses.POST,
            "http://localhost/v1/workflows",
            status=error_code,
        )
        with pytest.raises(expected_exception) as exc_info:
            runtime.create_workflow_run(TEST_WORKFLOW)
        for telltale in telltales:
            assert telltale in str(exc_info)
        _save_workflow_run.assert_not_called()

    def test_get_workflow_run_status(
        self,
        error_code,
        expected_exception,
        telltales,
        monkeypatch,
        runtime,
        mocked_responses,
    ):
        _get_workflow_run = Mock(
            return_value=StoredWorkflowRun(
                workflow_run_id="hello-there-abc123-r000",
                config_name="hello",
                workflow_def=TEST_WORKFLOW,
            )
        )
        monkeypatch.setattr(_db.WorkflowDB, "get_workflow_run", _get_workflow_run)
        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/workflow",
            status=error_code,
        )
        with pytest.raises(expected_exception) as exc_info:
            runtime.get_workflow_run_status("hello-there-abc123-r000")
        for telltale in telltales:
            assert telltale in str(exc_info)

    def test_get_all_workflow_run_status(
        self,
        error_code,
        expected_exception,
        telltales,
        monkeypatch,
        runtime,
        mocked_responses,
    ):
        monkeypatch.setattr(
            _db.WorkflowDB,
            "get_workflow_runs_list",
            Mock(
                return_value=[
                    StoredWorkflowRun(
                        workflow_run_id="hello-there-abc123-r000",
                        config_name="hello",
                        workflow_def=TEST_WORKFLOW,
                    )
                ]
            ),
        )
        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/workflowlist",
            status=error_code,
        )
        with pytest.raises(expected_exception) as exc_info:
            runtime.get_all_workflow_runs_status()
        for telltale in telltales:
            assert telltale in str(exc_info)

    def test_get_workflow_run_outputs_non_blocking(
        self,
        error_code,
        expected_exception,
        telltales,
        monkeypatch,
        runtime,
        mocked_responses,
    ):
        monkeypatch.setattr(
            _db.WorkflowDB,
            "get_workflow_run",
            Mock(
                return_value=StoredWorkflowRun(
                    workflow_run_id="hello-there-abc123-r000",
                    config_name="hello",
                    workflow_def=TEST_WORKFLOW,
                )
            ),
        )
        mocked_responses.add(
            responses.GET,
            "http://localhost/v1/workflow",
            status=error_code,
        )

        with pytest.raises(expected_exception) as exc_info:
            _ = runtime.get_workflow_run_outputs_non_blocking("hello-there-abc123-r000")
        for telltale in telltales:
            assert telltale in str(exc_info)

    def test_stop_workflow_run(
        self,
        error_code,
        expected_exception,
        telltales,
        monkeypatch,
        runtime,
        mocked_responses,
    ):
        mocked_responses.add(
            responses.DELETE,
            "http://localhost/v1/workflows/hello-there-abc123-r000",
            status=error_code,
        )
        with pytest.raises(expected_exception) as exc_info:
            runtime.stop_workflow_run("hello-there-abc123-r000")
        for telltale in telltales:
            assert telltale in str(exc_info)
