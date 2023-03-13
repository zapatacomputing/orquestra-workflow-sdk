################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Tests for orquestra.sdk._base._driver._client.
"""
from typing import Any, Dict
from unittest.mock import create_autospec

import numpy as np
import pytest
import responses

import orquestra.sdk as sdk
from orquestra.sdk._base._driver import _exceptions
from orquestra.sdk._base._driver._client import DriverClient, Paginated
from orquestra.sdk._base._driver._models import Resources
from orquestra.sdk.schema.ir import WorkflowDef
from orquestra.sdk.schema.responses import JSONResult, PickleResult, WorkflowResult
from orquestra.sdk.schema.workflow_run import RunStatus, State, TaskRun

from . import resp_mocks


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.fixture
def endpoint_mocker_base(mocked_responses):
    """
    Returns a helper for mocking requests.
    Does some boilerplate required for all request mocks
    """

    def _inner(method: str, url: str, *, default_status_code: int = 200):
        def _mocker(**responses_kwargs):
            try:
                status = responses_kwargs.pop("status")
            except KeyError:
                status = default_status_code
            mocked_responses.add(
                method,
                url,
                status=status,
                **responses_kwargs,
            )

        return _mocker

    return _inner


class TestClient:
    @pytest.fixture
    def base_uri(self):
        return "https://shouldnt.matter.example.com"

    @pytest.fixture
    def token(self):
        return "shouldn't matter"

    @pytest.fixture
    def client(self, base_uri, token):
        return DriverClient.from_token(base_uri=base_uri, token=token)

    @pytest.fixture
    def workflow_def_id(self):
        return "00000000-0000-0000-0000-000000000000"

    @pytest.fixture
    def workflow_run_id(self):
        return "00000000-0000-0000-0000-000000000000"

    @pytest.fixture
    def task_run_id(self):
        return "00000000-0000-0000-0000-000000000000"

    @pytest.fixture
    def workflow_def(self):
        @sdk.task
        def task():
            return 1

        @sdk.workflow
        def workflow():
            return task()

        return workflow().model

    class TestWorkflowDefinitions:
        # ------ queries ------

        class TestGet:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(
                endpoint_mocker_base, base_uri: str, workflow_def_id: str
            ):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    responses.GET,
                    f"{base_uri}/api/workflow-definitions/{workflow_def_id}",
                )

            @staticmethod
            def test_response_parsing(
                endpoint_mocker,
                client: DriverClient,
                workflow_def_id: str,
                workflow_def: WorkflowDef,
            ):
                """
                Verifies that the response is correctly deserialized.
                """
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_def_response(
                        id_=workflow_def_id,
                        wf_def=workflow_def,
                    ),
                )

                returned_wf_def = client.get_workflow_def(workflow_def_id)

                assert returned_wf_def == workflow_def

            @staticmethod
            def test_sets_auth(
                endpoint_mocker,
                client: DriverClient,
                token,
                workflow_def_id: str,
                workflow_def: WorkflowDef,
            ):
                endpoint_mocker(
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                    json=resp_mocks.make_get_wf_def_response(
                        id_=workflow_def_id, wf_def=workflow_def
                    ),
                )

                client.get_workflow_def(workflow_def_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_missing_wf_def_id(
                endpoint_mocker, client: DriverClient, workflow_def_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/2b353476d5b0161da31584533be208611a131bdc/openapi/src/resources/workflow-definition.yaml#L28
                    status=404,
                )

                with pytest.raises(_exceptions.WorkflowDefNotFound):
                    _ = client.get_workflow_def(workflow_def_id)

            @staticmethod
            def test_invalid_id(
                endpoint_mocker, client: DriverClient, workflow_def_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/2b353476d5b0161da31584533be208611a131bdc/openapi/src/resources/workflow-definition.yaml#L20
                    status=400,
                )

                with pytest.raises(_exceptions.InvalidWorkflowDefID):
                    _ = client.get_workflow_def(workflow_def_id)

            @staticmethod
            def test_unauthorized(
                endpoint_mocker, client: DriverClient, workflow_def_id
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/2b353476d5b0161da31584533be208611a131bdc/openapi/src/resources/workflow-definition.yaml#L26
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.get_workflow_def(workflow_def_id)

            @staticmethod
            def test_forbidden(endpoint_mocker, client: DriverClient, workflow_def_id):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definition.yaml#L28
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.get_workflow_def(workflow_def_id)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker, client: DriverClient, workflow_def_id
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/2b353476d5b0161da31584533be208611a131bdc/openapi/src/resources/workflow-definition.yaml#L34
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.get_workflow_def(workflow_def_id)

        class TestList:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(endpoint_mocker_base, base_uri: str):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """
                return endpoint_mocker_base(
                    responses.GET,
                    f"{base_uri}/api/workflow-definitions",
                )

            @staticmethod
            def test_list_workflow_defs(
                endpoint_mocker,
                client: DriverClient,
                workflow_def_id: str,
                workflow_def: WorkflowDef,
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definitions.yaml#L18
                    json=resp_mocks.make_list_wf_def_response(
                        ids=[workflow_def_id] * 10,
                        wf_defs=[workflow_def] * 10,
                    )
                )

                defs = client.list_workflow_defs()

                assert isinstance(defs, Paginated)
                assert len(defs.contents) == 10
                assert defs.contents[0] == workflow_def
                assert defs.next_page_token is None
                assert defs.prev_page_token is None

            @staticmethod
            def test_list_workflow_defs_with_pagination(
                endpoint_mocker,
                client: DriverClient,
                workflow_def_id: str,
                workflow_def: WorkflowDef,
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definitions.yaml#L18
                    json=resp_mocks.make_list_wf_def_paginated_response(
                        ids=[workflow_def_id] * 10,
                        wf_defs=[workflow_def] * 10,
                    )
                )

                defs = client.list_workflow_defs()

                assert isinstance(defs, Paginated)
                assert len(defs.contents) == 10
                assert defs.contents[0] == workflow_def
                assert (
                    defs.next_page_token == "1989-12-13T00:00:00.000000Z,"
                    "00000000-0000-0000-0000-0000000000000"
                )

            @staticmethod
            @pytest.mark.parametrize(
                "kwargs,params",
                [
                    ({}, {}),
                    ({"page_size": 100}, {"pageSize": 100}),
                    ({"page_token": "abc"}, {"pageToken": "abc"}),
                    (
                        {"page_size": 100, "page_token": "abc"},
                        {"pageSize": 100, "pageToken": "abc"},
                    ),
                ],
            )
            def test_params_encoding(
                endpoint_mocker, client: DriverClient, kwargs: Dict, params: Dict
            ):
                """
                Verifies that params are correctly sent to the server.
                """
                endpoint_mocker(
                    json=resp_mocks.make_list_wf_def_response(ids=[], wf_defs=[]),
                    match=[responses.matchers.query_param_matcher(params)],
                    # Based on:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definitions.yaml#L8
                    status=200,
                )

                client.list_workflow_defs(**kwargs)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_sets_auth(
                endpoint_mocker,
                client: DriverClient,
                token,
            ):
                endpoint_mocker(
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                    json=resp_mocks.make_list_wf_def_response(ids=[], wf_defs=[]),
                )

                client.list_workflow_defs()

                # The assertion is done by mocked_responses

            @staticmethod
            def test_unauthorized(endpoint_mocker, client: DriverClient):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definitions.yaml#L25
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.list_workflow_defs()

            @staticmethod
            def test_forbidden(endpoint_mocker, client: DriverClient):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definitions.yaml#L27
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.list_workflow_defs()

            @staticmethod
            def test_unknown_error(endpoint_mocker, client: DriverClient):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definitions.yaml#L29
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.list_workflow_defs()

        # ------ mutations ------

        class TestCreate:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(endpoint_mocker_base, base_uri: str):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """
                return endpoint_mocker_base(
                    responses.POST,
                    f"{base_uri}/api/workflow-definitions",
                    default_status_code=201,
                )

            @staticmethod
            def test_params_encoding(
                endpoint_mocker,
                client: DriverClient,
                workflow_def_id,
                workflow_def: WorkflowDef,
            ):
                """
                Verifies that params are correctly sent to the server.
                """
                endpoint_mocker(
                    json=resp_mocks.make_create_wf_def_response(id_=workflow_def_id),
                    match=[responses.matchers.json_params_matcher(workflow_def.dict())],
                    # Based on:
                    # https://github.com/zapatacomputing/workflow-driver/blob/2b353476d5b0161da31584533be208611a131bdc/openapi/src/resources/workflow-definitions.yaml#L42
                    status=201,
                )

                client.create_workflow_def(workflow_def)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_sets_auth(
                endpoint_mocker,
                client: DriverClient,
                token,
                workflow_def_id: str,
                workflow_def: WorkflowDef,
            ):
                endpoint_mocker(
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                    # Based on:
                    # https://github.com/zapatacomputing/workflow-driver/blob/2b353476d5b0161da31584533be208611a131bdc/openapi/src/resources/workflow-definitions.yaml#L42
                    status=201,
                    json=resp_mocks.make_create_wf_def_response(id_=workflow_def_id),
                )

                client.create_workflow_def(workflow_def)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_invalid_definition(
                endpoint_mocker,
                client: DriverClient,
                workflow_def: WorkflowDef,
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definitions.yaml#L50
                    status=400,
                    json=resp_mocks.make_error_response("Bad definition", "details"),
                )

                with pytest.raises(_exceptions.InvalidWorkflowDef):
                    client.create_workflow_def(workflow_def)

            @staticmethod
            def test_unauthorized(
                endpoint_mocker, client: DriverClient, workflow_def: WorkflowDef
            ):
                endpoint_mocker(
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definitions.yaml#L56
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    client.create_workflow_def(workflow_def)

            @staticmethod
            def test_forbidden(
                endpoint_mocker, client: DriverClient, workflow_def: WorkflowDef
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definitions.yaml#L58
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.create_workflow_def(workflow_def)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker, client: DriverClient, workflow_def: WorkflowDef
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/2b353476d5b0161da31584533be208611a131bdc/openapi/src/resources/workflow-definitions.yaml#L52
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.create_workflow_def(workflow_def)

        class TestDelete:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(endpoint_mocker_base, base_uri: str, workflow_def_id):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """
                return endpoint_mocker_base(
                    responses.DELETE,
                    f"{base_uri}/api/workflow-definitions/{workflow_def_id}",
                    default_status_code=204,
                )

            @staticmethod
            def test_sets_auth(
                endpoint_mocker, client: DriverClient, token, workflow_def_id
            ):
                endpoint_mocker(
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                )

                client.delete_workflow_def(workflow_def_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_missing_wf_def(
                endpoint_mocker, client: DriverClient, workflow_def_id
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/2b353476d5b0161da31584533be208611a131bdc/openapi/src/resources/workflow-definition.yaml#L56
                    status=404,
                )

                with pytest.raises(_exceptions.WorkflowDefNotFound):
                    _ = client.delete_workflow_def(workflow_def_id)

            @staticmethod
            def test_invalid_id(endpoint_mocker, client: DriverClient, workflow_def_id):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/2b353476d5b0161da31584533be208611a131bdc/openapi/src/resources/workflow-definition.yaml#L48
                    status=400,
                )

                with pytest.raises(_exceptions.InvalidWorkflowDefID):
                    _ = client.delete_workflow_def(workflow_def_id)

            @staticmethod
            def test_unauthorized(
                endpoint_mocker, client: DriverClient, workflow_def_id
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definition.yaml#L56
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.delete_workflow_def(workflow_def_id)

            @staticmethod
            def test_forbidden(
                endpoint_mocker, client: DriverClient, workflow_def_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definition.yaml#L58
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.delete_workflow_def(workflow_def_id)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker, client: DriverClient, workflow_def_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-definition.yaml#L66
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.delete_workflow_def(workflow_def_id)

    class TestWorkflowRuns:
        @pytest.fixture
        def workflow_run_status(self):
            return RunStatus(state=State.WAITING, start_time=None, end_time=None)

        @pytest.fixture
        def task_run_status(self):
            return RunStatus(state=State.WAITING, start_time=None, end_time=None)

        @pytest.fixture
        def workflow_run_tasks(self, task_run_id, task_run_status):
            return [
                TaskRun(id=task_run_id, invocation_id=f"{i}", status=task_run_status)
                for i in range(3)
            ]

        @pytest.fixture
        def resources(self):
            return Resources(cpu=None, memory=None, gpu=None)

        class TestGet:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(endpoint_mocker_base, base_uri: str, workflow_run_id):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    responses.GET,
                    f"{base_uri}/api/workflow-runs/{workflow_run_id}",
                )

            @staticmethod
            @pytest.fixture
            def mock_get_workflow_def(
                client: DriverClient,
                workflow_def: WorkflowDef,
                monkeypatch: pytest.MonkeyPatch,
            ):
                mock = create_autospec(client.get_workflow_def)
                mock.return_value = workflow_def
                monkeypatch.setattr(client, "get_workflow_def", mock)

            @staticmethod
            def test_invalid_wf_run_id(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run.yaml#L20
                    status=400,
                )

                with pytest.raises(_exceptions.InvalidWorkflowRunID):
                    _ = client.get_workflow_run(workflow_run_id)

            @staticmethod
            def test_missing_wf_run(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run.yaml#L28
                    status=404,
                )

                with pytest.raises(_exceptions.WorkflowRunNotFound):
                    _ = client.get_workflow_run(workflow_run_id)

            @staticmethod
            def test_missing_task_run_status(
                endpoint_mocker,
                mock_get_workflow_def,
                client: DriverClient,
                workflow_run_id: str,
                workflow_def_id: str,
                workflow_run_status: RunStatus,
            ):
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_run_missing_task_run_status(
                        workflow_run_id, workflow_def_id, workflow_run_status
                    ),
                )

                wf_run = client.get_workflow_run(workflow_run_id)

                assert len(wf_run.task_runs) == 1
                assert wf_run.task_runs[0].status == RunStatus(
                    state=State.WAITING, end_time=None, start_time=None
                )

            @staticmethod
            def test_sets_auth(
                endpoint_mocker,
                mock_get_workflow_def,
                client: DriverClient,
                token,
                workflow_run_id,
                workflow_def_id,
                workflow_run_status,
                workflow_run_tasks,
            ):
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_run_response(
                        id_=workflow_run_id,
                        workflow_def_id=workflow_def_id,
                        status=workflow_run_status,
                        task_runs=workflow_run_tasks,
                    ),
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                )

                client.get_workflow_run(workflow_run_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_unauthorized(
                endpoint_mocker, client: DriverClient, workflow_run_id
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run.yaml#L26
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.get_workflow_run(workflow_run_id)

            @staticmethod
            def test_forbidden(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run.yaml#L28
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.get_workflow_run(workflow_run_id)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run.yaml#L36
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.get_workflow_run(workflow_run_id)

        class TestList:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(endpoint_mocker_base, base_uri: str):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    responses.GET,
                    f"{base_uri}/api/workflow-runs",
                )

            @staticmethod
            @pytest.fixture
            def mock_get_workflow_def(
                client: DriverClient,
                workflow_def: WorkflowDef,
                monkeypatch: pytest.MonkeyPatch,
            ):
                mock = create_autospec(client.get_workflow_def)
                mock.return_value = workflow_def
                monkeypatch.setattr(client, "get_workflow_def", mock)

            @staticmethod
            def test_list_workflow_runs(
                endpoint_mocker,
                mock_get_workflow_def,
                client: DriverClient,
                workflow_run_id: str,
                workflow_def_id: str,
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-runs.yaml#L14
                    json=resp_mocks.make_list_wf_run_response(
                        ids=[workflow_run_id] * 10,
                        workflow_def_ids=[workflow_def_id] * 10,
                    )
                )

                defs = client.list_workflow_runs()

                assert isinstance(defs, Paginated)
                assert len(defs.contents) == 10
                assert defs.contents[0].id == workflow_run_id
                assert defs.next_page_token is None
                assert defs.prev_page_token is None

            @staticmethod
            def test_list_workflow_runs_with_pagination(
                endpoint_mocker,
                mock_get_workflow_def,
                client: DriverClient,
                workflow_run_id: str,
                workflow_def_id: str,
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-runs.yaml#L14
                    json=resp_mocks.make_list_wf_run_paginated_response(
                        ids=[workflow_run_id] * 10,
                        workflow_def_ids=[workflow_def_id] * 10,
                    )
                )

                defs = client.list_workflow_runs()

                assert isinstance(defs, Paginated)
                assert len(defs.contents) == 10
                assert defs.contents[0].id == workflow_run_id
                assert (
                    defs.next_page_token == "1989-12-13T00:00:00.000000Z,"
                    "00000000-0000-0000-0000-0000000000000"
                )

            @staticmethod
            @pytest.mark.parametrize(
                "kwargs,params",
                [
                    ({}, {}),
                    ({"workflow_def_id": "0"}, {"workflowDefinitionID": "0"}),
                    ({"page_size": 100}, {"pageSize": 100}),
                    ({"page_token": "abc"}, {"pageToken": "abc"}),
                    (
                        {"page_size": 100, "page_token": "abc"},
                        {"pageSize": 100, "pageToken": "abc"},
                    ),
                ],
            )
            def test_params_encoding(
                endpoint_mocker,
                mock_get_workflow_def,
                client: DriverClient,
                kwargs: Dict,
                params: Dict,
            ):
                """
                Verifies that params are correctly sent to the server.
                """
                endpoint_mocker(
                    json=resp_mocks.make_list_wf_def_response(ids=[], wf_defs=[]),
                    match=[responses.matchers.query_param_matcher(params)],
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-runs.yaml#L14
                    status=200,
                )

                client.list_workflow_runs(**kwargs)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_sets_auth(
                endpoint_mocker,
                mock_get_workflow_def,
                client: DriverClient,
                token,
                workflow_run_id,
                workflow_def_id,
            ):
                endpoint_mocker(
                    json=resp_mocks.make_list_wf_run_response(
                        ids=[workflow_run_id] * 10,
                        workflow_def_ids=[workflow_def_id] * 10,
                    ),
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                )

                client.list_workflow_runs()

                # The assertion is done by mocked_responses

            @staticmethod
            def test_unauthorized(endpoint_mocker, client: DriverClient):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-runs.yaml#L33
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.list_workflow_runs()

            @staticmethod
            def test_forbidden(endpoint_mocker, client: DriverClient):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-runs.yaml#L35
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.list_workflow_runs()

            @staticmethod
            def test_unknown_error(endpoint_mocker, client: DriverClient):
                endpoint_mocker(
                    # Specified in
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-runs.yaml#L37
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.list_workflow_runs()

        class TestCreate:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(endpoint_mocker_base, base_uri: str):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    responses.POST,
                    f"{base_uri}/api/workflow-runs",
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-runs.yaml#L43
                    default_status_code=201,
                )

            @staticmethod
            def test_invalid_request(
                endpoint_mocker,
                client: DriverClient,
                workflow_def_id: str,
                resources: Resources,
            ):
                endpoint_mocker(
                    json=resp_mocks.make_error_response("Bad definition", "details"),
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-runs.yaml#L45
                    status=400,
                )

                with pytest.raises(_exceptions.InvalidWorkflowRunRequest):
                    _ = client.create_workflow_run(workflow_def_id, resources)

            @staticmethod
            def test_sets_auth(
                endpoint_mocker,
                client: DriverClient,
                token,
                workflow_def_id,
                resources: Resources,
            ):
                endpoint_mocker(
                    json=resp_mocks.make_submit_wf_run_response(workflow_def_id),
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                )

                client.create_workflow_run(workflow_def_id, resources)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_unauthorized(
                endpoint_mocker,
                client: DriverClient,
                workflow_def_id,
                resources: Resources,
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-runs.yaml#L63
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.create_workflow_run(workflow_def_id, resources)

            @staticmethod
            def test_forbidden(
                endpoint_mocker,
                client: DriverClient,
                workflow_def_id,
                resources: Resources,
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-runs.yaml#L65
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.create_workflow_run(workflow_def_id, resources)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker,
                client: DriverClient,
                workflow_def_id,
                resources: Resources,
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-runs.yaml#L67
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.create_workflow_run(workflow_def_id, resources)

        class TestTerminate:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(
                endpoint_mocker_base, base_uri: str, workflow_run_id: str
            ):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    method=responses.POST,
                    url=f"{base_uri}/api/workflow-runs/{workflow_run_id}/terminate",
                    # Specified in
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run-terminate.yaml#L11
                    default_status_code=204,
                )

            @staticmethod
            def test_not_found(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run-terminate.yaml#L17
                    status=404,
                )

                with pytest.raises(_exceptions.WorkflowRunNotFound):
                    _ = client.terminate_workflow_run(workflow_run_id)

            @staticmethod
            def test_sets_auth(
                endpoint_mocker, client: DriverClient, token: str, workflow_run_id: str
            ):
                endpoint_mocker(
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                )

                client.terminate_workflow_run(workflow_run_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_unauthorized(
                endpoint_mocker,
                client: DriverClient,
                workflow_run_id: str,
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run-terminate.yaml#L13
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.terminate_workflow_run(workflow_run_id)

            @staticmethod
            def test_forbidden(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run-terminate.yaml#L15
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.terminate_workflow_run(workflow_run_id)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run-terminate.yaml#L23
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.terminate_workflow_run(workflow_run_id)

    class TestWorkflowRunArtifacts:
        @pytest.fixture
        def workflow_run_artifact_id(self):
            return "00000000-0000-0000-0000-000000000000"

        class TestGetWorkflowRunArtifacts:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(endpoint_mocker_base, base_uri: str):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    responses.GET,
                    f"{base_uri}/api/artifacts",
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/artifacts.yaml#L13
                    default_status_code=200,
                )

            @staticmethod
            def test_params_encoding(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                """
                Verifies that params are correctly sent to the server.
                """
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_run_artifacts_response(),
                    match=[
                        responses.matchers.query_param_matcher(
                            {"workflowRunId": workflow_run_id}
                        )
                    ],
                )

                _ = client.get_workflow_run_artifacts(workflow_run_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_not_found(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/artifacts.yaml#L32
                    status=404,
                )

                with pytest.raises(_exceptions.WorkflowRunNotFound):
                    _ = client.get_workflow_run_artifacts(workflow_run_id)

            @staticmethod
            def test_invalid_id(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/artifacts.yaml#L22
                    status=400,
                )

                with pytest.raises(_exceptions.InvalidWorkflowRunID):
                    _ = client.get_workflow_run_artifacts(workflow_run_id)

            @staticmethod
            def test_sets_auth(
                endpoint_mocker, client: DriverClient, token: str, workflow_run_id: str
            ):
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_run_artifacts_response(),
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                )

                _ = client.get_workflow_run_artifacts(workflow_run_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_unauthorized(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/artifacts.yaml#L28
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.get_workflow_run_artifacts(workflow_run_id)

            @staticmethod
            def test_forbidden(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/artifacts.yaml#L30
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.get_workflow_run_artifacts(workflow_run_id)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/artifacts.yaml#L38
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.get_workflow_run_artifacts(workflow_run_id)

        class TestGetWorkflowRunArtifact:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(
                endpoint_mocker_base, base_uri: str, workflow_run_artifact_id: str
            ):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    responses.GET,
                    f"{base_uri}/api/artifacts/{workflow_run_artifact_id}",
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/artifact.yaml#L11
                    default_status_code=200,
                )

            @staticmethod
            @pytest.mark.parametrize("obj", [None, 100, "hello", np.eye(10)])
            def test_objects(
                endpoint_mocker,
                client: DriverClient,
                workflow_run_artifact_id: str,
                obj: Any,
            ):
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_run_artifact_response(obj),
                )

                result = client.get_workflow_run_artifact(workflow_run_artifact_id)

                assert isinstance(result, (JSONResult, PickleResult))

            @staticmethod
            def test_not_found(
                endpoint_mocker, client: DriverClient, workflow_run_artifact_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/artifact.yaml#L29
                    status=404,
                )

                with pytest.raises(_exceptions.WorkflowRunArtifactNotFound):
                    _ = client.get_workflow_run_artifact(workflow_run_artifact_id)

            @staticmethod
            def test_invalid_id(
                endpoint_mocker, client: DriverClient, workflow_run_artifact_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/artifact.yaml#L19
                    status=400,
                )

                with pytest.raises(_exceptions.InvalidWorkflowRunArtifactID):
                    _ = client.get_workflow_run_artifact(workflow_run_artifact_id)

            @staticmethod
            def test_sets_auth(
                endpoint_mocker,
                client: DriverClient,
                token: str,
                workflow_run_artifact_id: str,
            ):
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_run_artifact_response(None),
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                )

                _ = client.get_workflow_run_artifact(workflow_run_artifact_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_unauthorized(
                endpoint_mocker, client: DriverClient, workflow_run_artifact_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/artifact.yaml#L25
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.get_workflow_run_artifact(workflow_run_artifact_id)

            @staticmethod
            def test_forbidden(
                endpoint_mocker, client: DriverClient, workflow_run_artifact_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/artifact.yaml#L27
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.get_workflow_run_artifact(workflow_run_artifact_id)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker, client: DriverClient, workflow_run_artifact_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/artifact.yaml#L35
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.get_workflow_run_artifact(workflow_run_artifact_id)

    class TestWorkflowRunResults:
        @pytest.fixture
        def workflow_run_result_id(self):
            return "00000000-0000-0000-0000-000000000000"

        class TestGetWorkflowRunResults:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(endpoint_mocker_base, base_uri: str):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    responses.GET,
                    f"{base_uri}/api/run-results",
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/run-results.yaml#L13
                    default_status_code=200,
                )

            @staticmethod
            def test_params_encoding(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                """
                Verifies that params are correctly sent to the server.
                """
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_run_results_response(),
                    match=[
                        responses.matchers.query_param_matcher(
                            {"workflowRunId": workflow_run_id}
                        )
                    ],
                )

                _ = client.get_workflow_run_results(workflow_run_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_invalid_wf_run_id(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/run-results.yaml#L22
                    status=400,
                )

                with pytest.raises(_exceptions.InvalidWorkflowRunID):
                    _ = client.get_workflow_run_results(workflow_run_id)

            @staticmethod
            def test_missing_wf_run(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/run-results.yaml#L32
                    status=404,
                )

                with pytest.raises(_exceptions.WorkflowRunNotFound):
                    _ = client.get_workflow_run_results(workflow_run_id)

            @staticmethod
            def test_sets_auth(
                endpoint_mocker, client: DriverClient, token: str, workflow_run_id: str
            ):
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_run_results_response(),
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                )

                _ = client.get_workflow_run_results(workflow_run_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_unauthorized(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/run-results.yaml#L28
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.get_workflow_run_results(workflow_run_id)

            @staticmethod
            def test_forbidden(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/run-results.yaml#L30
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.get_workflow_run_results(workflow_run_id)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/run-results.yaml#L38
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.get_workflow_run_results(workflow_run_id)

        class TestGetWorkflowRunResult:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(
                endpoint_mocker_base, base_uri: str, workflow_run_result_id: str
            ):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    responses.GET,
                    f"{base_uri}/api/run-results/{workflow_run_result_id}",
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/run-result.yaml#L11
                    default_status_code=200,
                )

            @staticmethod
            @pytest.mark.parametrize("obj", [None, 100, "hello", np.eye(10)])
            def test_objects(
                endpoint_mocker,
                client: DriverClient,
                workflow_run_result_id: str,
                obj: Any,
            ):
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_run_result_response(obj),
                )

                result = client.get_workflow_run_result(workflow_run_result_id)

                assert isinstance(result, (JSONResult, PickleResult))

            @staticmethod
            def test_sets_auth(
                endpoint_mocker,
                client: DriverClient,
                token: str,
                workflow_run_result_id: str,
            ):
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_run_result_response(None),
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                )

                _ = client.get_workflow_run_result(workflow_run_result_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_not_found(
                endpoint_mocker, client: DriverClient, workflow_run_result_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/run-result.yaml#L29
                    status=404,
                )

                with pytest.raises(_exceptions.WorkflowRunResultNotFound):
                    _ = client.get_workflow_run_result(workflow_run_result_id)

            @staticmethod
            def test_invalid_id(
                endpoint_mocker, client: DriverClient, workflow_run_result_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/run-result.yaml#L19
                    status=400,
                )

                with pytest.raises(_exceptions.InvalidWorkflowRunResultID):
                    _ = client.get_workflow_run_result(workflow_run_result_id)

            @staticmethod
            def test_unauthorized(
                endpoint_mocker, client: DriverClient, workflow_run_result_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/run-result.yaml#L25
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.get_workflow_run_result(workflow_run_result_id)

            @staticmethod
            def test_forbidden(
                endpoint_mocker, client: DriverClient, workflow_run_result_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/run-result.yaml#L27
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.get_workflow_run_result(workflow_run_result_id)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker, client: DriverClient, workflow_run_result_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/run-result.yaml#L35
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.get_workflow_run_result(workflow_run_result_id)

    class TestWorkflowLogs:
        class TestWorkflowRunLogs:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(endpoint_mocker_base, base_uri: str):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    responses.GET,
                    f"{base_uri}/api/workflow-run-logs",
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run-logs.yaml#L11
                    default_status_code=200,
                )

            @staticmethod
            def test_params_encoding(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                """
                Verifies that params are correctly sent to the server.
                """
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_run_logs_response(),
                    match=[
                        responses.matchers.query_param_matcher(
                            {"workflowRunId": workflow_run_id}
                        )
                    ],
                )

                _ = client.get_workflow_run_logs(workflow_run_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_invalid_id(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run-logs.yaml#L18
                    status=400,
                )

                with pytest.raises(_exceptions.InvalidWorkflowRunID):
                    _ = client.get_workflow_run_logs(workflow_run_id)

            @staticmethod
            def test_not_found(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run-logs.yaml#L28
                    status=404,
                )

                with pytest.raises(_exceptions.WorkflowRunLogsNotFound):
                    _ = client.get_workflow_run_logs(workflow_run_id)

            @staticmethod
            def test_sets_auth(
                endpoint_mocker, client: DriverClient, token: str, workflow_run_id: str
            ):
                endpoint_mocker(
                    json=resp_mocks.make_get_wf_run_logs_response(),
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                )

                _ = client.get_workflow_run_logs(workflow_run_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_unauthorized(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run-logs.yaml#L24
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.get_workflow_run_logs(workflow_run_id)

            @staticmethod
            def test_forbidden(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run-logs.yaml#L26
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.get_workflow_run_logs(workflow_run_id)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker, client: DriverClient, workflow_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/workflow-run-logs.yaml#L34
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.get_workflow_run_logs(workflow_run_id)

        class TestTaskRunLogs:
            @staticmethod
            @pytest.fixture
            def endpoint_mocker(endpoint_mocker_base, base_uri: str):
                """
                Returns a helper for mocking requests. Assumes that most of the tests
                inside this class contain a very similar set up.
                """

                return endpoint_mocker_base(
                    responses.GET,
                    f"{base_uri}/api/task-run-logs",
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/task-run-logs.yaml#L11
                    default_status_code=200,
                )

            @staticmethod
            def test_params_encoding(
                endpoint_mocker, client: DriverClient, task_run_id: str
            ):
                """
                Verifies that params are correctly sent to the server.
                """
                endpoint_mocker(
                    json=resp_mocks.make_get_task_run_logs_response(),
                    match=[
                        responses.matchers.query_param_matcher(
                            {"taskRunId": task_run_id}
                        )
                    ],
                )

                _ = client.get_task_run_logs(task_run_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_sets_auth(
                endpoint_mocker, client: DriverClient, token: str, task_run_id: str
            ):
                endpoint_mocker(
                    json=resp_mocks.make_get_task_run_logs_response(),
                    match=[
                        responses.matchers.header_matcher(
                            {"Authorization": f"Bearer {token}"}
                        )
                    ],
                )

                _ = client.get_task_run_logs(task_run_id)

                # The assertion is done by mocked_responses

            @staticmethod
            def test_unauthorized(
                endpoint_mocker, client: DriverClient, task_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/task-run-logs.yaml#L18
                    status=401,
                )

                with pytest.raises(_exceptions.InvalidTokenError):
                    _ = client.get_task_run_logs(task_run_id)

            @staticmethod
            def test_forbidden(endpoint_mocker, client: DriverClient, task_run_id: str):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/task-run-logs.yaml#L20
                    status=403,
                )

                with pytest.raises(_exceptions.ForbiddenError):
                    _ = client.get_task_run_logs(task_run_id)

            @staticmethod
            def test_unknown_error(
                endpoint_mocker, client: DriverClient, task_run_id: str
            ):
                endpoint_mocker(
                    # Specified in:
                    # https://github.com/zapatacomputing/workflow-driver/blob/6270a214fff40f53d7b25ec967f2e7875eb296e3/openapi/src/resources/task-run-logs.yaml#L22
                    status=500,
                )

                with pytest.raises(_exceptions.UnknownHTTPError):
                    _ = client.get_task_run_logs(task_run_id)
