################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Code for accessing the Workflow Driver API.

Implemented API spec:
    https://github.com/zapatacomputing/workflow-driver/tree/2b3534/openapi
"""

from typing import Generic, List, Mapping, Optional, TypeVar
from urllib.parse import urljoin

import pydantic
import requests
from requests import codes

from orquestra.sdk.schema.ir import WorkflowDef
from orquestra.sdk.schema.responses import WorkflowResult
from orquestra.sdk.schema.workflow_run import WorkflowRun, WorkflowRunMinimal

from . import _exceptions, _models

API_ACTIONS = {
    # Workflow Definitions
    "create_workflow_def": "/api/workflow-definitions",
    "list_workflow_defs": "/api/workflow-definitions",
    "get_workflow_def": "/api/workflow-definitions/{}",
    "delete_workflow_def": "/api/workflow-definitions/{}",
    # Workflow Runs
    "create_workflow_run": "/api/workflow-runs",
    "list_workflow_runs": "/api/workflow-runs",
    "get_workflow_run": "/api/workflow-runs/{}",
    "terminate_workflow_run": "/api/workflow-runs/{}/terminate",
    # Artifacts
    "get_workflow_run_artifacts": "/api/artifacts",
    "get_artifact": "/api/artifacts/{}",
    # Run results
    "get_workflow_run_results": "/api/run-results",
    "get_workflow_run_result": "/api/run-results/{}",
    # Logs
    "get_workflow_run_logs": "/api/workflow-run-logs",
    "get_task_run_logs": "/api/task-run-logs",
    # Login
    "get_login_url": "v1/login",
}


def _handle_common_errors(response: requests.Response):
    if response.status_code == codes.UNAUTHORIZED:
        raise _exceptions.InvalidTokenError()
    elif response.status_code == codes.FORBIDDEN:
        raise _exceptions.ForbiddenError()
    elif not response.ok:
        raise _exceptions.UnknownHTTPError(response)


T = TypeVar("T")


class Paginated(Generic[T]):
    """
    Represents a paginated response.

    The contents of the current page can be accessed via ``contents``.

    ``prev_page_token`` and ``next_page_token`` can be used with the original method to
    return the previous and next page, respectively.
    """

    def __init__(
        self,
        contents: List[T],
        prev_page_token: Optional[str] = None,
        next_page_token: Optional[str] = None,
    ):
        self._contents = contents
        self._prev_token = prev_page_token
        self._next_token = next_page_token

    def __repr__(self):
        return (
            f"Paginated(contents={self._contents}, "
            f"prev_token={self._prev_token}, next_token={self._next_token})"
        )

    @property
    def contents(self) -> List[T]:
        """The contents property."""
        return self._contents

    @property
    def prev_page_token(self) -> Optional[str]:
        """The prev_token property."""
        return self._prev_token

    @property
    def next_page_token(self) -> Optional[str]:
        """The next_token property."""
        return self._next_token


class DriverClient:
    """
    Client for interacting with the Workflow Driver API via HTTP.
    """

    def __init__(self, base_uri: str, session: requests.Session):
        self._base_uri = base_uri
        self._session = session

    @classmethod
    def from_token(cls, base_uri: str, token: str):
        """
        Args:
            base_uri: Orquestra cluster URI, like 'https://foobar.orquestra.io'.
            token: Auth token taken from logging in.
        """
        session = requests.Session()
        session.headers["Content-Type"] = "application/json"
        session.headers["Authorization"] = f"Bearer {token}"
        return cls(base_uri=base_uri, session=session)

    # --- helpers ---

    def _get(
        self,
        endpoint: str,
        query_params: Optional[Mapping],
        allow_redirects: bool = True,
    ) -> requests.Response:
        """Helper method for GET requests"""
        response = self._session.get(
            urljoin(self._base_uri, endpoint),
            params=query_params,
            allow_redirects=allow_redirects,
        )

        return response

    def _post(self, endpoint: str, body_params: Optional[Mapping]) -> requests.Response:
        """Helper method for POST requests"""
        response = self._session.post(
            urljoin(self._base_uri, endpoint),
            json=body_params,
        )
        return response

    def _delete(self, endpoint: str) -> requests.Response:
        """Helper method for DELETE requests"""
        response = self._session.delete(urljoin(self._base_uri, endpoint))

        return response

    # --- queries ---

    # ---- Worklow Defs ----

    def create_workflow_def(self, workflow_def: WorkflowDef) -> _models.WorkflowDefID:
        """
        Stores a workflow definition for future submission

        Raises:
            InvalidWorkflowDef: see the exception's docstring
            InvalidTokenError: see the exception's docstring
            ForbiddenError: see the exception's docstring
            UnknownHTTPError: see the exception's docstring

        Returns:
            the WorkflowDefID associated with the stored definition
        """
        resp = self._post(
            API_ACTIONS["create_workflow_def"],
            body_params=workflow_def.dict(),
        )

        if resp.status_code == codes.BAD_REQUEST:
            error = _models.Error.parse_obj(resp.json())
            raise _exceptions.InvalidWorkflowDef(
                message=error.message, detail=error.detail
            )

        _handle_common_errors(resp)

        return (
            _models.Response[_models.CreateWorkflowDefResponse, _models.MetaEmpty]
            .parse_obj(resp.json())
            .data.id
        )

    def list_workflow_defs(
        self, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Paginated[WorkflowDef]:
        """
        Lists all known workflow definitions

        Raises:
            InvalidTokenError: see the exception's docstring
            ForbiddenError: see the exception's docstring
            UnknownHTTPError: see the exception's docstring
        """
        resp = self._get(
            API_ACTIONS["list_workflow_defs"],
            query_params=_models.ListWorkflowDefsRequest(
                pageSize=page_size,
                pageToken=page_token,
            ).dict(),
        )

        _handle_common_errors(resp)

        parsed_response = _models.Response[
            _models.ListWorkflowDefsResponse, _models.Pagination
        ].parse_obj(resp.json())
        contents = [d.workflow for d in parsed_response.data]
        if parsed_response.meta is not None:
            next_token = parsed_response.meta.nextPageToken
        else:
            next_token = None

        return Paginated(
            contents=contents,
            next_page_token=next_token,
        )

    def get_login_url(self, redirect_port: int) -> str:
        """First step in the auth flow. Fetches the URL that the user has to visit.

        Raises:
            requests.ConnectionError: if the request fails.
            KeyError: if the URL couldn't be found in the response.
        """
        resp = self._get(
            API_ACTIONS["get_login_url"],
            query_params={"state": f"{redirect_port}"},
            allow_redirects=False,
        )
        return resp.headers["Location"]

    def get_workflow_def(self, workflow_def_id: _models.WorkflowDefID) -> WorkflowDef:
        """
        Gets a stored workflow definition

        Raises:
            InvalidWorkflowDefID: see the exception's docstring
            WorkflowDefNotFound: see the exception's docstring
            InvalidTokenError: see the exception's docstring
            ForbiddenError: see the exception's docstring
            UnknownHTTPError: see the exception's docstring

        Returns:
            a parsed WorkflowDef
        """
        resp = self._get(
            API_ACTIONS["get_workflow_def"].format(workflow_def_id),
            query_params=None,
        )

        if resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowDefID(workflow_def_id)
        elif resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowDefNotFound(workflow_def_id)

        _handle_common_errors(resp)

        parsed_resp = _models.Response[
            _models.GetWorkflowDefResponse, _models.MetaEmpty
        ].parse_obj(resp.json())

        return parsed_resp.data.workflow

    def delete_workflow_def(self, workflow_def_id: _models.WorkflowDefID):
        """
        Gets a stored workflow definition

        Raises:
            InvalidWorkflowDefID: see the exception's docstring
            WorkflowDefNotFound: see the exception's docstring
            InvalidTokenError: see the exception's docstring
            ForbiddenError: see the exception's docstring
            UnknownHTTPError: see the exception's docstring
        """
        resp = self._delete(
            API_ACTIONS["delete_workflow_def"].format(workflow_def_id),
        )

        if resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowDefID(workflow_def_id)
        elif resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowDefNotFound(workflow_def_id)

        _handle_common_errors(resp)

    # ---- Workflow Runs ----

    def create_workflow_run(
        self, workflow_def_id: _models.WorkflowDefID, resources: _models.Resources
    ) -> _models.WorkflowRunID:
        """
        Submit a workflow def to run in the workflow driver

        Raises:
            InvalidWorkflowRunRequest: see the exception's docstring
            InvalidTokenError: see the exception's docstring
            ForbiddenError: see the exception's docstring
            UnknownHTTPError: see the exception's docstring
        """
        resp = self._post(
            API_ACTIONS["create_workflow_run"],
            body_params=_models.CreateWorkflowRunRequest(
                workflowDefinitionID=workflow_def_id, resources=resources
            ).dict(),
        )

        if resp.status_code == codes.BAD_REQUEST:
            error = _models.Error.parse_obj(resp.json())
            raise _exceptions.InvalidWorkflowRunRequest(
                message=error.message, detail=error.detail
            )

        _handle_common_errors(resp)

        return (
            _models.Response[_models.CreateWorkflowRunResponse, _models.MetaEmpty]
            .parse_obj(resp.json())
            .data.id
        )

    def list_workflow_runs(
        self,
        workflow_def_id: Optional[_models.WorkflowDefID] = None,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> Paginated[WorkflowRunMinimal]:
        """
        List workflow runs with a specified workflow def ID from the workflow driver

        Raises:
            InvalidTokenError: see the exception's docstring
            ForbiddenError: see the exception's docstring
            UnknownHTTPError: see the exception's docstring
        """
        resp = self._get(
            API_ACTIONS["list_workflow_runs"],
            query_params=_models.ListWorkflowRunsRequest(
                workflowDefinitionID=workflow_def_id,
                pageSize=page_size,
                pageToken=page_token,
            ).dict(),
        )

        _handle_common_errors(resp)

        parsed_response = _models.Response[
            _models.ListWorkflowRunsResponse, _models.Pagination
        ].parse_obj(resp.json())

        if parsed_response.meta is not None:
            next_token = parsed_response.meta.nextPageToken
        else:
            next_token = None

        workflow_runs = []
        for r in parsed_response.data:
            workflow_def = self.get_workflow_def(r.definitionId)
            workflow_runs.append(r.to_ir(workflow_def))

        return Paginated(
            contents=workflow_runs,
            next_page_token=next_token,
        )

    def get_workflow_run(self, wf_run_id: _models.WorkflowRunID) -> WorkflowRun:
        """
        Gets the status of a workflow run from the workflow driver

        Raises:
            InvalidWorkflowRunID: see the exception's docstring
            WorkflowRunNotFound: see the exception's docstring
            InvalidTokenError: see the exception's docstring
            ForbiddenError: see the exception's docstring
            UnknownHTTPError: see the exception's docstring
        """

        resp = self._get(
            API_ACTIONS["get_workflow_run"].format(wf_run_id),
            query_params=None,
        )

        if resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowRunID(wf_run_id)
        elif resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowRunNotFound(wf_run_id)

        _handle_common_errors(resp)

        parsed_response = _models.Response[
            _models.WorkflowRunResponse, _models.MetaEmpty
        ].parse_obj(resp.json())

        workflow_def = self.get_workflow_def(parsed_response.data.definitionId)

        return parsed_response.data.to_ir(workflow_def)

    def terminate_workflow_run(self, wf_run_id: _models.WorkflowRunID):
        """
        Asks the workflow driver to terminate a workflow run

        Raises:
            WorkflowRunNotFound: see the exception's docstring
            InvalidTokenError: see the exception's docstring
            ForbiddenError: see the exception's docstring
            UnknownHTTPError: see the exception's docstring
        """

        resp = self._post(
            API_ACTIONS["terminate_workflow_run"].format(wf_run_id),
            body_params=None,
        )

        if resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowRunNotFound(wf_run_id)

        _handle_common_errors(resp)

    # --- Workflow Run Artifacts ---

    def get_workflow_run_artifacts(
        self, wf_run_id: _models.WorkflowRunID
    ) -> Mapping[_models.TaskRunID, List[_models.WorkflowRunArtifactID]]:
        """
        Gets the workflow run artifact IDs of a workflow run from the workflow driver

        Raises:
            InvalidWorkflowRunID: see the exception's docstring
            WorkflowRunNotFound: see the exception's docstring
            InvalidTokenError: see the exception's docstring
            ForbiddenError: see the exception's docstring
            UnknownHTTPError: see the exception's docstring
        """

        resp = self._get(
            API_ACTIONS["get_workflow_run_artifacts"],
            query_params=_models.GetWorkflowRunArtifactsRequest(
                workflowRunId=wf_run_id
            ).dict(),
        )

        if resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowRunNotFound(wf_run_id)
        elif resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowRunID(wf_run_id)

        _handle_common_errors(resp)

        parsed_response = _models.Response[
            _models.GetWorkflowRunArtifactsResponse, _models.MetaEmpty
        ].parse_obj(resp.json())

        return parsed_response.data

    def get_workflow_run_artifact(
        self, artifact_id: _models.WorkflowRunArtifactID
    ) -> WorkflowResult:
        """
        Gets workflow run artifacts from the workflow driver

        Raises:
            InvalidWorkflowRunArtifactID: see the exception's docstring
            WorkflowRunArtifactNotFound: see the exception's docstring
            InvalidTokenError: see the exception's docstring
            ForbiddenError: see the exception's docstring
            UnknownHTTPError: see the exception's docstring
        """

        resp = self._get(
            API_ACTIONS["get_artifact"].format(artifact_id),
            query_params=None,
        )

        if resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowRunArtifactNotFound(artifact_id)
        elif resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowRunArtifactID(artifact_id)

        _handle_common_errors(resp)

        # Bug with mypy and Pydantic:
        #   Unions cannot be passed to parse_obj_as: pydantic/pydantic#1847
        return pydantic.parse_obj_as(
            WorkflowResult, resp.json()  # type: ignore[arg-type]
        )

    # --- Workflow Run Results ---

    def get_workflow_run_results(
        self, wf_run_id: _models.WorkflowRunID
    ) -> List[_models.WorkflowRunResultID]:
        """
        Gets the workflow run result IDs of a workflow run from the workflow driver

        Raises:
            InvalidWorkflowRunID: see the exception's docstring
            WorkflowRunNotFound: see the exception's docstring
            InvalidTokenError: see the exception's docstring
            ForbiddenError: see the exception's docstring
            UnknownHTTPError: see the exception's docstring
        """

        resp = self._get(
            API_ACTIONS["get_workflow_run_results"],
            query_params=_models.GetWorkflowRunResultsRequest(
                workflowRunId=wf_run_id
            ).dict(),
        )

        if resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowRunID(wf_run_id)
        elif resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowRunNotFound(wf_run_id)

        _handle_common_errors(resp)

        parsed_response = _models.Response[
            _models.GetWorkflowRunResultsResponse, _models.MetaEmpty
        ].parse_obj(resp.json())

        return parsed_response.data

    def get_workflow_run_result(
        self, result_id: _models.WorkflowRunResultID
    ) -> WorkflowResult:
        """
        Gets workflow run results from the workflow driver

        Raises:
            InvalidWorkflowRunResultID: see the exception's docstring
            WorkflowRunResultNotFound: see the exception's docstring
            InvalidTokenError: see the exception's docstring
            ForbiddenError: see the exception's docstring
            UnknownHTTPError: see the exception's docstring
        """

        resp = self._get(
            API_ACTIONS["get_workflow_run_result"].format(result_id), query_params=None
        )

        if resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowRunResultNotFound(result_id)
        elif resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowRunResultID(result_id)

        _handle_common_errors(resp)

        # Bug with mypy and Pydantic:
        #   Unions cannot be passed to parse_obj_as: pydantic/pydantic#1847
        return pydantic.parse_obj_as(
            WorkflowResult, resp.json()  # type: ignore[arg-type]
        )

    # --- Workflow Logs ---

    def get_workflow_run_logs(self, wf_run_id: _models.WorkflowRunID) -> bytes:
        """
        Gets the logs of a workflow run from the workflow driver

        Raises:
            InvalidWorkflowRunID: see the exception's docstring
            WorkflowRunNotFound: see the exception's docstring
            InvalidTokenError: see the exception's docstring
            ForbiddenError: see the exception's docstring
            UnknownHTTPError: see the exception's docstring
        """

        resp = self._get(
            API_ACTIONS["get_workflow_run_logs"],
            query_params=_models.GetWorkflowRunLogsRequest(
                workflowRunId=wf_run_id
            ).dict(),
        )

        if resp.status_code == codes.NOT_FOUND:
            raise _exceptions.WorkflowRunLogsNotFound(wf_run_id)
        elif resp.status_code == codes.BAD_REQUEST:
            raise _exceptions.InvalidWorkflowRunID(wf_run_id)

        _handle_common_errors(resp)

        # TODO: unzip, get logs (ORQSDK-652)
        return resp.content

    def get_task_run_logs(self, task_run_id: _models.TaskRunID) -> bytes:
        """
        Gets the logs of a task run from the workflow driver

        Raises:
            InvalidTokenError: see the exception's docstring
            ForbiddenError: see the exception's docstring
            UnknownHTTPError: see the exception's docstring
        """

        resp = self._get(
            API_ACTIONS["get_task_run_logs"],
            query_params=_models.GetTaskRunLogsRequest(taskRunId=task_run_id).dict(),
        )

        # TODO: Handle other errors, not specified in spec yet (ORQSDK-655)
        _handle_common_errors(resp)

        # TODO: unzip, get logs (ORQSDK-654)
        return resp.content
