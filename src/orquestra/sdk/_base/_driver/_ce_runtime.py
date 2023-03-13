################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union

from orquestra.sdk import exceptions
from orquestra.sdk._base import serde
from orquestra.sdk._base._db import WorkflowDB
from orquestra.sdk._base.abc import ArtifactValue, RuntimeInterface
from orquestra.sdk.kubernetes.quantity import parse_quantity
from orquestra.sdk.schema.configs import RuntimeConfiguration
from orquestra.sdk.schema.ir import TaskInvocationId, WorkflowDef
from orquestra.sdk.schema.local_database import StoredWorkflowRun
from orquestra.sdk.schema.workflow_run import (
    State,
    TaskRunId,
    WorkflowRun,
    WorkflowRunId,
    WorkflowRunMinimal,
)

from . import _client, _exceptions, _models


class CERuntime(RuntimeInterface):
    """
    A runtime for communicating with the Compute Engine API endpoints
    """

    def __init__(
        self,
        config: RuntimeConfiguration,
        verbose: bool = False,
    ):
        """
        Args:
            config: contains the runtime configuration, including the name of the
                config being used and the associated runtime options. These options
                control how to connect to a CE cluster.
            verbose: if `True`, CERuntime may print debug information about
                its inner working to stderr.

        Raises:
            RuntimeConfigError: when the config is invalid
        """
        self._config = config
        self._verbose = verbose

        # We're using a reusable session to allow shared headers
        # In the future we can store cookies, etc too.
        try:
            base_uri = self._config.runtime_options["uri"]
            token = self._config.runtime_options["token"]
        except KeyError as e:
            raise exceptions.RuntimeConfigError(
                "Invalid CE configuration. Did you login first?"
            ) from e

        self._client = _client.DriverClient.from_token(base_uri=base_uri, token=token)

    @classmethod
    def from_runtime_configuration(
        cls, project_dir: Path, config: RuntimeConfiguration, verbose: bool
    ) -> "RuntimeInterface":
        """Returns an initilaised version of the class from a runtime configuration"""
        return cls(config, verbose)

    def create_workflow_run(
        self,
        workflow_def: WorkflowDef,
    ) -> WorkflowRunId:
        """
        Schedules a workflow definition for execution

        Args:
            workflow_def: the IR of the workflow to run

        Raises:
            WorkflowSyntaxError: when the workflow definition was rejected by the remote
                cluster
            WorkflowRunNotStarted: for all other errors that prevented the workflow run
                from being scheduled
            UnauthorizedError: if the current token was rejected by the remote cluster

        Returns:
            the workflow run ID
        """

        max_gpu = None
        max_memory = None
        max_cpu = None
        for inv in workflow_def.task_invocations.values():
            if inv.resources is None:
                continue
            if inv.resources.memory is not None:
                if parse_quantity(inv.resources.memory) > parse_quantity(
                    max_memory or "0"
                ):
                    max_memory = inv.resources.memory
            if inv.resources.cpu is not None:
                if parse_quantity(inv.resources.cpu) > parse_quantity(max_cpu or "0"):
                    max_cpu = inv.resources.cpu
            if inv.resources.gpu is not None:
                if int(inv.resources.gpu) > int(max_gpu or "0"):
                    max_gpu = inv.resources.gpu
        try:
            workflow_def_id = self._client.create_workflow_def(workflow_def)
            workflow_run_id = self._client.create_workflow_run(
                workflow_def_id,
                _models.Resources(cpu=max_cpu, memory=max_memory, gpu=max_gpu),
            )
        except _exceptions.InvalidWorkflowDef as e:
            raise exceptions.WorkflowSyntaxError(f"{e}") from e
        except _exceptions.InvalidWorkflowRunRequest as e:
            raise exceptions.WorkflowRunNotStarted(
                f"Unable to start the workflow run: {e}"
            ) from e
        except (_exceptions.InvalidTokenError, _exceptions.ForbiddenError) as e:
            raise exceptions.UnauthorizedError(f"{e}") from e

        with WorkflowDB.open_db() as db:
            db.save_workflow_run(
                StoredWorkflowRun(
                    workflow_run_id=workflow_run_id,
                    config_name=self._config.config_name,
                    workflow_def=workflow_def,
                )
            )
        return workflow_run_id

    def get_all_workflow_runs_status(self) -> List[WorkflowRun]:
        """Gets the status of all workflow runs."""
        raise NotImplementedError()

    def get_workflow_run_status(self, workflow_run_id: WorkflowRunId) -> WorkflowRun:
        """
        Gets the status of a workflow run

        Args:
            workflow_run_id: the ID of a workflow run

        Raises:
            WorkflowRunNotFound: if the workflow run cannot be found
            UnauthorizedError: if the remote cluster rejects the token

        Returns:
            The status of the workflow run
        """
        try:
            return self._client.get_workflow_run(workflow_run_id)
        except (_exceptions.InvalidWorkflowRunID, _exceptions.WorkflowRunNotFound) as e:
            raise exceptions.WorkflowRunNotFoundError(
                f"Workflow run with id `{workflow_run_id}` not found"
            ) from e
        except (_exceptions.InvalidTokenError, _exceptions.ForbiddenError) as e:
            raise exceptions.UnauthorizedError(f"{e}") from e

    def get_workflow_run_outputs(self, workflow_run_id: WorkflowRunId) -> Sequence[Any]:
        """Returns the output artifacts of a workflow run

        For example, for this workflow:

            @sdk.workflow
            def my_wf():
                return [my_task(), another_task()]

        this method will return an iterable that yields the results from my_task and
        another_task().

        This method blocks until the workflow is completed
        """
        raise NotImplementedError()

    def get_workflow_run_outputs_non_blocking(
        self, workflow_run_id: WorkflowRunId
    ) -> Sequence[Any]:
        """Non-blocking version of get_workflow_run_outputs.

        This method raises exceptions if the workflow output artifacts are not available

        Args:
            workflow_run_id: the ID of a workflow run

        Raises:
            WorkflowRunNotFound: if the workflow run cannot be found
            WorkflowRunNotSucceeded: if the workflow has not yet succeeded
            UnauthorizedError: if the remote cluster rejects the token

        Returns:
            the outputs associated with the workflow run
        """
        try:
            result_ids = self._client.get_workflow_run_results(workflow_run_id)
        except (_exceptions.InvalidWorkflowRunID, _exceptions.WorkflowRunNotFound) as e:
            raise exceptions.WorkflowRunNotFoundError(
                f"Workflow run with id `{workflow_run_id}` not found"
            ) from e
        except (_exceptions.InvalidTokenError, _exceptions.ForbiddenError) as e:
            raise exceptions.UnauthorizedError(f"{e}") from e

        if len(result_ids) == 0:
            wf_run = self.get_workflow_run_status(workflow_run_id)
            raise exceptions.WorkflowRunNotSucceeded(
                f"Workflow run `{workflow_run_id}` is in state {wf_run.status.state}",
                wf_run.status.state,
            )

        assert len(result_ids) == 1, "We're currently expecting a single result."

        try:
            wf_result = self._client.get_workflow_run_result(result_ids[0])
            return tuple(serde.deserialize(wf_result))
        except (_exceptions.InvalidTokenError, _exceptions.ForbiddenError) as e:
            raise exceptions.UnauthorizedError(f"{e}") from e

    def get_available_outputs(
        self, workflow_run_id: WorkflowRunId
    ) -> Dict[TaskInvocationId, Union[ArtifactValue, Tuple[ArtifactValue, ...]]]:
        """Returns all available outputs for a workflow

        This method returns all available artifacts. When the workflow fails
        it returns artifacts only for the steps that did success.
        Might raise an exception if runtime doesn't support getting artifacts from
        in-progress workflow

        Careful: This method does NOT return status of a workflow. Verify it beforehand
        to make sure if workflow failed/succeeded/is running. You might get incomplete
        results

        Args:
            workflow_run_id: the ID of a workflow run

        Raises:
            WorkflowRunNotFound: if the workflow run cannot be found
            UnauthorizedError: if the remote cluster rejects the token

        Returns:
            a mapping between task invocation ID and the available artifacts from the
                matching task run.
        """
        try:
            artifact_map = self._client.get_workflow_run_artifacts(workflow_run_id)
        except (_exceptions.InvalidWorkflowRunID, _exceptions.WorkflowRunNotFound) as e:
            raise exceptions.WorkflowRunNotFoundError(
                f"Workflow run with id `{workflow_run_id}` not found"
            ) from e
        except (_exceptions.InvalidTokenError, _exceptions.ForbiddenError) as e:
            raise exceptions.UnauthorizedError(f"{e}") from e

        artifact_vals: Dict[
            TaskInvocationId, Union[ArtifactValue, Tuple[ArtifactValue]]
        ] = {}

        for task_run_id, artifact_ids in artifact_map.items():
            inv_id = self._invocation_id_by_task_run_id(workflow_run_id, task_run_id)
            outputs = []
            for artifact_id in artifact_ids:
                try:
                    output = serde.deserialize(
                        self._client.get_workflow_run_artifact(artifact_id)
                    )
                except Exception:
                    # If we fail for any reason, this artifact wasn't available yet
                    continue
                outputs.append(output)

            if len(outputs) > 0:
                # We don't want to litter the dictionary with empty containers.
                artifact_vals[inv_id] = tuple(outputs)

        return artifact_vals

    def _invocation_id_by_task_run_id(
        self, wf_run_id: WorkflowRunId, task_run_id: TaskRunId
    ) -> TaskInvocationId:
        # We shouldn't expect any particular format of the task run ID.
        # TODO: use workflow run -> task runs -> invocation ID.
        # https://zapatacomputing.atlassian.net/browse/ORQSDK-694
        return task_run_id.split("@")[-1]

    def stop_workflow_run(self, workflow_run_id: WorkflowRunId) -> None:
        """Stops a workflow run.

        Raises:
            WorkflowRunCanNotBeTerminated if workflow run is cannot be terminated.
        """
        try:
            self._client.terminate_workflow_run(workflow_run_id)
        except _exceptions.WorkflowRunNotFound:
            raise exceptions.WorkflowRunNotFoundError(
                f"Workflow run with id `{workflow_run_id}` not found"
            )
        except (_exceptions.InvalidTokenError, _exceptions.ForbiddenError) as e:
            raise exceptions.UnauthorizedError(f"{e}") from e

    def list_workflow_runs(
        self,
        *,
        limit: Optional[int] = None,
        max_age: Optional[timedelta] = None,
        state: Optional[Union[State, List[State]]] = None,
    ) -> List[WorkflowRunMinimal]:
        """
        List the workflow runs, with some filters

        Args:
            limit: Restrict the number of runs to return, prioritising the most recent.
            max_age: Only return runs younger than the specified maximum age.
            status: Only return runs of runs with the specified status.

        Raises:
            UnauthorizedError: if the remote cluster rejects the token

        Returns:
                A list of the workflow runs
        """
        try:
            # TODO(ORQSDK-684): driver client cannot do filtering via API yet
            runs = self._client.list_workflow_runs()
        except (_exceptions.InvalidTokenError, _exceptions.ForbiddenError) as e:
            raise exceptions.UnauthorizedError(f"{e}") from e
        return runs.contents

    def get_workflow_logs(self, wf_run_id: WorkflowRunId):
        raise NotImplementedError()

    def get_task_logs(self, wf_run_id: WorkflowRunId, task_inv_id: TaskInvocationId):
        raise NotImplementedError()
