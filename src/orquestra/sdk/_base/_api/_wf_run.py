################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################

"""User-facing ``WorkflowRun`` object.

WorkflowRun represents, and allows interaction with, an individual workflow run.
"""

import os
import re
import sys
import time
import typing as t
import warnings
from datetime import timedelta
from functools import cached_property
from pathlib import Path
from urllib.parse import urlparse

from ..._base import _config, _exec_ctx
from ...exceptions import (
    ConfigFileNotFoundError,
    ConfigNameNotFoundError,
    ProjectInvalidError,
    QERemoved,
    RayNotRunningError,
    RemoteConnectionError,
    RuntimeQuerySummaryError,
    UnauthorizedError,
    VersionMismatch,
    WorkflowRunCanNotBeTerminated,
    WorkflowRunNotFinished,
    WorkflowRunNotFoundError,
    WorkflowRunNotSucceeded,
    WorkspacesNotSupportedError,
)
from ...schema import ir
from ...schema.configs import ConfigName
from ...schema.responses import WorkflowResult
from ...schema.workflow_run import ProjectId, State
from ...schema.workflow_run import TaskRun as TaskRunModel
from ...schema.workflow_run import TaskRunId
from ...schema.workflow_run import WorkflowRun as WorkflowRunModel
from ...schema.workflow_run import (
    WorkflowRunId,
    WorkflowRunMinimal,
    WorkflowRunSummary,
    WorkspaceId,
)
from .. import _env, serde
from .._graphs import iter_invocations_topologically
from .._logs._interfaces import WorkflowLogs
from .._spaces._resolver import resolve_studio_ref, resolve_studio_workspace_ref
from .._spaces._structs import ProjectRef
from ..abc import RuntimeInterface
from ._config import RuntimeConfig, resolve_config
from ._task_run import TaskRun


class WorkflowRun:
    """Represents a single "execution" of a workflow.

    Used to get the workflow results.
    """

    @classmethod
    def by_id(
        cls,
        run_id: str,
        config: t.Optional[t.Union["RuntimeConfig", str]] = None,
        project_dir: t.Optional[t.Union[Path, str]] = None,
    ) -> "WorkflowRun":
        """Get the WorkflowRun corresponding to a previous workflow run.

        Args:
            run_id: The id of the workflow run to be loaded.
            config: Determines where to look for the workflow run record. If omitted,
                we will retrieve the config name from a local cache of workflow runs
                submitted from this machine.
            project_dir: DEPRECATED

        Raises:
            orquestra.sdk.exceptions.RuntimeQuerySummaryError: when ``config``
                wasn't passed, and it wasn't possible to infer the runtime
                matching ``wf_run_id``.
            orquestra.sdk.exceptions.WorkflowRunNotFoundError: when ``config`` was
                passed, but a matching workflow run couldn't be found.
            orquestra.sdk.exceptions.UnauthorizedError: when ``config`` was passed,
                but authorization with the remote runtime failed when getting the run
                details.
            orquestra.sdk.exceptions.ConfigFileNotFoundError: when the config file
                couldn't be read.
            orquestra.sdk.exceptions.ConfigNameNotFoundError: when there's no
                corresponding config entry in the config file.
        """
        if project_dir:
            warnings.warn(
                "project_dir argument is deprecated and will be removed"
                "in upcoming versions of orquestra-sdk"
            )

        # Resolve config
        resolved_config: RuntimeConfig
        if config is None:
            # Shorthand: query all known runtimes.
            try:
                resolved_config = _find_config_for_workflow(
                    wf_run_id=run_id,
                )
            except (
                RuntimeQuerySummaryError,
                ConfigNameNotFoundError,
                ConfigFileNotFoundError,
            ):
                raise
        else:
            resolved_config = resolve_config(config)

        # Retrieve workflow def from the runtime:
        # - Ray stores wf def for us under a metadata entry.
        # - CE will have endpoints for getting [wf def] by [wf run ID]. See:
        #   https://zapatacomputing.atlassian.net/browse/ORQP-1317
        runtime = resolved_config._get_runtime()
        try:
            wf_run_model = runtime.get_workflow_run_status(run_id)
        except (UnauthorizedError, WorkflowRunNotFoundError):
            raise

        workflow_run = WorkflowRun(
            run_id=run_id,
            wf_def=wf_run_model.workflow_def,
            runtime=runtime,
            config=resolved_config,
        )

        return workflow_run

    @classmethod
    def start_from_ir(
        cls,
        wf_def: ir.WorkflowDef,
        config: t.Union[RuntimeConfig, str],
        workspace_id: t.Optional[WorkspaceId] = None,
        project_id: t.Optional[ProjectId] = None,
        dry_run: bool = False,
        project_dir: t.Optional[t.Union[str, Path]] = None,
    ):
        """Start workflow run from its IR representation.

        Args:
            wf_def: IR definition of a workflow.
            config: SDK needs to know where to execute the workflow. The config
                contains the required details. This can be a RuntimeConfig object, or
                the name of a saved configuration.
            workspace_id: ID of the workspace for workflow - supported only on CE
            project_id: ID of the project for workflow - supported only on CE
            dry_run: Run the workflow without actually executing any task code.
                Useful for testing infrastructure, dependency imports, etc.
            project_dir: DEPRECATED
        """
        if project_dir:
            warnings.warn(
                "project_dir argument is deprecated and will be removed"
                "in upcoming versions of orquestra-sdk"
            )

        _config = resolve_config(config)

        runtime = _config._get_runtime()

        assert runtime is not None

        _project: t.Optional[ProjectRef] = resolve_studio_ref(
            workspace_id,
            project_id,
        )

        wf_run = cls._start(
            wf_def=wf_def,
            runtime=runtime,
            config=_config,
            project=_project,
            dry_run=dry_run,
        )

        return wf_run

    @classmethod
    def _start(
        cls,
        wf_def: ir.WorkflowDef,
        runtime: RuntimeInterface,
        config: t.Optional[RuntimeConfig],
        dry_run: bool,
        project: t.Optional[ProjectRef] = None,
    ):
        """Schedule workflow for execution and return WorkflowRun."""
        run_id = runtime.create_workflow_run(wf_def, project, dry_run)

        workflow_run = WorkflowRun(
            run_id=run_id,
            wf_def=wf_def,
            runtime=runtime,
            config=config,
        )

        return workflow_run

    def __init__(
        self,
        run_id: WorkflowRunId,
        wf_def: ir.WorkflowDef,
        runtime: RuntimeInterface,
        config: t.Optional["RuntimeConfig"] = None,
    ):
        """Initialiser for the WorkflowRun class.

        Note:
        Users aren't expected to use __init__() directly.
        Please use ``WorkflowRun.by_id`` or ``WorkflowDef.run()``.

        Args:
            run_id: The ID of this workflow run.
            wf_def: the workflow being run. Workflow definition in the model
                (serializable) form.
            runtime: the adapter object used to interact with the runtime to
                submit workflow, get results, etc. Different "runtimes" like
                Ray or Compute Engine have corresponding classes.
            config: the configuration defining the runtime by which this workflow run
                was executed.
        """
        self._run_id = run_id
        self._wf_def = wf_def
        self._runtime = runtime
        self._config = config

    def __str__(self) -> str:
        outstr: str = ""

        outstr += f"WorkflowRun '{self._run_id}' with parameters:"
        if self._config is None:
            outstr += "\n- Runtime: In-process runtime."
        else:
            outstr += f"\n- Config name: {self._config.name}"
            outstr += f"\n- Runtime name: {self._config._runtime_name}"
        return outstr

    @property
    def config(self):
        """The configuration for this workflow run."""
        if self._config is None:
            no_config_message = (
                "This workflow run was created without a runtime configuration. "
            )
            no_config_message += "The default in-process runtime was used."
            warnings.warn(no_config_message)

        return self._config

    @property
    def run_id(self):
        """The run_id for this workflow run."""
        return self._run_id

    @cached_property
    def project(self):
        """Get the project and workspace id of a workflowrun.

        Currently supported only on CE.

        Raises:
            orquestra.sdk.exceptions.WorkspacesNotSupportedError: when runtime
                does not support workspaces and projects
        """
        try:
            return self._runtime.get_workflow_project(self.run_id)
        except WorkspacesNotSupportedError:
            raise

    def wait_until_finished(
        self, frequency: float = 0.25, verbose: bool = True
    ) -> State:
        """Block until the workflow run finishes.

        This method draws no distinctions between whether the workflow run completes
        successfully, fails, or is terminated for any other reason.

        Args:
            frequency: The frequency in Hz at which the status should be checked.
            verbose: If ``True``, each iteration of the polling loop will print to
                stderr.

        Returns:
            orquestra.sdk.schema.workflow_run.State: The state of the finished workflow.
        """
        assert frequency > 0.0, "Frequency must be a positive non-zero value"

        status_model = self.get_status_model()
        status = status_model.status.state

        while not status.is_completed():
            sleep_time = 1.0 / frequency

            if verbose:
                print(
                    f"{self.run_id} is {status.name}. Sleeping for {sleep_time}s...",
                    file=sys.stderr,
                )

            time.sleep(sleep_time)
            status_model = self.get_status_model()
            status = status_model.status.state

        if verbose:
            print(
                f"{self.run_id} is {status.name}"
                if status_model.message is None
                else f"{self.run_id} is {status.name} - {status_model.message}",
                file=sys.stderr,
            )

        return status

    def stop(self, *, force: t.Optional[bool] = None):
        """Asks the runtime to stop the workflow run.

        Args:
            force: Asks the runtime to terminate the workflow without waiting for the
                workflow to gracefully exit.
                By default, this behavior is up to the runtime, but can be overridden
                with True/False.

        Raises:
            orquestra.sdk.exceptions.UnauthorizedError: when communication with runtime
                failed because of an auth error
            orquestra.sdk.exceptions.WorkflowRunCanNotBeTerminated: if the termination
                attempt failed
        """
        try:
            self._runtime.stop_workflow_run(self.run_id, force=force)
        except (UnauthorizedError, WorkflowRunCanNotBeTerminated):
            raise

    def get_status(self) -> State:
        """Return the current status of the workflow."""
        return self.get_status_model().status.state

    def get_status_model(self) -> WorkflowRunModel:
        """Serializable representation of the workflow run state.

        This reflects the state of the workflow run at a given point in time.
        """
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=VersionMismatch)
            return self._runtime.get_workflow_run_status(self.run_id)

    def get_results_serialized(self, wait: bool = False) -> t.Sequence[WorkflowResult]:
        """Retrieves workflow results in serialized form.

        Result value is a sequence of WorkflowResult objects where each can be
        deserialized separately

        Args:
            wait: whether or not to wait for workflow run completion.
                Uses the default options for waiting, use ``wait_until_finished()`` for
                more control.

        Raises:
            WorkflowRunNotFinished: when the workflow run has not finished and `wait` is
                False
            WorkflowRunNotSucceeded: when the workflow is no longer executing, but it
                did not succeed.
        """  # noqa 501
        if wait:
            self.wait_until_finished()

        if not (state := self.get_status()).is_completed():
            raise WorkflowRunNotFinished(
                f"Workflow run with id {self.run_id} has not finished. "
                f"Current state: {state}",
                state,
            )
        try:
            return self._runtime.get_workflow_run_outputs_non_blocking(self.run_id)
        except WorkflowRunNotSucceeded:
            raise

    def get_results(self, wait: bool = False) -> t.Sequence[t.Any]:
        """
        Retrieves workflow results, as returned by the workflow function.

        A workflow function is expected to return task outputs
        (ArtifactFutures) or constants (10, "hello", etc.). This method returns values
        of these. The order is dictated by the return statement in the workflow
        function, for example `return a, b, c` means this function returns (a, b, c).
        See also:
        https://docs.orquestra.io/docs/core/sdk/guides/workflow-syntax.html/workflow-syntax.html

        Args:
            wait:  whether or not to wait for workflow run completion.
                   Uses the default options for waiting, use ``wait_until_finished()`` for
                   more control.

        Raises:
            WorkflowRunNotFinished: when the workflow run has not finished and ``wait`` is
                                   False
            WorkflowRunNotSucceeded: when the workflow is no longer executing, but it did not
                succeed.
        """  # noqa 501
        try:
            serialized_results = self.get_results_serialized(wait=wait)
        except WorkflowRunNotSucceeded:
            raise

        results = (*(serde.deserialize(o) for o in serialized_results),)

        # If we only get one result back, return it directly rather than as a sequence
        if len(results) == 1:
            return results[0]

        return results

    def get_artifacts_serialized(
        self,
    ) -> t.Mapping[ir.TaskInvocationId, WorkflowResult]:
        """Unstable: this API will change.

        Returns values calculated by this workflow's tasks in serialized form.
        If a given task hasn't succeeded yet, the mapping won't
        contain the corresponding entry.

        Returns:
            A dictionary with an entry for each task run in the workflow. The key is the
                task's invocation ID. The value is whatever the task returned
                in serialized form
        """
        return self._runtime.get_available_outputs(self.run_id)

    def get_artifacts(self) -> t.Mapping[ir.TaskInvocationId, t.Any]:
        """Unstable: this API will change.

        Returns values calculated by this workflow's tasks. If a given task hasn't
        succeeded yet, the mapping won't contain the corresponding entry.

        Returns:
            A dictionary with an entry for each task run in the workflow. The key is the
                task's invocation ID. The value is whatever the task returned. If the
                task has 1 output, it's the dict entry's value. If the tasks has n
                outputs, the dict entry's value is a n-tuple.
        """
        inv_outputs = self.get_artifacts_serialized()

        # The output shape differs across runtimes when the workflow functions returns a
        # single, packed future. See more in:
        # https://zapatacomputing.atlassian.net/browse/ORQSDK-801
        return {
            inv_id: serde.deserialize(inv_output)
            for inv_id, inv_output in inv_outputs.items()
        }

    def get_artifact_serialized(
        self, task_invocation_id: ir.TaskInvocationId
    ) -> WorkflowResult:
        """Unstable: this API is not implemented for all runtimes yet.

        Returns serialized value calculated by given task if the task is finished.
        Raises exception if given task did not finish yet.
        """
        return self._runtime.get_output(self.run_id, task_invocation_id)

    def get_artifact(self, task_invocation_id: ir.TaskInvocationId) -> t.Any:
        """Unstable: this API is not implemented for all runtimes yet.

        Returns value calculated by given task if the task is finished.
        Raises exception if given task did not finish yet.
        """
        serialized_artifact = self.get_artifact_serialized(task_invocation_id)
        return serde.deserialize(serialized_artifact)

    def get_logs(self) -> WorkflowLogs:
        """Unstable: this API will change.

        Returns logs produced this workflow. See ``WorkflowLogs`` attributes for log
        categories or ``TaskRun.get_logs()`` for logs related to only a single task.
        """
        return self._runtime.get_workflow_logs(wf_run_id=self.run_id)

    @classmethod
    def _task_matches_schema_filters(
        cls,
        task_run_model: TaskRunModel,
        state: t.Optional[t.Union[State, t.List[State]]] = None,
        task_run_id: t.Optional[t.Union[str, TaskRunId]] = None,
        task_invocation_id: t.Optional[t.Union[str, ir.TaskInvocationId]] = None,
    ) -> bool:
        """Filters that can be applied to orquestra.sdk.schema.workflow_run.TaskRun."""
        if state:
            states: t.List[State]
            if isinstance(state, State):
                states = [state]
            else:
                states = state
            if task_run_model.status.state not in states:
                return False

        if task_run_id and not re.compile(task_run_id).fullmatch(task_run_model.id):
            return False

        if task_invocation_id and not re.compile(task_invocation_id).fullmatch(
            task_run_model.invocation_id
        ):
            return False

        return True

    @classmethod
    def _task_matches_api_filters(
        cls,
        task_run: TaskRun,
        task_fn_name: t.Optional[str] = None,
    ) -> bool:
        """Filters that can applied to orquestra.sdk._base._api._task_run.TaskRun."""
        if task_fn_name and not re.compile(task_fn_name).fullmatch(task_run.fn_name):
            return False
        return True

    def get_tasks(
        self,
        *,
        state: t.Optional[t.Union[State, t.List[State]]] = None,
        function_name: t.Optional[str] = None,
        task_run_id: t.Optional[t.Union[str, TaskRunId]] = None,
        task_invocation_id: t.Optional[t.Union[str, ir.TaskInvocationId]] = None,
    ) -> t.List[TaskRun]:
        """Returns TaskRun representations of the tasks executed in this workflow.

        Args:
            state: If specified, only tasks with matching states will be returned.
            function_name: A function name, or regex string matching the desired
                function name(s). If specified, only tasks with matching function names
                will be returned.
            task_run_id: A task run ID, or regex string matching the desired task run
                ID(s). If specified, only tasks with matching task run IDs will be
                returned.
            task_invocation_id: A task invocation ID, or regex string matching the
                desired task invocation ID(s). If specified, only tasks with matching
                task invocation IDs will be returned.

        Returns:
            An iterable of TaskRuns
        """
        wf_run_model: WorkflowRunModel = self.get_status_model()
        wf_ir = self._wf_def
        sorted_invs: t.List[ir.TaskInvocationId] = [
            inv.id for inv in iter_invocations_topologically(wf_ir)
        ]
        task_runs: t.Mapping[ir.TaskInvocationId, TaskRunModel] = {
            task_run.invocation_id: task_run for task_run in wf_run_model.task_runs
        }
        sorted_task_runs = [task_runs[inv_id] for inv_id in sorted_invs]

        tasks = []
        for task_model in sorted_task_runs:
            if not self._task_matches_schema_filters(
                task_model,
                state=state,
                task_run_id=task_run_id,
                task_invocation_id=task_invocation_id,
            ):
                continue
            task = TaskRun(
                task_run_id=task_model.id,
                task_invocation_id=task_model.invocation_id,
                workflow_run_id=self.run_id,
                runtime=self._runtime,
                wf_def=self._wf_def,
            )
            if not self._task_matches_api_filters(
                task,
                task_fn_name=function_name,
            ):
                continue
            tasks.append(task)

        return tasks


def _handle_common_listing_project_errors(
    project: t.Optional[ProjectId], workspace: t.Optional[WorkspaceId]
):
    """Handle common errors when specifying a project for listing wfs.

    Args:
        project: ID of a specific project.
        workspace: ID of a specific workspace.

    Raises:
        ProjectInvalidError: When a project is specified without a workspace.
    """
    if project and not workspace:
        raise ProjectInvalidError(
            f"The project `{project}` cannot be uniquely identified "
            "without a workspace parameter."
        )

    if project:
        warnings.warn(
            "`project` parameter in `list_workflow_runs` is deprecated and will be "
            "removed in the next release.",
            FutureWarning,
        )

    # Null project - it is ignored by platform anyway - left as parameter for
    # backward compatibility only
    return None


def _find_config_for_workflow(wf_run_id: WorkflowRunId) -> RuntimeConfig:
    not_found_configs: t.List[RuntimeConfig] = []
    unauthorized_configs: t.List[RuntimeConfig] = []
    not_running_configs: t.List[RuntimeConfig] = []

    config_names = RuntimeConfig.list_configs()
    for config_name in config_names:
        config_obj = RuntimeConfig.load(config_name)
        try:
            runtime = config_obj._get_runtime()
        except RayNotRunningError:
            # Ray connection is set up in `RayRuntime.__init__()`. We'll get the
            # exception when runtime object is created.
            not_running_configs.append(config_obj)
            continue
        except QERemoved:
            # When someone has old QE config stored in their config file
            # adding it to unauthorized as someone might re-login into cluster using CE
            unauthorized_configs.append(config_obj)
            continue

        try:
            _ = runtime.get_workflow_run_status(wf_run_id)
        except WorkflowRunNotFoundError:
            not_found_configs.append(config_obj)
            continue
        except UnauthorizedError:
            # TODO (ORQSDK-990): short circuit remote call by checking token validity
            # on the client side
            unauthorized_configs.append(config_obj)
            continue
        except RemoteConnectionError:
            # Cluster might be decommissioned or temporarily unavailable
            unauthorized_configs.append(config_obj)
            continue

        # We're here = the runtime knows about this run ID.
        return config_obj

    raise RuntimeQuerySummaryError(
        wf_run_id=wf_run_id,
        not_found_runtimes=[_make_runtime_info(config) for config in not_found_configs],
        unauthorized_runtimes=[
            _make_runtime_info(config) for config in unauthorized_configs
        ],
        not_running_runtimes=[
            _make_runtime_info(config) for config in not_running_configs
        ],
    )


def _make_runtime_info(config: RuntimeConfig) -> RuntimeQuerySummaryError.RuntimeInfo:
    try:
        uri = getattr(config, "uri")
    except AttributeError:
        uri = None
    return RuntimeQuerySummaryError.RuntimeInfo(
        runtime_name=config._runtime_name,
        config_name=config.name,
        server_uri=uri,
    )


def list_workflow_run_summaries(
    config: t.Union[ConfigName, "RuntimeConfig"],
    *,
    limit: t.Optional[int] = None,
    max_age: t.Optional[str] = None,
    state: t.Optional[t.Union[State, t.List[State]]] = None,
    workspace: t.Optional[WorkspaceId] = None,
    project: t.Optional[ProjectId] = None,
) -> t.List[WorkflowRunSummary]:
    """List summaries of the workflow runs, with some filters.

    Note: this function returns ``WorkflowRunSummary`` objects that are static overviews
    of the workflow runs. If you need to perform operations on the workflows, you
    probably want ``sdk.list_workflow_runs()``.

    Args:
        config: The name of the configuration to use.
        limit: Restrict the number of runs to return, prioritising the most recent.
        max_age: Only return runs younger than the specified maximum age.
        state: Only return runs of runs with the specified status.
        workspace: Only return runs from the specified workspace when using CE.
        project: will be used to list workflows from specific workspace and project
            when using CE.

    Raises:
        ConfigNameNotFoundError: when the named config is not found in the file.
        NotImplementedError: when a filter is specified for a runtime that does not
            support it.
    """
    _handle_common_listing_project_errors(project, workspace)
    workspace = resolve_studio_workspace_ref(workspace_id=workspace)

    # Resolve config
    try:
        resolved_config: RuntimeConfig = resolve_config(config)
    except ConfigNameNotFoundError:
        raise

    # resolve runtime
    runtime = resolved_config._get_runtime()

    # Grab the workflow summaries from the runtime.
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=VersionMismatch)
        try:
            run_summaries: t.List[
                WorkflowRunSummary
            ] = runtime.list_workflow_run_summaries(
                limit=limit,
                max_age=_parse_max_age(max_age),
                state=state,
                workspace=workspace,
            )
        except NotImplementedError:
            raise

    return run_summaries


def list_workflow_runs(
    config: t.Union[ConfigName, "RuntimeConfig"],
    *,
    limit: t.Optional[int] = None,
    max_age: t.Optional[str] = None,
    state: t.Optional[t.Union[State, t.List[State]]] = None,
    project_dir: t.Optional[t.Union[Path, str]] = None,
    workspace: t.Optional[WorkspaceId] = None,
    project: t.Optional[ProjectId] = None,
) -> t.List[WorkflowRun]:
    """List the workflow runs, with some filters.

    Note: this function returns a list of full ``WorkflowRun`` objects.
    This will allow you to iterate through workflow runs performing actions on them,
    e.g. stopping all workflows older than a month.
    If you want an overview of workflows without necessarily needing to interact with
    them, ``sdk.list_workflow_run_summaries()`` is a more efficient alternative.

    Args:
        config: The name of the configuration to use.
        limit: Restrict the number of runs to return, prioritising the most recent.
        max_age: Only return runs younger than the specified maximum age.
        state: Only return runs of runs with the specified status.
        project_dir: DEPRECATED
        workspace: Only return runs from the specified workspace when using CE.
        project: will be used to list workflows from specific workspace and project
            when using CE.

    Raises:
        ConfigNameNotFoundError: when the named config is not found in the file.
        NotImplementedError: when a filter is specified for a runtime that does not
            support it.

    Returns:
        a list of WorkflowRuns
    """
    # TODO: update docstring when platform workspace/project filtering is merged [ORQP-1479](https://zapatacomputing.atlassian.net/browse/ORQP-1479?atlOrigin=eyJpIjoiZWExMWI4MDUzYTI0NDQ0ZDg2ZTBlNzgyNjE3Njc4MDgiLCJwIjoiaiJ9) # noqa: E501

    if project_dir:
        warnings.warn(
            "project_dir argument is deprecated and will be removed"
            "in upcoming versions of orquestra-sdk"
        )

    _handle_common_listing_project_errors(project, workspace)
    workspace = resolve_studio_workspace_ref(workspace_id=workspace)

    # Resolve config
    try:
        resolved_config: RuntimeConfig = resolve_config(config)
    except ConfigNameNotFoundError:
        raise
    # If user wasn't specific with workspace and project, we might want to resolve it

    # resolve runtime
    runtime = resolved_config._get_runtime()

    # Grab the "workflow runs" from the runtime.
    # Note: WorkflowRun means something else in runtime land. To avoid overloading, this
    #       import is aliased to WorkflowRunStatus in here.
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=VersionMismatch)
        try:
            run_statuses: t.Sequence[WorkflowRunMinimal] = runtime.list_workflow_runs(
                limit=limit,
                max_age=_parse_max_age(max_age),
                state=state,
                workspace=workspace,
            )
        except NotImplementedError:
            raise

    # We need to convert to the public API notion of a WorkflowRun
    runs = []
    for run_status in run_statuses:
        assert run_status.workflow_def is not None
        workflow_run = WorkflowRun(
            run_id=run_status.id,
            wf_def=run_status.workflow_def,
            runtime=runtime,
            config=resolved_config,
        )
        runs.append(workflow_run)
    return runs


def _parse_max_age(age: t.Optional[str]) -> t.Optional[timedelta]:
    """Parse a string specifying an age into a timedelta object.

    Args:
        age: the string to be parsed.

    Raises:
        ValueError: if the age string cannot be parsed

    Returns:
        datetime.timedelta: the age specified by the 'age' string, as a timedelta.
        None: if age is None
    """
    if age is None:
        return None
    time_params = {}

    # time in format "{days}d{hours}h{minutes}m{seconds}s"

    # match one or more digits "\d+?" followed by a single character from the 'units'
    # list. Capture the digits in a group labelled 'name'
    time_capture_group = r"(?P<{name}>\d+?)[{units}]"

    re_string = ""
    name_units = (
        ("days", "dD"),
        ("hours", "hH"),
        ("minutes", "mM"),
        ("seconds", "sS"),
    )
    for name, units in name_units:
        # match each potential time unit capture group between 0 and 1 times.
        re_string += rf"({time_capture_group.format(name=name, units=units)})?"

    if parts := re.compile(re_string).fullmatch(age):
        for name, param in parts.groupdict().items():
            if param:
                time_params[name] = int(param)
        if len(time_params) > 0:
            return timedelta(**time_params)

    raise ValueError(
        'Time strings must be in the format "{days}d{hours}h{minutes}m{seconds}s". '
        "Accepted units are:\n"
        "- d or D: days\n"
        "- h or H: hours\n"
        "- m or M: minutes\n"
        "- s or S: seconds\n"
        "For example:\n"
        '- "9h32m" = 9 hours and 32 minutes,\n'
        '- "8H6S" = 8 hours and 6 seconds,\n'
        '- "10m" = 10 minutes,\n'
        '- "3D6h8M13s" = 3 days, 6 hours, 8 minutes and 13 seconds.'
    )


def _get_workspace_and_project_ids() -> (
    t.Tuple[t.Optional[WorkspaceId], t.Optional[ProjectId]]
):
    if not os.getenv(_env.CURRENT_CLUSTER_ENV):
        return None, None

    return os.getenv(_env.CURRENT_WORKSPACE_ENV), os.getenv(_env.CURRENT_PROJECT_ENV)


def _generate_cluster_uri_name(uri: str) -> str:
    return str(urlparse(uri).path).split(".")[0]


def _get_config_context() -> str:
    cluster_uri = os.getenv(_env.CURRENT_CLUSTER_ENV)
    if not cluster_uri:
        context = _exec_ctx.get_current_exec_context()
        if context == _exec_ctx.ExecContext.RAY:
            return _config.RAY_CONFIG_NAME_ALIAS
        elif context == _exec_ctx.ExecContext.DIRECT:
            return _config.IN_PROCESS_CONFIG_NAME
        else:
            raise NotImplementedError(
                f"Got unexpected global context {context}. Please report this as a bug."
            )

    return _generate_cluster_uri_name(cluster_uri)


class CurrentWorkflowIDs(t.NamedTuple):
    config_name: ConfigName
    workspace_id: t.Optional[WorkspaceId]
    project_id: t.Optional[ProjectId]


def current_wf_ids() -> CurrentWorkflowIDs:
    """Get the backend IDs related to current workflow execution context.

    config_name is the name of the config used to execute current workflow.

    workspace_id  and project_id are the workspace and project
    in which the workflow is executed. If run locally, they are set to None.

    This function is intended to be used within the task code in the following way::

        @sdk.task
        def t():
            config_name, workspace_id, project_id =  sdk.current_wf_ids()
            ...

    Returns:
        The config, workspace and project IDs associated with the current
            run, in a named tuple. See: CurrentWorkflowIDs
    """
    config = _get_config_context()
    workspace_id, project_id = _get_workspace_and_project_ids()
    return CurrentWorkflowIDs(
        config_name=config, workspace_id=workspace_id, project_id=project_id
    )
