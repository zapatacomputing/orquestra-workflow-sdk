################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
import base64
import gzip
import io
import json
import re
import sqlite3
import sys
import tarfile
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import requests

from orquestra.sdk import exceptions
from orquestra.sdk._base import serde
from orquestra.sdk._base._conversions._yaml_exporter import (
    pydantic_to_yaml,
    workflow_to_yaml,
)
from orquestra.sdk._base._db import WorkflowDB
from orquestra.sdk._base.abc import RuntimeInterface
from orquestra.sdk.schema.configs import RuntimeConfiguration
from orquestra.sdk.schema.ir import TaskInvocation, TaskInvocationId, WorkflowDef
from orquestra.sdk.schema.local_database import StoredWorkflowRun
from orquestra.sdk.schema.workflow_run import (
    RunStatus,
    State,
    TaskRun,
    TaskRunId,
    WorkflowRun,
    WorkflowRunId,
)

from . import _client

# From: https://pkg.go.dev/github.com/argoproj/argo/pkg/apis/workflow/v1alpha1#NodePhase
QE_PHASE_ORQ_STATUS = {
    "Pending": State.WAITING,
    "Running": State.RUNNING,
    "Succeeded": State.SUCCEEDED,
    "Skipped": State.TERMINATED,
    "Failed": State.FAILED,
    "Error": State.ERROR,
    # Omitted means a step didn't run
    "Omitted": State.WAITING,
    # Additional phases for workflows
    # see: quantum-engine/api/entity/workflow.go
    "Initializing": State.WAITING,
    "Submitted": State.WAITING,
    "Processing": State.RUNNING,
    "Terminated": State.TERMINATED,
}


def parse_date_or_none(date_str: Optional[str]) -> Optional[datetime]:
    if date_str is None or date_str == "":
        return None
    # QE should only return datetimes ending in Z - i.e. Zulu timezone
    if date_str[-1].capitalize() != "Z":
        raise ValueError("Date is not 'Zulu' timezone (UTC)")
    try:
        # 2022-01-28T18:41:01Z
        dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        # 2022-01-28T18:41:01.12345678Z
        # 2022-02-25T08:53:57.8711442Z
        # Python can only format datetimes with microseconds
        # QE returns dates with a varying number of decimal points
        # JC has seen 6 and over.
        # TODO: This will fail with < 6 decimal places
        matches = re.match(r".*\.(\d{6})", date_str)
        # This format is standard to QE and should always result in a match
        if matches is None:
            raise ValueError(f"Date not in expected format: {date_str}. Try RFC3339!")
        microseconds = matches.group(1)
        head, sep, _ = date_str.rpartition(".")
        _date_str = f"{head}{sep}{microseconds}Z"
        dt = datetime.strptime(_date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    # Rebuild the datetime object with "Zulu" aka UTC timezone
    return datetime.combine(dt.date(), dt.time(), timezone.utc)


def _get_nodes_from_argo_representation(json_representation: Dict[str, Any]):
    try:
        json_status = json_representation["status"]
    except KeyError:
        return {}
    if "nodes" in json_status:
        return json_status["nodes"]
    if "compressedNodes" in json_status:
        compressed_nodes = base64.b64decode(json_status["compressedNodes"])
        decompressed_nodes = gzip.decompress(compressed_nodes)
        return json.loads(decompressed_nodes)
    return {}


def _argo_node_to_task_run(node, *, workflow_state: State) -> TaskRun:
    """Converts between an Argo/QE node to the SDK schema for a TaskRun"""

    # We use a heuristic to find the task run's state.
    # 0. Prereq: the workflow state is available
    # 1. Convert the Argo node phase to TaskRun state
    # 2. If the workflow state is TERMINATED or ERROR:
    #    a. If the TaskRun state is RUNNING, use the workflow state
    #    b. If the TaskRun state is WAITING, use TERMINATED
    #    c. Otherwise, use the TaskRun state
    # 3. Otherwise, use the TaskRun state
    #
    # Note: If there are multiple RUNNING task runs and the workflow is in the ERROR
    #       state, the exact task run that caused the error will be hard to pinpoint.
    # TODO(JC): Find an example of a workflow that has multiple RUNNING task runs with
    #           one resulting in a workflow ERROR.
    #           See: https://zapatacomputing.atlassian.net/browse/ORQSDK-589

    # 1. Convert the Argo phase to TaskRun state
    task_run_state = QE_PHASE_ORQ_STATUS[node["phase"]]

    # 2. If the workflow state is TERMINATED or ERROR
    if workflow_state in (State.TERMINATED, State.ERROR):
        # 2a. If the TaskRun state is RUNNING, use the workflow state
        if task_run_state == State.RUNNING:
            task_run_state = workflow_state
        # 2b. If the TaskRun state is WAITING, use TERMINATED
        elif task_run_state == State.WAITING:
            task_run_state = State.TERMINATED
    ### End "state" heuristic

    task_status = RunStatus(
        state=task_run_state,
        start_time=parse_date_or_none(node.get("startedAt", None)),
        end_time=parse_date_or_none(node.get("finishedAt", None)),
    )
    return TaskRun(
        id=node["id"],
        invocation_id=node["templateName"],
        status=task_status,
        message=node.get("message"),
    )


def _parse_workflow_run_representation(
    json_representation: Dict[str, Any],
    workflow_run_id: WorkflowRunId,
    wf_def: WorkflowDef,
    qe_status: str,
) -> WorkflowRun:
    """
    Parses an Argo "currentRepresentation" from QE and gets the state of a workflow run
    Args:
        json_representation: the json.loads(...) of the Argo representation
        workflow_run_id: the workflow run ID we care about
        wf_def: the original workflow definition
    Returns:
        the WorkflowRun parsed from the Argo representation
    """
    # QE can get out of sync with the Argo representation.
    # This happens when QE terminates a workflow and Argo doesn't update the
    # representation
    # For this reason, we take the state from the QE response.
    workflow_state = QE_PHASE_ORQ_STATUS[qe_status]
    nodes = _get_nodes_from_argo_representation(json_representation)

    # The workflow's status is different found differently than each task run.
    #  - The "state" is taken from the QE response, instead of using a heuristic.
    #  - We pull the start and end time from the Argo status, not the Argo node.
    workflow_status = RunStatus(
        state=workflow_state,
        start_time=parse_date_or_none(json_representation["status"].get("startedAt")),
        end_time=parse_date_or_none(json_representation["status"].get("finishedAt")),
    )

    # Task run statuses are built from the nodes in the Argo representation
    # - The state is found using a heuristic, see _argo_node_to_task_run
    # - The start and end time is from the Argo node, see _argo_node_to_task_run
    task_runs = [
        _argo_node_to_task_run(node, workflow_state=workflow_state)
        for node in nodes.values()
        if node["type"] != "DAG" and node["templateName"] != "Finalizing-your-data"
    ]
    return WorkflowRun(
        id=workflow_run_id,
        workflow_def=wf_def,
        task_runs=task_runs,
        status=workflow_status,
    )


def _get_task_invocations(wf_def: WorkflowDef) -> Dict[str, TaskInvocation]:
    """
    Returns a dictionary of:
        Normalized task invocation id: task invocation object
    """
    return {
        task_invocation_id: task_invocation
        for task_invocation_id, task_invocation in wf_def.task_invocations.items()
    }


def _parse_workflow_result(result_bytes: bytes) -> Dict[str, Dict]:
    """Parse a workflow run result

    Args:
        result_bytes: bytes received from QE from '/v2/workflows/{id}/result'

    Returns:
        A nested dictionary. Level 1 keys are QE step IDs. The level 2 keys
            are QE artifact names as well as:
            - 'inputs'
            - 'stepID'
            - 'stepName'
            - 'workflowId'
    Raises:
        IndexError: when the `result_bytes` is not a archive we expect.
    """

    try:
        return extract_result_json(result_bytes)
    except IndexError as e:
        raise exceptions.NotFoundError("Invalid archive") from e


def extract_result_json(tgz_bytes: bytes) -> Dict:
    # Treat the response as a tgz file and extract the workflow result JSON
    fileobj = io.BytesIO(tgz_bytes)
    with tarfile.open(mode="r:gz", fileobj=fileobj) as tar:
        # TODO: Assuming result is the only file in the tgz
        file_to_extract = tar.getmembers()[0]
        file_buf = tar.extractfile(file_to_extract)
        # Assuming we get a real file
        assert file_buf is not None
        return json.load(file_buf)


def _get_artifacts(
    task_invocations: Dict[str, TaskInvocation],
    workflow_result: Dict[str, Dict],
) -> Dict[str, Dict]:
    """Returns a dictionary of:
    artifact id: dictionary representation of WorkflowResult

    The "WorkflowResult" is a dictionary loaded from JSON for a serialized result of a
    task.
    For more information on the container and how the serialization is done, see:
     - WorkflowResult from orquestra.sdk.schema.responses
     - The module: orquestra.sdk._base.serde
    """
    # Go through the workflow result dict, looking for artifacts
    # For each task run:
    #   1. Find the task invocation id
    #   2. Check the workflow definition for the task invocation's outputs
    #   3. Store these artifacts in a dict
    artifacts = {}
    for _, wf_step in workflow_result.items():
        task_invocation_id = wf_step["stepName"]
        artifacts.update(
            {
                artifact_id: wf_step[artifact_id]
                for artifact_id in task_invocations[task_invocation_id].output_ids
                # this condition checks if the step finished successfully
                if artifact_id in wf_step
            }
        )
    return artifacts


def _find_first(predicate, iterable):
    return next(filter(predicate, iterable))


# This is what we expect for IDs generated by Quantum Engine. This is tightly
# coupled with the QE's rules for generating "workflow ID" and "step ID". In
# short, "step-id = concat(workflow-id, random-chars)"
#
# See also:
# https://github.com/zapatacomputing/quantum-engine/blob/8c5036bba251d8bebf249b543e67898455fdc236/api/workflow/id.go#L23-L50
RUN_ID_PATTERN = re.compile(r"([a-zA-Z0-9_-]+-r\d{3})(?:-(\w+))?")


def parse_run_id(run_id: str) -> Tuple[str, Optional[str]]:
    """
    Raises:
        ValueError: if `run_id` can't be parsed.
    """

    match = re.match(RUN_ID_PATTERN, run_id)
    if match is None:
        raise ValueError(f"'{run_id}' doesn't look like a run ID from Quantum Engine")

    if len(match.groups()) == 1:
        (wf_id,) = match.groups()
        return wf_id, None
    elif len(match.groups()) == 2:
        wf_id, step_id = match.groups()
        return wf_id, step_id
    else:
        raise ValueError(f"'{run_id}' doesn't look like a run ID from Quantum Engine")


def print_yaml_of_workflow(workflow: WorkflowDef):
    """
    Prints the yaml representation of the workflow to stderr
    """
    print(
        "\nYAML representation of the workflow\n"
        "\tThis is a temporary debugging tool and is available while the Orquestra"
        " platform supports YAML files.\n"
        "#---- Beginning of YAML representation of the workflow ----#",
        file=sys.stderr,
    )
    print(
        pydantic_to_yaml(workflow_to_yaml(workflow)),
        file=sys.stderr,
    )
    print(
        "#---- End of YAML representation of the workflow ----#\n",
        file=sys.stderr,
    )


@contextmanager
def _http_error_handling():
    try:
        yield
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == requests.codes.unauthorized:
            raise exceptions.UnauthorizedError(str(e)) from e
        elif e.response.status_code == requests.codes.request_entity_too_large or (
            e.response.status_code == requests.codes.bad_request
            and "Request entity too large" in e.response.text
        ):
            raise exceptions.WorkflowTooLargeError(
                "The submitted workflow is too large to be run on this cluster.\n"
                "Try reducing the size of your workflow either by using fewer tasks or "
                "contact Zapata support for more assistance."
            ) from e
        else:
            raise e


class QERuntime(RuntimeInterface):
    def __init__(
        self,
        config: RuntimeConfiguration,
        project_dir: Path,
        verbose: bool = False,
    ):
        """
        Args:
            config: contains the runtime configuration, including the name of the
                config being used and the associated runtime options. These options
                control how to connect to a QE cluster.
            project_dir: the project directory, either Path-like or a string.
                This is to (de)serialise the WorkflowDef associated with this workflow
                run.
            verbose: if `True`, QERuntime may print debug information about
                its inner working to stderr.

        Raises:
            orquestra.sdk.exceptions.RuntimeConfigError: when config is invalid
        """
        self._config = config
        self._project_dir = project_dir
        self._verbose = verbose

        # We're using a reusable session to allow shared headers
        # In the future we can store cookies, etc too.
        try:
            base_uri = self._config.runtime_options["uri"]
            token = self._config.runtime_options["token"]
        except KeyError:
            raise exceptions.RuntimeConfigError(
                "Invalid QE configuration. Did you login first?"
            )

        session = requests.Session()
        session.headers["Content-Type"] = "application/json; charset=utf-8"
        session.headers["Authorization"] = f"Bearer {token}"
        self._client = _client.QEClient(session=session, base_uri=base_uri)

    @classmethod
    def from_runtime_configuration(
        cls,
        project_dir: Path,
        config: RuntimeConfiguration,
        verbose: bool = False,
    ) -> "QERuntime":
        """
        Args:
            config: contains the runtime configuration, including the name of the
                config being used and the associated runtime options. These options
                control how to connect to a QE cluster.
            project_dir: the project directory, either Path-like or a string.
                This is to (de)serialise the WorkflowDef associated with this workflow
                run.
            verbose: boolean, if TRUE the QERuntime is set to print information about
                        its inner working, useful to debug

        Raises:
            orquestra.sdk.exceptions.RuntimeConfigError: when the config is invalid
        """
        return cls(project_dir=project_dir, config=config, verbose=verbose)

    def _get_task_run_logs(
        self, wf_run_id: WorkflowRunId, task_run_id: TaskRunId
    ) -> List[str]:
        """Returns the logs for a specific task run.

        Raises:
            orquestra.sdk.exceptions.UnauthorizedError if QE returns 401
        """
        with _http_error_handling():
            logs_response = self._client.get_log(wf_id=wf_run_id, step_name=task_run_id)
        log_lines = logs_response["logs"].splitlines()
        try:
            jsonl_log_lines = [
                log_entry["log"] for log_entry in map(json.loads, log_lines)
            ]
            return jsonl_log_lines
        except json.JSONDecodeError:
            return log_lines

    def _get_workflow_run_logs(
        self, wf_run_id: WorkflowRunId, task_runs: Sequence[TaskRun]
    ) -> Dict[TaskInvocationId, List[str]]:
        """Returns the logs for each task run in a workflow run.

        Raises:
            orquestra.sdk.exceptions.UnauthorizedError if QE returns 401
        """
        # TODO: remove this method, it's not used anymore.
        return {
            task_run.invocation_id: self._get_task_run_logs(wf_run_id, task_run.id)
            for task_run in task_runs
        }

    def _check_workflow_name(self, workflow_name: str):
        """Ensure the the workflow name is compliant with QE.

        We're using a modified version of QE's regex that allows underscores. This
        relies on the fact that the conversion to YAML inside the SDK will replace
        the underscores.

        Args:
            workflow_name: The name to be sanitized

        Raises:
            InvalidWorkflowDefinitionError when the name is not QE compliant.
        """
        if not re.fullmatch(
            r"[a-z0-9]([-_a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-_a-z0-9]*[a-z0-9])?)*",
            workflow_name,
        ):
            raise exceptions.InvalidWorkflowDefinitionError(
                f'Workflow name "{workflow_name}" is invalid. '
                "Workflows names submitted to QE must conform to the following rules:\n"
                "- consist only of lowercase alphanumeric characters, '-', '_', "
                "or '.'\n"
                "- start and end with an alphanumeric character\n"
                "Potential workflow names can be checked here: "
                "https://regex101.com/r/CxEZKW/1"
            )

    def create_workflow_run(self, workflow_def: WorkflowDef) -> WorkflowRunId:
        """
        Submits a workflow to the Quantum Engine

        Args:
            workflow_def: The workflow definition to submit

        Returns:
            A workflow run id, created by QE

        Raises:
            orquestra.sdk.exceptions.UnauthorizedError if QE returns 401
            orquestra.sdk.exceptions.InvalidWorkflowDefinitionError if the
                workflow definition name does not conform to QE requirements.
        """
        if self._verbose:
            print_yaml_of_workflow(workflow_def)

        self._check_workflow_name(workflow_def.name)

        try:
            qe_workflow = workflow_to_yaml(workflow_def)
        except RuntimeError as e:
            raise exceptions.InvalidWorkflowDefinitionError(e.args[0]) from e

        # Band-aid for QE returning 400 instead of 413 for some large workflows.
        # This workaround lives here instead of _http_error_handling to avoid false
        # positives for other 400 errors
        try:
            with _http_error_handling():
                workflow_run_id = self._client.submit_workflow(
                    workflow=qe_workflow.json(exclude_none=True)
                )
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == requests.codes.bad_request:
                raise exceptions.WorkflowSyntaxError(e.response.text) from e
            else:
                raise e

        wf_run = StoredWorkflowRun(
            workflow_run_id=workflow_run_id,
            config_name=self._config.config_name,
            workflow_def=workflow_def,
        )
        with WorkflowDB.open_project_db(self._project_dir) as db:
            db.save_workflow_run(wf_run)

        return workflow_run_id

    def get_all_workflow_runs_status(self) -> List[WorkflowRun]:
        """
        Returns the workflow runs from QE that are inside the current project directory

        Raises:
            orquestra.sdk.exceptions.UnauthorizedError if QE returns 401
        """
        with WorkflowDB.open_project_db(self._project_dir) as db:
            wf_runs = db.get_workflow_runs_list(config_name=self._config.config_name)
        wf_run_dict = {
            wf_run.workflow_run_id: wf_run.workflow_def for wf_run in wf_runs
        }
        # To avoid making N requests to get all known workflows, we ask QE to give us
        # the list of workflows it knows about and return the ones in the local DB
        # TODO: make sure we get all workflows

        with _http_error_handling():
            json_response = self._client.get_workflow_list()
        workflow_runs = []
        for qe_workflow_run in json_response:
            workflow_run_id = qe_workflow_run["id"]
            if workflow_run_id in wf_run_dict:
                json_representation = json.loads(
                    qe_workflow_run["currentRepresentation"]
                )
                workflow_run = _parse_workflow_run_representation(
                    json_representation,
                    workflow_run_id,
                    wf_run_dict[workflow_run_id],
                    qe_workflow_run["status"],
                )
                workflow_runs.append(workflow_run)
        return workflow_runs

    def get_workflow_run_status(self, workflow_run_id: WorkflowRunId) -> WorkflowRun:
        """
        Returns the status of a given workflow run

        Args:
            workflow_run_id: the ID of the workflow run

        Raises:
            orquestra.sdk.exceptions.UnauthorizedError if QE returns 401
            orquestra.sdk.exceptions.InvalidProjectError: if an Orquestra
                project directory is not found
            orquestra.sdk.exceptions.WorkflowNotFoundError: if workflow run couldn't be
                found in the local database.
        """
        try:
            with WorkflowDB.open_project_db(self._project_dir) as db:
                try:
                    wf_run = db.get_workflow_run(workflow_run_id)
                except exceptions.WorkflowNotFoundError:
                    # explicit re-raise
                    raise

        except sqlite3.OperationalError as e:
            raise exceptions.InvalidProjectError(
                "Not an Orquestra project directory. Navigate to the repo root."
            ) from e

        wf_def = wf_run.workflow_def
        with _http_error_handling():
            json_response = self._client.get_workflow(wf_id=workflow_run_id)

        # Load the Argo representation from the response
        # TODO/FIXME: Is this a stable interface? Should it be exposed?
        representation = base64.b64decode(json_response["currentRepresentation"])
        json_representation = json.loads(representation)
        return _parse_workflow_run_representation(
            json_representation, workflow_run_id, wf_def, json_response["status"]
        )

    def get_workflow_run_outputs(self, workflow_run_id: WorkflowRunId) -> Sequence[Any]:
        raise NotImplementedError(
            "Blocking output is not implemented for Quantum Engine"
        )

    def get_workflow_run_outputs_non_blocking(
        self, workflow_run_id: WorkflowRunId
    ) -> Sequence[Any]:
        """Returns all output artifacts of a workflow run
        Args:
            workflow_run_id: the ID of the workflow run

        Raises:
            InvalidProjectError: if an Orquestra project directory is not found
            NotFoundError: if the workflow run cannot be found or is unrelated to this
                project
            orquestra.sdk.exceptions.WorkflowRunNotSucceeded: if the
                workflow output artifacts are not available yet
            orquestra.sdk.exceptions.UnauthorizedError if QE returns 401

        Returns:
            A list of objects given by the "output_ids" inside the workflow definition
        """
        try:
            with WorkflowDB.open_project_db(self._project_dir) as db:
                wf_run = db.get_workflow_run(workflow_run_id)
        except sqlite3.OperationalError as e:
            raise exceptions.InvalidProjectError(
                "Not an Orquestra project directory. Navigate to the repo root."
            ) from e
        wf_def = wf_run.workflow_def

        # TODO: instead of querying wf status, can't we just send the
        # get_workflow_result request and rely on http error codes?
        workflow_status = self.get_workflow_run_status(workflow_run_id)
        if workflow_status.status.state != State.SUCCEEDED:
            raise exceptions.WorkflowRunNotSucceeded(
                f"Workflow Run {workflow_run_id} is {workflow_status.status.state}",
                workflow_status.status.state,
            )

        with _http_error_handling():
            result_bytes = self._client.get_workflow_result(workflow_run_id)
        result_dict = _parse_workflow_result(result_bytes)
        task_invocations = _get_task_invocations(wf_def)
        artifacts = _get_artifacts(task_invocations, result_dict)

        # 1. Find the output IDs from the workflow def
        # 2, Find the artifact from the artifact dict
        # 3. Deserialise the artifact
        # 4. Append the artifact to the outputs list
        # 5. ???
        # 6. Profit
        output_ids = wf_def.output_ids

        return (
            *(
                serde.value_from_result_dict(artifacts[output_id])
                for output_id in output_ids
            ),
        )

    def get_available_outputs(self, workflow_run_id: WorkflowRunId):
        """Returns all available output artifacts of a workflow
        run even if workflow failed

         Args:
             workflow_run_id: the ID of the workflow run

         Raises:
             NotFoundError: if the workflow run cannot be found or is unrelated to this
                 project
             orquestra.sdk.exceptions.WorkflowRunNotFinished: if the
                 workflow hasn't finished yet and artifacts are not available yet
             orquestra.sdk.exceptions.UnauthorizedError if QE returns 401

         Returns:
             a dictionary of:
                 task invocation IDs: value returned from each invocation
        """
        with WorkflowDB.open_project_db(self._project_dir) as db:
            wf_run = db.get_workflow_run(workflow_run_id)
        wf_def = wf_run.workflow_def
        # Return dict contains return values for task invocation
        return_dict = {}
        with _http_error_handling():
            # retrieve each artifact for each step
            for inv in wf_def.task_invocations.values():
                output_ids = wf_def.task_invocations[inv.id].output_ids
                invocation_output_values = []
                try:
                    for artifact_id in output_ids:
                        artifact_bytes = self._client.get_artifact(
                            workflow_id=workflow_run_id,
                            step_name=inv.id,
                            artifact_name=artifact_id,
                        )
                        artifact_value = serde.value_from_result_dict(
                            _parse_workflow_result(artifact_bytes)
                        )
                        invocation_output_values.append(artifact_value)
                except requests.exceptions.HTTPError as e:
                    # 404 error happens when task is not finished yet.
                    # 500 error is thrown by QE in case of failed task.
                    # A task invocation can have multiple outputs. If any of the above
                    # errors happens for any of this invocation's outputs we consider
                    # the whole invocation unavailable.
                    if e.response.status_code == 404 or e.response.status_code == 500:
                        continue
                    else:
                        raise e

                # Assumption: this line won't be executed if the invocation isn't ready.
                return_dict[inv.id] = tuple(invocation_output_values)

        return return_dict

    def get_task_logs(
        self, wf_run_id: WorkflowRunId, task_inv_id: TaskInvocationId
    ) -> List[str]:
        """
        Raise:
            orquestra.sdk.exceptions.WorkflowNotFoundError: when couldn't find workflow
                with ``wf_run_id``.
            orquestra.sdk.exceptions.TaskInvocationNotFoundError: when ``task_inv_id``
                doesn't fit any tasks in this workflow.
            orquestra.sdk.exceptions.NotFoundError: when couldn't fetch logs from QE for
                this task.
        """
        try:
            workflow_run = self.get_workflow_run_status(wf_run_id)
        except exceptions.WorkflowNotFoundError as e:
            # explicit re-raise
            raise e

        try:
            # QE's API requires our TaskRunID. We know WorkflowRunID + TaskInvID so we
            # need to map it via the workflow status model.
            task_run = _find_first(
                lambda task_run: task_run.invocation_id == task_inv_id,
                workflow_run.task_runs,
            )
        except StopIteration as e:
            raise exceptions.TaskInvocationNotFoundError(
                invocation_id=task_inv_id
            ) from e

        try:
            log_lines = self._get_task_run_logs(
                wf_run_id=wf_run_id,
                task_run_id=task_run.id,
            )
        except requests.exceptions.HTTPError as e:
            raise exceptions.NotFoundError(
                f"Can't get logs for workflow run {parse_run_id}"
            ) from e

        return log_lines

    def get_workflow_logs(
        self, wf_run_id: WorkflowRunId
    ) -> Dict[TaskInvocationId, List[str]]:
        try:
            workflow_run = self.get_workflow_run_status(wf_run_id)
        except exceptions.NotFoundError as e:
            # explicit re-raise
            raise e

        # NOTE: we're making N requests here.
        return {
            task_run.invocation_id: self._get_task_run_logs(
                wf_run_id=wf_run_id,
                task_run_id=task_run.id,
            )
            for task_run in workflow_run.task_runs
        }

    def stop_workflow_run(self, run_id: WorkflowRunId) -> None:
        """Terminates a workflow run.

        Args:
            run_id: workflow run ID.

        Raises:
            orquestra.sdk.exceptions.UnauthorizedError if QE returns 401
            orquestra.sdk.exceptions.WorkflowRunCanNotBeTerminated if the
                termination failed
        """
        try:
            with _http_error_handling():
                self._client.stop_workflow(wf_id=run_id)
        except (
            requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
        ) as e:
            raise exceptions.WorkflowRunCanNotBeTerminated(
                f"{run_id} cannot be terminated."
            ) from e

    def list_workflow_runs(
        self,
        *,
        limit: Optional[int] = None,
        max_age: Optional[timedelta] = None,
        state: Optional[Union[State, List[State]]] = None,
    ) -> List[WorkflowRun]:
        """
        List the workflow runs, with some filters

        Args:
            limit: Restrict the number of runs to return, prioritising the most recent.
            max_age: Only return runs younger than the specified maximum age.
            status: Only return runs of runs with the specified status.

        Raises:
            orquestra.sdk.exceptions.UnauthorizedError: if QE returns 401

        Returns:
            A list of the workflow runs
        """
        now = datetime.now(timezone.utc)

        # Grab the workflows we know about from the DB
        with WorkflowDB.open_project_db(self._project_dir) as db:
            stored_runs = db.get_workflow_runs_list(
                config_name=self._config.config_name
            )

        # Short circuit if we don't have any workflow runs
        if len(stored_runs) == 0:
            return []

        if state is not None:
            if not isinstance(state, list):
                state_list = [state]
            else:
                state_list = state
        else:
            state_list = None

        wf_runs = []
        for wf_run_id in (r.workflow_run_id for r in stored_runs):
            try:
                wf_run = self.get_workflow_run_status(wf_run_id)
            except exceptions.WorkflowRunNotFoundError:
                continue

            # Let's filter the workflows at this point, instead of iterating over a list
            # multiple times
            if state_list is not None and wf_run.status.state not in state_list:
                continue
            if max_age is not None and (
                now - (wf_run.status.start_time or now) >= max_age
            ):
                continue
            wf_runs.append(wf_run)

        # We have to wait until we have all the workflow runs before sorting
        if limit is not None:
            wf_runs = sorted(wf_runs, key=lambda run: run.status.start_time or now)[
                -limit:
            ]
        return wf_runs
