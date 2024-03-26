################################################################################
# © Copyright 2023-2024 Zapata Computing Inc.
################################################################################

"""Code that stores values on disk as a result of a CLI command."""
import typing as t
from functools import singledispatchmethod
from pathlib import Path

from orquestra.sdk._base import serde
from orquestra.sdk._base._logs._interfaces import LogOutput, WorkflowLogs
from orquestra.sdk.schema.workflow_run import TaskInvocationId, WorkflowRunId


class WFOutputDumper:
    """Writes a workflow run's output artifacts to a file."""

    def dump(
        self,
        value: t.Any,
        wf_run_id: WorkflowRunId,
        output_index: int,
        dir_path: Path,
    ) -> serde.DumpDetails:
        """Serialize a single artifact value and save it as a new file.

        Missing directories will be created.
        Filenames will be generate based on ``wf_run_id`` and ``output_index``.
        The serialization format and file extensions are inferred based on the object.

        No standard errors are expected to be raised.
        """
        dump_dir = dir_path / wf_run_id / "wf_results"

        dump_details = serde.dump_to_file(
            value=value, dir_path=dump_dir, file_name_prefix=str(output_index)
        )

        return dump_details


class TaskOutputDumper:
    """Writes task run's output artifact to a file."""

    def dump(
        self,
        value: t.Any,
        wf_run_id: WorkflowRunId,
        task_inv_id: TaskInvocationId,
        output_index: int,
        dir_path: Path,
    ) -> serde.DumpDetails:
        """Serialize artifact value and save it as a new file.

        Creates missing directories. Generates filenames based on ``wf_run_id`` and
        ``output_index``.
        Figures out the serialization format based on the object.
        The generated file extension matches the inferred format.

        No standard errors are expected to be raised.
        """
        dump_dir = dir_path / wf_run_id / "task_results" / task_inv_id

        dump_details = serde.dump_to_file(
            value=value,
            dir_path=dump_dir,
            file_name_prefix=str(output_index),
        )

        return dump_details


class LogsDumper:
    """Writes logs to files."""

    @staticmethod
    def _get_logs_file(
        dir_path: Path,
        wf_run_id: WorkflowRunId,
        log_type: t.Optional[WorkflowLogs.WorkflowLogTypeName] = None,
        is_stderr: bool = False,
    ) -> Path:
        dir_path.mkdir(parents=True, exist_ok=True)
        extension = "err" if is_stderr else "log"
        if log_type:
            return (
                dir_path
                / f"{wf_run_id}_{log_type.value.lower().replace(' ', '_')}.{extension}"
            )
        return dir_path / f"{wf_run_id}.{extension}"

    def dump(
        self,
        logs: t.Union[
            t.Mapping[TaskInvocationId, t.Sequence[str]],
            t.Sequence[str],
            LogOutput,
            t.Mapping[TaskInvocationId, LogOutput],
        ],
        wf_run_id: WorkflowRunId,
        dir_path: Path,
        log_type: t.Optional[WorkflowLogs.WorkflowLogTypeName] = None,
    ):
        """Save logs from wf into a file.

        Creates missing directories.
        Generates filenames based on ``wf_run_id``.
        No standard errors are expected to be raised.
        """
        logs_file = self._get_logs_file(dir_path, wf_run_id, log_type=log_type)
        err_logs_file = self._get_logs_file(
            dir_path, wf_run_id, log_type=log_type, is_stderr=True
        )

        out_lines, err_lines = self._construct_output_log_lines(logs)

        with logs_file.open("w") as f:
            f.writelines("\n".join(out_lines))

        with err_logs_file.open("w") as f:
            f.writelines("\n".join(err_lines))

        return logs_file, err_logs_file

    @singledispatchmethod
    @staticmethod
    def _construct_output_log_lines(*args) -> t.Tuple[t.List[str], t.List[str]]:
        """Construct a list of log lines to be printed.

        This method has overloads for dict and list arguments.
        """
        raise NotImplementedError(
            f"No log lines constructor for args {args}"
        )  # pragma: no cover

    @_construct_output_log_lines.register(LogOutput)
    @staticmethod
    def _(logs: LogOutput) -> t.Tuple[t.List[str], t.List[str]]:
        return logs.out, logs.err

    @_construct_output_log_lines.register(dict)
    @staticmethod
    def _(
        logs: t.Union[
            t.Mapping[TaskInvocationId, t.Sequence[str]],
            t.Mapping[TaskInvocationId, LogOutput],
        ]
    ) -> t.Tuple[t.List[str], t.List[str]]:
        outlines = []
        errlines = []
        for task_invocation_id, log_values in logs.items():
            outlines.append(f"stdout logs for task invocation: {task_invocation_id}:\n")
            errlines.append(f"stderr logs for task invocation: {task_invocation_id}:\n")
            if isinstance(log_values, LogOutput):
                outlines.extend(log_values.out)
                errlines.extend(log_values.err)
            else:
                outlines.extend(log_values)
            outlines.append("\n")
            errlines.append("\n")
        return outlines, errlines

    @_construct_output_log_lines.register(list)
    @staticmethod
    def _(logs: list) -> t.Tuple[t.List[str], t.List[str]]:
        return logs, []
