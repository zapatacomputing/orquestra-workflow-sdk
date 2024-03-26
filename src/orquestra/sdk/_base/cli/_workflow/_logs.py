################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""Code for 'orq workflow logs'."""
import typing as t
import warnings
from pathlib import Path

from orquestra.sdk._base._logs._interfaces import WorkflowLogs
from orquestra.sdk.schema.configs import ConfigName
from orquestra.sdk.schema.workflow_run import WorkflowRunId

from .. import _arg_resolvers, _dumpers, _repos
from .._ui import _presenters


class Action:
    """Encapsulates app-related logic for handling ``orq workflow logs``."""

    def __init__(
        self,
        logs_presenter=_presenters.LogsPresenter(),
        error_presenter=_presenters.WrappedCorqOutputPresenter(),
        dumper=_dumpers.LogsDumper(),
        wf_run_repo=_repos.WorkflowRunRepo(),
        config_resolver: t.Optional[_arg_resolvers.WFConfigResolver] = None,
        wf_run_resolver: t.Optional[_arg_resolvers.WFRunResolver] = None,
    ):
        # data sources
        self._wf_run_repo = wf_run_repo

        # arg resolvers
        self._config_resolver = config_resolver or _arg_resolvers.WFConfigResolver(
            wf_run_repo=wf_run_repo
        )
        self._wf_run_resolver = wf_run_resolver or _arg_resolvers.WFRunResolver(
            wf_run_repo=wf_run_repo
        )

        # output
        self._logs_presenter = logs_presenter
        self._error_presenter = error_presenter
        self._dumper = dumper

    def on_cmd_call(
        self,
        wf_run_id: t.Optional[WorkflowRunId],
        config: t.Optional[ConfigName],
        download_dir: t.Optional[Path],
        task: t.Optional[bool],
        system: t.Optional[bool],
        env_setup: t.Optional[bool],
        other: t.Optional[bool],
    ):
        try:
            self._on_cmd_call_with_exceptions(
                wf_run_id=wf_run_id,
                download_dir=download_dir,
                config=config,
                task=task,
                system=system,
                env_setup=env_setup,
                other=other,
            )
        except Exception as e:
            self._error_presenter.show_error(e)

    def _on_cmd_call_with_exceptions(
        self,
        wf_run_id: t.Optional[WorkflowRunId],
        config: t.Optional[ConfigName],
        download_dir: t.Optional[Path],
        task: t.Optional[bool],
        system: t.Optional[bool],
        env_setup: t.Optional[bool],
        other: t.Optional[bool],
    ):
        # The order of resolving config and run ID is important. It dictates the flow
        # user sees, and possible choices in the prompts.
        resolved_config = self._config_resolver.resolve(wf_run_id, config)
        resolved_wf_run_id = self._wf_run_resolver.resolve_id(
            wf_run_id, resolved_config
        )

        # Get the available logs
        logs: WorkflowLogs = self._wf_run_repo.get_wf_logs(
            wf_run_id=resolved_wf_run_id, config_name=resolved_config
        )

        # Resolve the log type switches. This must happen after getting the logs as we
        # need to check against which logs are available.
        switches: t.Mapping[
            WorkflowLogs.WorkflowLogTypeName, bool
        ] = self._wf_run_resolver.resolve_log_switches(
            task, system, env_setup, other, logs
        )

        for log_type in switches:
            if not switches[log_type]:
                continue

            log = logs.get_log_type(log_type)

            if len(log) < 1:
                warnings.warn(f"No {log_type} logs found.", category=UserWarning)
                continue
            if download_dir:
                dump_paths = self._dumper.dump(
                    logs=log,
                    wf_run_id=resolved_wf_run_id,
                    dir_path=download_dir,
                    log_type=log_type,
                )
                for dump_path in dump_paths:
                    self._logs_presenter.show_dumped_wf_logs(dump_path, log_type=log_type)
            else:
                self._logs_presenter.show_logs(log, log_type=log_type)
