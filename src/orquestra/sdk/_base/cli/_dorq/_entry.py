################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
"dorq" CLI entrypoint.

``click`` uses function name as the group and command name.
"""
import typing as t
from pathlib import Path

import click
import cloup

# Adds '-h' alias for '--help'
CLICK_CTX_SETTINGS = {"help_option_names": ["-h", "--help"]}


@cloup.group(context_settings=CLICK_CTX_SETTINGS)
def dorq():
    # Normally, click would infer command name from function name. This is different,
    # because it's the top-level group. User-facing name depends on the entrypoint spec
    # in setup.cfg.
    pass


# ----------- 'orq workflow' commands ----------


@dorq.group(aliases=["wf"])
def workflow():
    """
    Commands related to workflow runs.
    """
    pass


@workflow.command()
@cloup.argument(
    "module",
    required=True,
    help="""
Location of the module where the workflow is defined. Can be a dotted
name like 'package.module' or a filepath like 'my_proj/workflows.py'. If
it's a filepath, the project layout is assumed to be "flat-layout" or
"src-layout" as defined by Setuptools:
https://setuptools.pypa.io/en/latest/userguide/package_discovery.html#automatic-discovery.
    """,
)
@cloup.argument(
    "name",
    required=False,
    help="""
Name of the workflow function to load from 'module'. If omitted, 'orq' will ask for
selecting a function from the ones available in 'module'.
""",
)
@cloup.option("-c", "--config")
@cloup.option(
    "--force",
    is_flag=True,
    default=False,
    help=(
        "If passed, submits the workflow without confirmation even if there are "
        "uncommitted changes."
    ),
)
def submit(module: str, name: t.Optional[str], config: t.Optional[str], force: bool):
    """
    Submits a workflow for execution.

    Loads workflow definition from a Python module,
    creates a new workflow run, and returns the workflow run ID for use in other
    commands.

    If there's a task defined in a git repo with uncommitted changes you are asked for
    confirmation before submitting the workflow.
    """
    # TODO: add help for config

    from ._workflow._submit import Action

    action = Action()
    action.on_cmd_call(module, name, config, force)


@workflow.command()
@cloup.argument("wf_run_id", required=False)
@cloup.option("-c", "--config")
def view(wf_run_id: t.Optional[str], config: t.Optional[str]):
    """
    Prints details of a single workflow run that was already submitted.
    """

    from ._workflow._view import Action

    action = Action()
    action.on_cmd_call(wf_run_id, config)


DOWNLOAD_DIR_OPTION = cloup.option(
    "--download-dir",
    help=(
        "Directory path to store the artifact value. If passed, the command will "
        "create a file under this location."
    ),
    type=click.Path(file_okay=False, dir_okay=True, writable=True, path_type=Path),
)


@workflow.command()
@cloup.argument("wf_run_id", required=False)
@cloup.option("-c", "--config")
@DOWNLOAD_DIR_OPTION
def results(
    wf_run_id: t.Optional[str],
    config: t.Optional[str],
    download_dir: t.Optional[Path],
):
    """
    Shows preview of a workflow output values corresponding to the variables
    returned from the ``@workflow`` function.

    Only works with succeeded workflows. If a workflow is still running this command
    won't wait for the workflow's completion.

    This command tries to print a human-friendly values preview, but the output isn't
    guaranteed to be a valid parseable value. If you need the artifact value for
    further processing, use the ``download_dir`` option or use
    ``orquestra.sdk.WorkflowRun.get_results()`` directly from Python.
    """

    from ._workflow._results import Action

    action = Action()
    action.on_cmd_call(
        wf_run_id=wf_run_id,
        config=config,
        download_dir=download_dir,
    )


@workflow.command()
@cloup.argument("wf_run_id", required=False)
@cloup.option("-c", "--config")
def stop(wf_run_id: t.Optional[str], config: t.Optional[str]):
    """
    Stops a running workflow.
    """

    from ._workflow._stop import Action

    action = Action()
    action.on_cmd_call(wf_run_id, config)


@workflow.command()
@cloup.option("-c", "--config", type=str, multiple=True)
@cloup.option(
    "-a",
    "--all",
    is_flag=True,
    flag_value=True,
    help="Show all workflow runs that match the specified filters.",
)
@cloup.option(
    "-i",
    "--interactive",
    is_flag=True,
    flag_value=True,
    help="Specify filters in an interactive terminal session.",
)
@cloup.option(
    "-l", "--limit", type=int, help="Maximum number of runs to display for each config."
)
@cloup.option("-t", "--max-age", help="Maximum age of runs to display.")
@cloup.option(
    "-s",
    "--state",
    multiple=True,
    help="State of workflow runs to display. Max be specified multiple times.",
)
@cloup.constraint(cloup.constraints.mutually_exclusive, ["all", "interactive"])
def list(
    config: t.Optional[str],
    all: t.Optional[bool] = False,
    interactive: t.Optional[bool] = False,
    limit: t.Optional[int] = None,
    max_age: t.Optional[str] = None,
    state: t.Optional[t.List[str]] = None,
):
    """
    Lists the available workflows
    """

    from ._workflow._list import Action

    action = Action()
    action.on_cmd_call(config, limit, max_age, state, all, interactive)


@cloup.command()
@cloup.option_group(
    "Services",
    cloup.option("--ray", is_flag=True, default=None, help="Start a Ray cluster"),
    cloup.option(
        "--fluentbit", is_flag=True, default=None, help="Start a Fluentbit container"
    ),
    cloup.option("--all", is_flag=True, default=None, help="Start all known services"),
)
def up(ray: t.Optional[bool], fluentbit: t.Optional[bool], all: t.Optional[bool]):
    """
    Starts managed services required to execute workflows locally.

    By default, this command only starts a Ray cluster.
    """
    from ._services._up import Action

    action = Action()
    action.on_cmd_call(ray, fluentbit, all)


@cloup.command()
@cloup.option_group(
    "Services",
    cloup.option("--ray", is_flag=True, default=None, help="Start a Ray cluster"),
    cloup.option(
        "--fluentbit", is_flag=True, default=None, help="Start a Fluentbit container"
    ),
    cloup.option("--all", is_flag=True, default=None, help="Start all known services"),
)
def down(ray: t.Optional[bool], fluentbit: t.Optional[bool], all: t.Optional[bool]):
    """
    Stops managed services required to execute workflows locally.

    By default, this command only stops the Ray cluster.
    """
    from ._services._down import Action

    action = Action()
    action.on_cmd_call(ray, fluentbit, all)


@cloup.command()
def status():
    """
    Prints the status of known services.

    Currently, this will print the status of the managed Ray cluster and managed Fluent
    Bit container.
    """
    from ._services._status import Action

    action = Action()
    action.on_cmd_call()


dorq.section(
    "Service Management",
    up,
    down,
    status,
)


@dorq.command()
@cloup.option(
    "-s", "--server", required=True, help="server URI that you want to log into"
)
@cloup.option(
    "-t",
    "--token",
    required=False,
    help="User Token to given server. To generate token, use this command without -t"
    "option first",
)
@cloup.option("--ce", is_flag=True, default=False, help="Start a Ray cluster")
def login(server: str, token: t.Optional[str], ce: bool):
    """
    Login in to remote cluster
    """
    from ._login._login import Action

    action = Action()
    action.on_cmd_call(server, token, ce)


def main():
    dorq()


if __name__ == "__main__":
    main()
