# Changelog

## Unreleased

🚨 *Breaking Changes*

* Workflow status will be set to `FAILED` as soon as first task fails. Tasks that already started will finish their execution

🔥 *Features*

* Add new API `current_wf_ids` which returns current `workspace_id` `project_id` and `config_name`. Can be used inside tasks

🧟 *Deprecations*

👩‍🔬 *Experimental*

🐛 *Bug Fixes*

💅 *Improvements*

* Tracebacks in `orq` are made more compact to help with copy and pasting when an issue happens.
* Added support for Pydantic V2 in addition to the previously supported `>=1.10.8`.
* Removed bunch of upper-bound constrains from SDK requirements to prevent dependency-hell

🥷 *Internal*

📃 *Docs*

## v0.61.0

🔥 *Features*

* Added `max_retries` in `sdk.task` decorator. This allows users to restart ray workers on system crashes (like OOMKills or sigterms). Restarts do not happen with Python exceptions.

🐛 *Bug Fixes*

* Requesting GPUs with the default image will now use a GPU image on CE.

🥷 *Internal*

* Switch out packaging to `hatchling` instead of `setuptools`. This should not impact people installing from wheels.

## v0.60.0

🔥 *Features*

* New API `WorkflowRun.get_artifact()` and `WorkflowRun.get_artifact_serialized()` to get single task output
* New API: `orquestra.sdk.dremio.DremioClient` for reading dataframes from Orquestra-hosted Dremio.

🐛 *Bug Fixes*

* Properly handle QE deprecated and connection timeout errors when using `WorkflowRun.by_id()` without `config` parameter passed

💅 *Improvements*

* Bumped Ray to 2.9.0.

## v0.59.0

🔥 *Features*

* Listing workflow runs on CE now supports filtering by state and max age.
* `head_node_resources` keyword argument has been added to the `@sdk.workflow` decorator to control the resources that the head node has available on CE.

🧟 *Deprecations*

* Deprecation of `project_dir` argument for all public API functions.
* `data_aggregation` keyword argument to `@sdk.workflow` decorator is deprecated and will be removed in a future version.

🐛 *Bug Fixes*

* Workaround for Ray cluster not starting because of a missing dependency, `async_timeout`.
* Fix warnings when used with `aiohttp>3.9`.

💅 *Improvements*

* When the user doesn't pass `config` directly to `sdk.WorkflowRun.by_id(run_id="...")` and `orq` commands, the SDK will query all known runtimes about this workflow run. This change improves accessing workflow runs submitted by other users.
* Secrets are only fetched once for imports in a workflow, instead of for each task invocation.

🥷 *Internal*
* Remove unused `_db` code

* Change `RuntimeConfig._name` typing from `Optional[str]` to `str`

📃 *Docs*

* Update _Runtime Configuration_ guide and config-related docstrings to align them with the ``auto`` config.

## v0.58.0

🚨 *Breaking Changes*

* The `orquestra-sdk` version used to submit a workflow is automatically added as a dependency for task execution environments. Specifying the SDK as a dependency in the `sdk.task()` decorator will be ignored.
* The base image for workflows is now Python 3.11.6. Submitting a remote workflow with this version of the SDK with a different version of Python may result in failed workflows.

🧟 *Deprecations*

* Un-deprecated not passing `workspace_id` when accessing secrets.

## v0.57.0

🚨 *Breaking Changes*

* Removed model classes that used to define the shape of defunct `orq` CLI command outputs.
* CE: When a task fails, the workflow will continue to be `RUNNING` while all other started tasks complete.

🔥 *Features*

* Added `sdk.list_workflow_run_summeries()` as a partner to `sdk.list_workflow_runs()` - the new function provides a static overview of workflow runs as a way to quickly check on their statuses.
* `orq wf list` now displays the `owner` for each CE workflow run.
* `WorkflowRun` objects in the API now have a "message" field, similar to that of `TaskRun`, to provide additional context to the status. If populated, this field is displayed alongside other workflow run info.

🐛 *Bug Fixes*

* `orq login --list` properly handles missing or empty config file
* Attempting to get the status of a workflow run that failed during environment setup will now report the workflow run as 'Failed' rather than raising a ValueError.

💅 *Improvements*

* Throw more informative exceptions if secret is used in any unintended way inside workflow function
* `orq wf list` now uses `sdk.list_workflow_run_summeries()` under the hood. On CE, this reduces the required API calls from 3N+1 to 1.
* Workflow runs failing due to errors during Ray environment setup will now display helpful error messages, including instructions on accessing the logs.

🥷 *Internal*

* Removed unused local cache code.
* The Ray metadata for failed tasks now includes a `failed` field in addition to the start and end times.

📃 *Docs*

* Fixed formatting errors in docstrings preventing them from rendering correctly in docs.
* Updated the "Dependency Installation" guide to reflect the requirement of matching `orquestra-sdk` versions between worker and head nodes.

## v0.56.0

🚨 *Breaking Changes*

* When `auto` config is passed from local machine, config set in `ORQ_CURRENT_CONFIG` env variable will be used. Using `auto` locally without that env variable set, will result in an error.
* Removed `orquestra.sdk.v2` module. Please use `orquestra.sdk` instead.
* Bump Ray version to 2.6.3

🔥 *Features*

* Adding `dry_run` parameter to `Workflow.run()`. It allows to test resources, dependencies and infrastructure while ignoring user task code.
* Added `orq reset` as a shortcut for `orq down`, `orq up`
* New CLI output formatting for a subset of commands.

🐛 *Bug Fixes*

* Package-dependent exception thrown from the task no-longer causes the red-herring error of `no module named <xxx>` in the logs. It prints proper exception

🥷 *Internal*
* Reformatted docs source files to put each sentence on its own line.
* Removed `UnsavedConfigChangesError`

## v0.55.0

🚨 *Breaking Changes*

* Quantum Engine support has been removed.
* All log return types have been changes from `Sequence[str]` to `LogOutput` which contains an `out` and an `err` property for the standard out and standard error, respectively.
* Task logs on Compute Engine will now be available under the dictionary `task_logs` with the task invocation ID as the key. Task logs from workflows submitted with v0.54.0 and earlier that ran on Compute Engine will be available under "other" logs.

🔥 *Features*
* Task logs are now available from Compute Engine.

🧟 *Deprecations*
* `list_workflow_runs`' project parameter emits a warning and will be removed in the next release. This change doesn't affect the system's behavior, the parameter was ignored anyway.

💅 *Improvements*
* `orq wf *` and `orq task *` commands (other than `orq wf submit`) won't prompt for project parameter anymore, as it was ignored anyway
* On macOS and Linux, task logs are stored in individual files instead being correlated with markers from Ray logs.

📃 *Docs*
* Corrected unclear language in `Secrets` docs.
* Corrected unclear language in the quickstart guide.
* Fixed resource management doc incorrectly stating that 10k == 10^7.
* Removed outdated references to the `wf.prepare()` method.
* Fixed wording and formatting issues in Resource Management, Workflow Syntax, Runtime Configuration, and Workflow Runs, and Parametrized Workflows.
* Updated Jupyter tutorial's content.

## v0.54.0

🚨 *Breaking Changes*
* `orq wf list` command does not accept `-p, --project-id` parameter anymore
* `orq wf list` only prompts for a single config now
* Deprecated functions `sdk.wfprint()` and `sdk.workflow_logger()` have been removed.

🔥 *Features*
* `orq --version` or `orq -V` will now show the current SDK version.
* `orq wf list` properly prompts for a workspace after selecting config if option `-w` was not provided
* Helpful error when Compute Engine rejects a workflow if the Workflow SDK version is too old.

🐛 *Bug Fixes*
* Fix an error when calling `get_artifacts()` on unfinished or failed workflow
* `KILLED` workflows will no longer show an error when waiting for the workflow to complete.

💅 *Improvements*
* Consolidate all `NotATaskWarning` warnings into a single warning for each workflow.
* Remove redundant environment variable checks in MLFlow connection utils.

📃 *Docs*
* Added "Beginner's Guide to the CLI"
* Update migration docs to use `orquestra-sdk[all]` to ensure extras are updated.
* Added "Workspaces and Projects" guide.
* Corrected minor typos in the dependency installation guide.
* Removed "Using Custom Container Images on Compute Engine" guide.
* Extended the "MLflow" guide with sections about tracking URI and token.

## v0.53.0

🚨 *Breaking Changes*
* Removed unsupported `WorkflowDef.local_run()` function
* Bump Ray version to 2.5.1

🔥 *Features*
* `sdk.mlflow.get_tracking_uri()` and `sdk.mlflow.get_tracking_token()` are now provided to give access to the MLFlow tracking information.
* Add `sdk.mlflow.get_current_user()` function to improve MLflow UI labeling
* Verify if workflow resources are sufficient to run all tasks at submission time

🐛 *Bug Fixes*
* Install Python modules in a venv using a non-root user to fix errors in custom Docker images.
* Fix listing workflows when using Ray if `ORQ_CURRENT_*` environment variables are set.

💅 *Improvements*
* `sdk.current_run_ids()` now returns a `NamedTuple` called `CurrentRunIDs` to help with typing.
* Tasks that request resources that are incompatible with Ray will throw an error at submission time.

📃 *Docs*
* The help string for configs in the CLI now specifies the correct `in_process` rather than `in-process`.
* The description for `config_name` in the `sdk.secrets.set` tutorial has been updated to be clearer.
* The workflow syntax guide now uses more precise language when discussing workflow run returns.
* Updated "Dependency Installation" guide for current best practices.
* Added "Migrating From Quantum Engine" guide.

## v0.52.0

🚨 *Breaking Changes*
* Removed `RuntimeConfig.load_default()`
* Removed any support for default configuration
* `sdk.secret` functions will no longer use default configuration from local runtimes. Config has to be passed explicitly unless running on remote cluster
* `WorkflowDef.get_tasks()` now returns topologically sorted list of `TaskRun` objects instead of set
* `orq wf logs` now supports `--task`, `--system` and `--env-setup` flags to control which logs are shown / downloaded. If none of these flags are set, the user is prompted for a choice of log types, with the default being `all`.

🔥 *Features*
* Adding `FutureWarning` when accessing CE Secrets without specifying Workspace.
* Users can use `ORQ_CURRENT_PROJECT` and `ORQ_CURRENT_WORKSPACE` env variables to set default workspace and project for their interactions with CE.
* In any execution environment, users can use `sdk.mlflow.get_temp_artifacts_dir()` to get the path to a temporary directory for writing artifacts prior to uploading to MLFlow.
* Add `--list` option to `orq login` that displays the stored remote logins, which runtimes they are using, and whether their access tokens are up to date.
* Local runtime now captures any logs printed to standard output and error streams when a task is running. In particular, this means plain `print()`s will be captured and reported back with `orq wf logs` or `orq task logs`.

🧟 *Deprecations*
* Deprecated `sdk.wfprint()` and `sdk.workflow_logger()`.

👩‍🔬 *Experimental*

🐛 *Bug Fixes*
* When automated login fails, the instructions for manually logging in now show the correct runtime flag.

💅 *Improvements*
* When a new config is saved, the message shown in the CLI now includes the runtime name.
* API: rather then returning empty lists, ray local logs now return messages for `system` and `other` log categories that direct the user to the logs directory.
* User-emitted logs are no longer wrapped in an JSON dictionary with metadata. `print("foo")` will now result in a log line `"foo"` instead of `'{"message": "foo", "timestamp": ..., "wf_run_id": ..., ...}'`

🥷 *Internal*
* Refactored `datetime` and timezone handling.
* Orquestra runtime now emits marker logs at Orquestra task start/end.
* Redesign how we provide URIs for `CE_RUNTIME` to easily swap for internal URIs when on cluster

📃 *Docs*
* "Remote Workflows" updated to describe logging in with a specific runtime, and reflect the current login process (automatic opening of login page, copying of token).

## v0.51.0

🚨 *Breaking Changes*
* Compute Engine is now the default when logging in
* `InlineImport()` is now always default `source_import` for tasks

🔥 *Features*
* Force stop workflow runs via the CLI or Python API
* `WorkflowRun.get_tasks()` supports filtering tasks by state, function name, task run ID and task invocation ID.
* 2 new methods added to public API of `WorkflowRun`: `get_artifacts_serialized()` and `get_results_serialized()`

🐛 *Bug Fixes*
* Fix Ray WFs failing caused by any task returning dict defined in return statement

💅 *Improvements*
* When using `GithubImport`, better error messages are raised when a value is passed to `personal_access_token` that is not a `sdk.Secret()`.
* `wf_run.get_logs().env_setup` now contains task dependency installation logs when running on the local `ray` runtime.

🥷 *Internal*

📃 *Docs*
* Fix broken "Dependency Installation" docs.

## v0.50.0

🚨 *Breaking Changes*
* `WorkflowRun.get_logs()` now returns a data structure with logs split into categories.
* Local workflow database has changed format: downgrading from v0.50.0 will require user intervention.

🔥 *Features*
* Add .project property to WorkflowRun to get the info about workspace and project of running workflow
* `VersionMismatch` warnings are shown only when interacting with specific workflow runs, not while listing workflows.
* Add `--qe` flag to `orq login`, this is the default so there is no change in behavior.
* Bump Ray version to 2.4.0
* New API method `WorkflowRun.start_from_ir()` that allows to start workflow run having only IR object
* The WorkflowLogs object returned from `WorkflowRun.get_logs()` now includes Compute Engine system logs for workflow runs using CE.

🐛 *Bug Fixes*
* Secrets with workspaces now work inside workflow functions and for personal access tokens in `GithubImport`.
* `list_workspaces` and `list_projects` work inside studio with `auto` config
* Listing workflows on QE no longer fails if there was a CE workflow in the DB

💅 *Improvements*
* Add prompters to `orq wf submit` command for CE runtime if workspace and project weren't passed explicitly
* Auto-select and highlight current workspace and project when using `auto` config in CLI in studio
* `orquestra-sdk-base` CPU container image has a 20% size reduction.
* Added `State` enum to the base `orquestra.sdk` package for easier filtering task runs.
* Logs fetched from CE are now split into "task" and "env setup" categories.

📃 *Docs*
* Update resource management guide for Compute Engine
* Add section for using custom images on Compute Engine

## v0.49.1

🐛 *Bug Fixes*
* Fix CLI prompters to not throw exceptions after selecting project and workspace

## v0.49.0

🚨 *Breaking Changes*
* Removed `WorkflowDef.prepare()` and `WorkflowRun.start()` functions. Use `WorkflowDef.run()` instead

🔥 *Features*
* The API list_workflow_runs() function now accepts workspace and project arguments when used with CE configs.
* Login CLI command now accepts the name of a stored config as an alternative to the uri.
* Choice and Checklist CLI prompts present an informative error message when there are no options rather than prompting the user to select from an empty list.
* New API functions: list_workspaces() and list_projects(). Usable only on CE runtime.
* Setting workflow_id and project_id is now available using "orq wf submit" command.
* `sdk.current_run_ids()` can now be used within task code to access the workflow run ID, task invocation ID, and task run ID.
* All CLI commands that prompted for `wf_run_id` will now first prompt for workspace and project if `wf_run_id` is not provided.
* The error raised when trying to submit to Ray while Ray is not running now tells the user how to start Ray.
* `sdk.secrets.list()`, `sdk.secrets.get()`, `sdk.secrets.set()` and `sdk.secrets.delete()` now accept `workspace_id` parameter to specify secrets in particular workspace.
* `auto` config inside studio will infer workspace and project IDs from studio instance.
* Support for running tasks in Docker containers with custom images on Compute Engine.

🐛 *Bug Fixes*
* Fixed tasks that failed when explicitly state `n_outputs=1` on QE and in-process.

💅 *Improvements*
* `orq login` will perform some sanity checks before saving the token.
* If `orq up` fails, the output will now include the error message from the underlying subprocess.

🥷 *Internal*
* Fix random CI failures on socket warning

📃 *Docs*
* Update secrets guide to take workspaces into account

## v0.48.0

🚨 *Breaking Changes*
* Removed deprecated "name" parameter for `RuntimeConfig` factory methods, like `qe()` or `ray()`
* Removed deprecated `save()` method from `RuntimeConfig` class
* Removed `is_saved()` method and "name" setter from `RuntimeConfig` class
* `WorkflowRun.get_results()` returns values consistent with vanilla python. Single results are returned as-as, multiple results are returned as a tuple.

🔥 *Features*
* New API functions: `list_workspaces()` and `list_projects()`. Usable only on CE runtime.
* Setting `workflow_id` and `project_id` is now available using `orq wf submit` command.

🐛 *Bug Fixes*
* Tasks will no longer be retried on Ray and Compute Engine when the process crashes, preventing duplicated MLflow errors.

💅 *Improvements*
* In the CLI, where the user would be prompted with a choice, but only one option is available, we now prompt for confirmation instead.

🥷 *Internal*
* Switch the login URL endpoint
* Rewrite tests to avoid hangs on Windows CI

## 0.47.0

🚨 *Breaking Changes*
* Task results on QE have changed shape. This may cause some oddness when downloading older task artifacts.

🔥 *Features*
* New built-in config name - "auto" - used to submit workflows to a remote cluster when used inside Orquestra Studio.
* "auto" built-in config name becomes alias to "local" if not in a Studio environment

👩‍🔬 *Experimental*
* Setting `workflow_id` and `project_id` is now available on workflow Python API `start()` and `prepare()` functions

🐛 *Bug Fixes*
* Retry getting results from CE if the results were not ready but the workflow succeeded.
* Using secrets inside the workflow function will now work correctly on Ray
* Fix `WorkflowDef.graph` - honor kwargs of tasks and add `aggregate_output` to show outputs
* Fixed returning intermediate workflow values (e.g. with `orq task results`) when the task has multiple outputs and only some of them were used in the rest of the workflow function. The following should work now as expected:

```python
@sdk.workflow
def my_wf():
    _, b = two_output_task()
    all_outputs = two_output_task()
    out1, out2 = all_outputs
    return b, all_outputs, out1, out2
```

* Pickled workflow/task results should no longer cause workflows to fail inside the SDK machinery. Note: when passing a Python object between your tasks, you **must** ensure the Python dependencies are installed.

💅 *Improvements*
* `VersionMismatch` warning will be presented if we detect accessing a workflow def IR generated with another SDK version.

🥷 *Internal*
* `TaskOutputMetadata` model was added to the workflow def IR schema.
* Workflows from CE return a new shape for workflow results and task results.
* Workflows from Ray return a new shape for workflow and task results.
* Workflows from QE return a new shape for task results.
* Custom images will default to `None`, unless using Quantum Engine where the defaults stay the same.

📃 *Docs*
* _Resource Management_ guide should render properly now.

## v0.46.0

🚨 *Breaking Changes*
* Workflow definitions now require at least one task in order to be submitted. This check is performed during traversal, and raises a `WorkflowSyntaxError` if no tasks are required to be executed.
* Remove `TaskDef.model` and `TaskDef.import_models` interfaces
* Public API classes `sdk.GitImport`, `sdk.GithubImport`, `sdk.LocalImport`, `sdk.InlineImport` now use `dataclasses.dataclass` instead of `typing.NamedTuple`.
* Local Ray will now always pass resources to underlying ray.remote functions.

🔥 *Features*
* Sort WF runs by start date in `list wf` command. Show start date as one of the columns
* Sort WF runs by start date in all workflow commands in prompt selection. Show start date with WF id
* Set resources for workflows on CE via `resources` keyword argument in the `@workflow` decorator or with `.with_resources()` on a `WorkflowDef`.
* New parameters for `@workflow` decorator - `default_source_import` and `default_dependency_imports`.
These parameters let you set the default imports for all tasks in given workflow.
If a task defines its own imports (either source, dependencies, or both) - it will overwrite workflow defaults.
* Allow single imports as `dependency_imports` in `@task` decorators.
* Listing workflow runs from Compute Engine now allows an upper limit to the number of runs to be listed to be set via the `limit` keyword.
* Print HTTP requests and other debug information from `orq` CLI if `ORQ_VERBOSE` env flag is set.
* CE runtime now supports getting logs from remote Ray runtimes.

🐛 *Bug Fixes*
* Stopping a QE workflow after it has already stopped will no longer raise an exception.
* Fix dependency issues causing CE workflows to fail if WF constant was library-dependent object.
* Attempting to use the "in-process" runtime on the CLI will no longer raise an exception. Instead, a message telling you to use the Python API or Ray will be printed.

🥷 *Internal*
* During YAML conversion, Workflow SDK repo matched on host and path, not full URL.
* On QE, Github URLs will be converted to SSH URLs.
* Removed `corq` code.
* Old `RuntimeInterface` methods have been removed.

📃 *Docs*
* Guide: CE Resource Management

## v0.45.1

🐛 *Bug Fixes*
* Ensure `int`-like resource values are passed to Ray correctly

## v0.45.0

🚨 *Breaking Changes*
* Pickling library switched to `cloudpickle` instead of `dill`. While no breakages are expected, this change may result in objects raising an error during pickling, even if they were previously able to be pickled. Please report any instances of these as bugs.

🔥 *Features*
* Use the requested resources from a workflow's tasks when submitting to CE

🥷 *Internal*
* RayRuntime can now be configured to pass resources to underlying remote functions
* Added version metadata to the workflow IR

## v0.44.0

🚨 *Breaking Changes*
* Removed FluentBit-related CLI options: `orq {up,down} --fluentbit` flag. Logs produced by the local Ray runtime are read directly by the SDK now. This only affects users who used the experimental integration with FluentBit docker container.
* `GitImport` will no longer be downloaded automatically when using Ray locally. This reverts behavior to `v0.42.0`.
* Internal configuration environment variables have changed.

🔥 *Features*
* Secrets can now be used inside workflow functions
* `sdk.secrets.get("name")` will now use passport-based authorization if `ORQUESTRA_PASSPORT_FILE` environment variable is set. Otherwise, passing a valid `config_name="..."` is required.
* Bump Ray version to 2.3
* `GithubImport` can be used with a username and a secret referring to a "personal access token" to enable private GitHub repositories on Compute Engine. Server side support coming soon!

👩‍🔬 *Experimental*

🐛 *Bug Fixes*
* Getting full logs produced by Ray workflows. Previously, the dictionary returned by `logs_dict = wf_run.get_logs()` had just a single entry: `{"logs": ["task 1 log", "task 1 log", "task 2 log", "task 2 log"]}`. Now, the dictionary has a correct shape: `{"task_invocation_id1": ["task 1 log", "task 1 log"], "task_invocation_id2": ["task 2 log", "task 2 log"]}`.
* Getting single task logs. Previously `orq task logs` would raise an unhandled exception. Now, it prints the log lines.
* Workflow run IDs inside logs on CE now match the expected run ID.

💅 *Improvements*
* `orq wf view` now shows `TaskInvocationID`s instead of `TaskRunID`s. This improves usage of `orq wf view` with other CLI commands that require passing invocation ID, like `orq task {logs,results}`.
* `sdk.WorkflowRun.wait_until_finished()` will now print workflow status every now and then.

🥷 *Internal*
* Git URL model changed inside the IR
* `orq up` will now configure Ray's Plasma directory

📃 *Docs*
* Guide: Dependency Installation - sources, order, and best practice

## v0.43.0

🚨 *Breaking Changes*
* Brand-new `orq` CLI with simplified command tree and interactive prompts when a required argument isn't passed. New commands:
  * `orq workflow submit`
  * `orq workflow view`
  * `orq workflow list`
  * `orq workflow stop`
  * `orq workflow logs`
  * `orq workflow results`
  * `orq wf` as a shorthand for `orq workflow`
  * `orq task logs`
  * `orq task results`
  * `orq up`
  * `orq down`
  * `orq status`
  * `orq login`
* `sdk.WorkflowRun.get_logs()` doesn't accept any arguments any more. Now, it returns all the logs produced by the tasks in the workflow. If you're interested in only a subset of your workflow's logs, please consider using one of the following filtering options:

```python
from orquestra import sdk
from orquestra.sdk.schema.workflow_run import State

wf_run = sdk.WorkflowRun.by_id("foo")

logs = wf_run.get_logs()
# Option 1
single_task_logs = logs["my_inv_id"]

# Option 2
logs_subset = {id: lines for id, lines in logs.items() if id in ["foo", "bar", "baz"]}

# Option 3
for task in wf_run.get_tasks():
    if task.get_status() == State.FAILED:
        print(task.get_logs())
```

* `sdk.WorkflowRun.get_artifacts()` doesn't accept any arguments any more. Now, it returns all the artifacts produced by the tasks in the workflow.
* `sdk.TaskRun.get_logs()` returns a list of log lines produced by this task. Previously, it returned a dictionary with one entry.
* Executing a workflow on Ray with Git imports will now install them. A known limitation is that this will only work for Git repositories that are Python packages and will fail for Git repositories that are not Python packages.
* The API will no longer accept `config_save_file` as optional parameters, from now on if you want to use a different config file use the `ORQ_CONFIG_PATH` environment variable.

🔥 *Features*

* `list_workflow_runs` added to the Public API. This lets you list the workflows for a given config, for example `sdk.list_workflow_runs("ray")` or `sdk.list_workflow_runs("prod-d")`.

🐛 *Bug Fixes*

* Fixed broken link on docs landing page.
* Internal logs from Ray are no longer displayed.
* Fixed the docstrings for `sdk.WorkflowRun.get_artifacts()`. It returns a dictionary with `TaskInvocationID` as keys and whatever the task returns as values.
* Fixed bug where some log line from Ray may be duplicated when viewing logs
* Tasks with duplicate imports will no longer fail when running on QE
* AST parser will no longer print a lot of "Info" messages
* `sdk.WorkflowRun.get_logs()` now only returns logs produced by the user. Previously, it included internal debug messages produced by Ray.
* Logs from workflows submitted to Ray are now always returned as JSONL lines

## v0.42.0

🚨 *Breaking Changes*

* `sdk.WorkflowRun.by_id()` has a new positional parameter. `sdk.WorkflowRun.by_id("wf.1", "my/project/path", "my/config/path)` becomes `sdk.WorkflowRun.by_id("wf.1", project_dir="my/project/path", config_save_file="my/config/path)`
* `in_process` runtime now executes workflows in topological order. This may be different to the order tasks were called in the workflow function.
* Configs can no longer be named. For in-process, use "in_process" name, for local ray "ray" or "local". For QE remote - config name is auto generated based on URI (for `https://prod-d.orquestra.io/` name becomes "prod-d" as an example).
* Removed ray_linked runtime.

👩‍🔬 *Experimental*

* Optional `config` param in `sdk.WorkflowRun.by_id()`. Allows access to workflows submitted by other users or from another machine, if the selected runtime supports it. Per-runtime support will be added separately.
* New CLI command: `python -m orquestra.sdk._base.cli._dorq._entry workflow stop`.
* New CLI commands that require `config` and `workflow_run_id` will now prompt the user for selecting value interactively.
* New CLI commands: `python -m orquestra.sdk._base.cli._dorq._entry up|down|status` for managing local services.

## v0.41.0

🚨 *Breaking Changes*

* `ray` is now a reserved configuration name.

🔥 *Features*

* Workflow def graph visualization via `my_wf().graph` (#317). The output object is rendered natively by Jupyter. For now, this shows task invocations and artifacts. Tasks run status will be added in the future, stay tuned!
* Automatically get the workflow run and task run ID on Argo when using `wfprint()`.
* `"ray"` can be used as an alias for `"local"` when using `RuntimeConfig.load(...)` or `WorkflowRun.run()`.
* New Python API to traverse and debug tasks in workflow: `wf_run.get_tasks()` (#283)
* New Runtime Config for CE (AKA Ray Remote): `RuntimeConfig.ce()`
* Public API for serializable model of the workflow run state: `sdk.WorkflowRun.get_status_model()`. Potential use cases include workflow status visualization by external tools.

👩‍🔬 *Experimental*

* Prototype of a new CLI command tree: `python -m orquestra.sdk._base.cli._dorq._entry workflow {submit,view}`.
* New Runtime for the Compute Engine API

🐛 *Bug Fixes*

* Fix `sdk.task()` typing when constructing a task inside a workflow.
* Fix getting logs from Ray after restarting the cluster, when not using Fluent.
* `WorkflowRun.get_results()` should return a Sequence instead of an Iterable.

🥷 *Internal*

* Allow Studio/Portal to override SDK internals.
* Workflow run endpoints added to the workflow driver client
* Deserialise workflow artifacts from Workflow Driver
* Add CODEOWNERS file
* Simplify internal interfaces
* Refactor: move old CLI code into a submodule.
* Remove obsolete system deps installation to speed up tests.
* Ignore slow tests in local development.

📃 *Docs*

* Streamline installation & quickstart tutorials (#324)
* Create tutorial for parametrized workflows.
* Cleanup and unify tutorials for local Ray execution
* Cleanup and unify tutorials for remote QE execution (#341)
* Add recipe for running VQE on quantum hardware.

## v0.40.0

🚨 *Breaking Changes*

* `WorkflowDef.run()` now requires passing config name explicitly (#308)

🔥 *Features*

* public API for secrets (#304). Supports usage from workflows on Ray, QE, and local scripts.
* `orq create-config` command for explicit creation of configs

🐛 *Bug Fixes*

* Fix Unknown error '_DockerClient' object has no attribute '_client' error when `python_on_whales` is installed (#314)
* Raise exception if `TaskDef` is used in a workflow but isn't called (#312)
* Fix handling nested calls in workflows: `fn()()` (#311)
