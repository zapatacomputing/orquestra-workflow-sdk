# Changelog

## Unreleased


🚨 *Breaking Changes*

* Pickling library switched to `cloudpickle` instead of `dill`. While no breakages are expected, this change may result in objects raising an error during pickling, even if they were previously able to be pickled. Please report any instances of these as bugs.
* Tasks with multiple outputs need to be unpacked in the workflow function:
```python
@sdk.workflow
def my_wf():
    a, b = two_output_task()
    return a, b
```
What worked previously, but now won't:
```python
@sdk.workflow
def my_wf():
    c = two_output_task()
    return c
```


🔥 *Features*


👩‍🔬 *Experimental*


🐛 *Bug Fixes*
* Fixed returning intermediate workflow values (eg. with `orq task results`) when the task has multiple outputs and only some of them were used in the rest of the workflow function. The following should work now as expected:
```python
@sdk.workflow
def my_wf():
    _, b = two_output_task()
    return b
```


💅 *Improvements*


🥷 *Internal*


📃 *Docs*


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
* Configs can no longer be named. For in-process, use "in_process" name, for local ray "ray" or "local". For QE remote - config name is auto generated based on URI (for https://prod-d.orquestra.io/ name becomes "prod-d" as an example).
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

*Internal*

* Allow Studio/Portal to override SDK internals.
* Workflow run endpoints added to the workflow driver client
* Deserialise workflow artifacts from Workflow Driver
* Add CODEOWNERS file
* Simplify internal interfaces
* Refactor: move old CLI code into a submodule.
* Remove obsolete system deps installation to speed up tests.
* Ignore slow tests in local development.

*Docs*

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
