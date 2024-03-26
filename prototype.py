################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

import inspect
from typing import Callable, Iterable, Optional, Union

import orquestra.sdk as sdk
from orquestra.sdk.schema.ir import Import, Resources, TaskDef


class OrqRunner:
    def __init__(
        self,
        config: str,
        workspace_id: Optional[str] = None,
        project_id: Optional[str] = None,
        resources: Optional[Resources] = None,
        dependency_imports: Union[Iterable[Import], Import, None] = None,
    ):
        self.config: str = config
        self.workspace_id: Optional[str] = workspace_id
        self.project_id: Optional[str] = project_id
        self.resources: Optional[Resources] = resources
        self.dependency_imports: Union[
            Iterable[Import], Import, None
        ] = dependency_imports

    @property
    def task_decorator_kwargs(self) -> dict:
        # Why a getter?
        # 1. It neatly encompasses all of the logic required to construct the args
        # list, reducing boilerplate in the map/starmap/etc. methods.
        # 2. This provides a neat way to extract the args and kwargs if the user wanted
        # to convert to using decorated workflows.
        # 3. We could compute this in the __init__ method, but this way the user can
        # change the OrqPool properties freely and the kwargs will change to match.
        # 3. We could compute this in a normal method, but since these args and kwargs
        # are derives solely from properties it makes intuitive sense for them to be
        # presented as properties.

        kwargs = {
            "source_import": sdk.InlineImport(),
        }
        if self.dependency_imports:
            kwargs["dependency_imports"] = self.dependency_imports
        if self.resources:
            kwargs["resources"] = sdk.Resources(
                cpu=self.resources.cpu,
                memory=self.resources.memory,
                disk=self.resources.disk,
                gpu=self.resources.gpu,
            )
        return kwargs

    @property
    def workflow_decorator_kwargs(self) -> dict:
        kwargs = {}
        if self.resources:
            kwargs["resources"] = sdk.Resources(nodes=self.resources.nodes)
        return kwargs

    @property
    def workflow_run_args(self) -> tuple:
        args = (self.config,)
        return args

    @property
    def workflow_run_kwargs(self) -> dict:
        kwargs = {}
        if self.workspace_id:
            kwargs["workspace_id"] = self.workspace_id
        if self.project_id:
            kwargs["project_id"] = self.project_id
        return kwargs

    def run(self, function: Callable, iterable: Iterable, name: Optional[str] = None):
        task: TaskDef = sdk.task(function, **self.task_decorator_kwargs)

        @sdk.workflow(**self.workflow_decorator_kwargs, custom_name=name)
        def map_wf():
            return [task(x) for x in iterable]

        @sdk.workflow(**self.workflow_decorator_kwargs, custom_name=name)
        def starmap_wf():
            return [task(*x) for x in iterable]

        if len(inspect.signature(function).parameters) == 1:
            return map_wf().run(*self.workflow_run_args, **self.workflow_run_kwargs)
        else:
            return starmap_wf().run(*self.workflow_run_args, **self.workflow_run_kwargs)

    def run_blocking(
        self, function: Callable, iterable: Iterable, name: Optional[str] = None
    ):
        return self.run(function, iterable, name=name).get_results(wait=True)

    def generate_decorated_workflow(self, func: Callable, iterable: Iterable):
        """Generate source code for an equivalent workflow run using the decorators.

        This is not on the critical path, and if we don't get around to it that's okay!

        If we do implement this, we can probably make task_decorator_kwargs,
        workflow_decorator_kwargs, workflow_run_args, and workflow_run_kwargs private.
        """
        outstr = "import orquestra.sdk as sdk\n\n"

        # construct task decorator
        task_decorator = "sdk.task("
        for key in self.task_decorator_kwargs:
            task_decorator += f"\n    {key} = {self.task_decorator_kwargs[key]},"
        task_decorator += "\n)\n"

        # construct task source
        task_source = inspect.getsource(func) + "\n"

        # construct workflow decorator
        workflow_decorator = "sdk.workflow("
        wf_kwargs = self.workflow_decorator_kwargs
        for key in wf_kwargs:
            workflow_decorator += f"\n    {key} = {wf_kwargs[key]},"
        workflow_decorator += "\n)\n"

        # construct workflow source
        workflow_source = "def wf():\n    return "
        # If the members of the iterable are iterables, use the starmap version,
        # otherwise use map.
        try:
            len(iterable[0])
        except TypeError:
            workflow_source += f"[{func.__name__}(x) for x in {iterable}]"
        else:
            workflow_source += f"[{func.__name__}(*x) for x in {iterable}]"
        workflow_source += "\n\n"

        # construct workflow run command
        wf_run = "wf_run = wf().run(\n"
        for arg in self.workflow_run_args:
            wf_run += f"    {arg},\n"
        for key in self.workflow_run_kwargs:
            wf_run += f"    {key} = {self.workflow_run_kwargs[key]},\n"
        wf_run += ")\n\n"

        # construct results command
        results = "results = wf_run.get_results(wait=True)\n\n"

        return (
            outstr
            + task_decorator
            + task_source
            + workflow_decorator
            + workflow_source
            + wf_run
            + results
        )


# ====================================== DEMO ==========================================
# Set up the execution environment
orq_runner = OrqRunner(
    "local",
    workspace_id=None,
    project_id=None,
    resources=sdk.Resources(cpu=1, nodes=4),
    dependency_imports=[],
)


# Define some functions
def single_input_func(x):
    return x * x


def multiple_input_func(x, y):
    return x * y


# Map
print(
    orq_runner.run_blocking(single_input_func, [1, 2, 3, 4], name="map_run")
)  # prints (1,4,9,16)

# Starmap
print(
    orq_runner.run_blocking(
        multiple_input_func, [[1, 2], [3, 4], [5, 6], [7, 8]], name="starmap_run"
    )
)  # prints (2, 12, 30, 56)

# Map async
map_async_run = orq_runner.run(single_input_func, [1, 2, 3, 4], name="map_async_run")
print(map_async_run.get_results(wait=True))  # prints (1,4,9,16)

# Starmap async
starmap_async_run = orq_runner.run(
    multiple_input_func, [[1, 2], [3, 4], [5, 6], [7, 8]]
)
print(starmap_async_run.get_results(wait=True))  # prints (2, 12, 30, 56)


# generate decorated equivalents.
print(orq_runner.generate_decorated_workflow(single_input_func, [1, 2, 3, 4]))
print(
    orq_runner.generate_decorated_workflow(
        multiple_input_func, [[1, 2], [3, 4], [5, 6], [7, 8]]
    )
)
# ======================================================================================
