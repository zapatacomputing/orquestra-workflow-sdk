################################################################################
# © Copyright 2021-2022 Zapata Computing Inc.
################################################################################
"""Tests workflow graph traversal, i.e. extracting the serializable IR from
functions decorated with @sdk.workflow and @sdk.task.
"""
import json
import re
import time
import typing as t
from contextlib import nullcontext as does_not_raise
from unittest.mock import Mock

import git
import pytest
from git.remote import Remote

import orquestra.sdk.schema.ir as ir
from orquestra.sdk import exceptions, secrets
from orquestra.sdk._base import _dsl, _traversal, _workflow, dispatch, serde
from orquestra.sdk.packaging import _versions

from .data.complex_serialization.workflow_defs import (
    generate_object_with_num,
    generate_simple_callable,
    generate_simple_callables,
    get_object_id,
    get_object_num,
    get_objects_id,
    invoke_callable,
    invoke_callables,
    join_strings,
    make_a_string,
    wf_futures_in_container_should_fail,
    wf_object_id,
    wf_objects_id,
    wf_pass_callable,
    wf_pass_callable_from_task,
    wf_pass_callable_from_task_inside_cont_should_fail,
    wf_pass_callables,
    wf_pass_callables_from_task,
    wf_pass_obj_with_num,
    wf_pass_obj_with_num_from_task,
)


# --- Tasks used in tests --- #
@_dsl.task()
def capitalize(text: str):
    return text.capitalize()


@_dsl.task(
    resources=_dsl.Resources(
        cpu="2000m",
        memory="10Gi",
    )
)
def capitalize_resourced(text: str):
    return text.capitalize()


@_dsl.task()
def uppercase(text: str):
    return text.upper()


@_dsl.task()
def increment(number):
    return number + 1


@_dsl.task(source_import=_dsl.GitImport(repo_url="hello", git_ref="main"))
def git_task():
    pass


@_dsl.task(source_import=_dsl.GitImport(repo_url="there", git_ref="main"))
def git_task2():
    pass


@_dsl.task(
    dependency_imports=[
        _dsl.GitImport(
            repo_url="git@github.com:zapatacomputing/orquestra-workflow-sdk.git",
            git_ref="main",
        )
    ],
)
def generate_graph():
    import zquantum.core.graph  # type: ignore

    graph = zquantum.core.graph.generate_random_graph_erdos_renyi(
        num_nodes=10,
        edge_probability=0.4,
        seed=0,
    )
    return graph


@_dsl.task(
    source_import=_dsl.GitImport.infer(git_ref="main"),
)
def capitalize_infer(text: str):
    return text.capitalize()


@_dsl.task(
    source_import=_dsl.InlineImport(),
)
def capitalize_inline(text: str):
    return text.capitalize()


@_dsl.task(dependency_imports=[_dsl.PythonImports("numpy==1.21.5")])
def python_imports_manual(text: str):
    return text.capitalize()


@_dsl.task(
    dependency_imports=[
        _dsl.PythonImports(file="tests/sdk/data/requirements_with_extras.txt")
    ]
)
def python_imports_from_requirements(text: str):
    return text.capitalize()


@_dsl.task
def task_arg_test(a, b, *args, c, d="x", **kwargs):
    pass


@_dsl.task
def pickled(value):
    pass


@_dsl.task(n_outputs=2)
def two_outputs():
    pass


@_dsl.task
def three_outputs():
    return "a", "b", "c"


dupe_import = _dsl.GithubImport("zapatacomputing/test")


@_dsl.task(source_import=dupe_import, dependency_imports=[dupe_import])
def duplicate_deps():
    pass


# - End Tasks used in tests - #


class AnyPositiveInt(int):
    """Is equal to any integer greater than 0. Used as a placeholder.

    It's a subclass of `int` to allow storing it in Pydantic models.
    """

    def __eq__(self, o):
        return isinstance(o, int) and o > 0

    def __str__(self):
        return "<AnyPositiveInt>"

    def __repr__(self):
        return "AnyPositiveInt()"


class AnyMatchingStr(str):
    def __init__(self, pattern: str):
        self.pattern = pattern

    def __eq__(self, o):
        return isinstance(o, str) and re.match(self.pattern, o) is not None

    def __str__(self):
        return f"<AnyMatchingStr {repr(self.pattern)}>"

    def __repr__(self):
        return f"AnyMatchingStr({repr(self.pattern)})"


@_workflow.workflow
def single_invocation():
    name = "emiliano"
    capitalized = capitalize(name)
    return [capitalized]


@_workflow.workflow
def kwargs_invocation():
    name = "emiliano"
    capitalized = capitalize(text=name)
    return [capitalized]


@_workflow.workflow
def multiple_git_imports():
    return [
        git_task(),
        git_task2(),
    ]


@_workflow.workflow
def multiple_invocations():
    first = "emiliano"
    last = "zapata"
    return [capitalize(first), capitalize(last)]


@_workflow.workflow
def multiple_identical_invocations():
    first = "emiliano"
    return [capitalize(first), capitalize(first)]


@_workflow.workflow
def multiple_tasks():
    first = "emiliano"
    last = "zapata"
    return [capitalize(first), uppercase(last)]


@_workflow.workflow
def daisy_chain():
    number = 42
    plus_1 = increment(number)
    plus_2 = increment(plus_1)
    return [plus_2]


@_workflow.workflow
def no_tasks():
    first = "emiliano"
    last = "zapata"
    return [first, last]


@_workflow.workflow
def wf_with_git_deps():
    """Single task invocation. The task's source import is local, but it has a
    git-based dependency.
    """
    graph = generate_graph()
    return [graph]


@_workflow.workflow
def arg_test():
    return [
        task_arg_test("a", "b", c="c"),
        task_arg_test("a", b="b", c="c"),
        task_arg_test("a", "b", c="c", d="d"),
    ]


@_workflow.workflow
def resources_invocation():
    name = "emiliano"
    with_task_meta = capitalize_resourced(name)
    with_inv_meta = with_task_meta.with_resources(
        cpu="1000m",
        memory=None,
    )
    no_meta = capitalize(name)

    return [with_task_meta, with_inv_meta, no_meta]


@_workflow.workflow
def unhashable_constants():
    names = ["emiliano", "zapata"]
    return [join_strings(names)]


@_workflow.workflow
def two_task_outputs():
    # We intentionally set wrong output number vs what is returned by task to see
    # if parsing is correct
    a, b = two_outputs()  # pyright:ignore
    _, c = two_outputs()  # pyright:ignore
    return [a, b, c]


@_workflow.workflow
def two_task_outputs_all_used():
    a, b = two_outputs()  # pyright:ignore
    return a, b


@_workflow.workflow
def two_task_outputs_packed_returned():
    packed = two_outputs()
    a, b = packed  # pyright:ignore
    return a, b, packed


@_workflow.workflow
def three_task_outputs():
    foo1, bar1, baz1 = three_outputs()
    _, bar2, baz2 = three_outputs()
    foo3, bar3, _ = three_outputs()
    _, bar4, _ = three_outputs()
    foo5, _, baz5 = three_outputs()

    return foo1, bar1, baz1, bar2, baz2, foo3, bar3, bar4, foo5, baz5


@_workflow.workflow
def constant_return():
    a = simple_task(1)
    return [42, a]


@_workflow.workflow
def constant_collisions():
    return [1.0, 1, True, simple_task(1)]


@_dsl.task()
def simple_task(a):
    return a


@_workflow.workflow
def large_workflow():
    val = 0
    for _ in range(1000):
        val = simple_task(val)
    return [val]


@_workflow.workflow
def wf_with_inline():
    return [capitalize_inline("hello")]


@_workflow.workflow
def dupe_import_wf():
    return duplicate_deps()


@_workflow.workflow
def workflow_with_secret():
    secret = secrets.get("my_secret", workspace_id="ws")
    return simple_task(secret)


def _id_from_wf(param):
    if isinstance(param, (_workflow.WorkflowDef, ir.WorkflowDef)):
        return param.name


def _first(iterable):
    return next(iter(iterable))


class TestFlattenGraph:
    def test_kwargs_invocation(self):
        # preconditions
        wf = kwargs_invocation.model
        assert len(wf.tasks) == 1, "This test assumes one task"
        assert len(wf.task_invocations) == 1, "This test assumes one invocation"
        assert len(wf.constant_nodes) == 1, "This test assumes one constant"

        task = _first(wf.tasks.values())
        assert len(task.parameters) == 1, "This test assumes one task parameter"

        # main assertions
        invocation = _first(wf.task_invocations.values())
        constant_id = _first(wf.constant_nodes.keys())
        assert invocation.args_ids == []
        assert invocation.kwargs_ids == {task.parameters[0].name: constant_id}

    def test_task_with_git_deps(self):
        # preconditions
        wf = wf_with_git_deps.model
        assert len(wf.task_invocations) == 1, "This test assumes one invocation"
        invocation = _first(wf.task_invocations.values())
        task = wf.tasks[invocation.task_id]
        source_import = wf.imports[task.source_import_id]

        assert task.dependency_import_ids is not None
        assert len(task.dependency_import_ids) == 1
        dep_import = wf.imports[task.dependency_import_ids[0]]

        # main assertions
        assert isinstance(source_import, ir.InlineImport)
        assert isinstance(dep_import, ir.GitImport)

    def test_setting_invocation_metadata(self):
        # preconditions
        wf = resources_invocation.model
        with_inv_meta, with_task_meta, no_meta = [
            invocation
            for output_id in wf.output_ids
            for invocation in wf.task_invocations.values()
            if output_id in invocation.output_ids
        ]

        # main assertions
        assert with_inv_meta.resources == ir.Resources(
            cpu="2000m",
            memory="10Gi",
            disk=None,
            gpu=None,
        )
        assert with_task_meta.resources == ir.Resources(
            cpu="1000m",
            memory=None,
            disk=None,
            gpu=None,
        )

        assert no_meta.resources is None

    def test_constant_return_workflow(self):
        wf = constant_return.model
        assert len(wf.task_invocations) == 1
        assert len(wf.tasks) == 1
        assert len(wf.constant_nodes) == 2
        assert len(wf.output_ids) == 2
        assert wf.output_ids[0] in wf.constant_nodes

    def test_equal_constants_different_types(self):
        wf = constant_collisions.model
        constant_nodes = []
        for constant_node in wf.constant_nodes.values():
            assert isinstance(constant_node, ir.ConstantNodeJSON)
            constant_nodes.append(constant_node)

        serialised_constants = [con.value for con in constant_nodes]
        assert len(wf.constant_nodes) == 3
        assert "true" in serialised_constants
        assert "1" in serialised_constants
        assert "1.0" in serialised_constants

    def test_workflow_without_data_aggregation(self):
        wf = constant_return.model
        assert wf.data_aggregation is None

    @pytest.mark.filterwarnings("ignore:data_aggregation")
    def test_workflow_with_data_aggregation(self):
        @_workflow.workflow(
            data_aggregation=_workflow.DataAggregation(
                resources=_dsl.Resources(cpu="1")
            )
        )
        def workflow_with_data_aggregation():
            return capitalize("hello there")

        wf = workflow_with_data_aggregation.model
        assert wf.data_aggregation is None

    def test_large_workflow(self):
        wf = large_workflow.model
        assert wf is not None
        assert len(wf.tasks) == 1
        assert len(wf.artifact_nodes) == 1000
        assert len(wf.constant_nodes) == 1

    def test_workflow_with_inline(self):
        wf = wf_with_inline.model

        assert len(wf.imports) == 1
        (imp,) = wf.imports.values()
        assert isinstance(imp, ir.InlineImport)

        assert len(wf.tasks) == 1
        (task_def,) = wf.tasks.values()
        assert task_def.source_import_id == imp.id

    def test_duplicate_deps(self):
        wf = dupe_import_wf.model

        tasks = list(wf.tasks.values())
        assert len(wf.imports) == 1
        assert len(tasks) == 1
        assert tasks[0].dependency_import_ids is not None
        assert tasks[0].source_import_id not in tasks[0].dependency_import_ids

    def test_raises_exception_for_zero_tasks(self):
        # preconditions
        with pytest.raises(exceptions.WorkflowSyntaxError) as e:
            no_tasks.model
        assert e.match(
            r"The workflow 'no_tasks' \(defined at .+\.py line \d+\) cannot be submitted as it does not define any tasks to be executed. Please modify the workflow definition to define at least one task and retry submitting the workflow."  # noqa: E501
        )


class ContextManager(t.Protocol):
    def __enter__(
        self,
        *args,
        **kwargs,
    ):
        ...

    def __exit__(
        self,
        *args,
        **kwargs,
    ):
        ...


@pytest.mark.parametrize(
    "workflow_template, task_defs, outputs, expectation",
    [
        (single_invocation, [capitalize], ["Emiliano"], does_not_raise()),
        (kwargs_invocation, [capitalize], ["Emiliano"], does_not_raise()),
        (multiple_git_imports, [git_task, git_task2], None, does_not_raise()),
        (multiple_invocations, [capitalize], ["Emiliano", "Zapata"], does_not_raise()),
        (
            multiple_identical_invocations,
            [capitalize],
            ["Emiliano", "Emiliano"],
            does_not_raise(),
        ),
        (
            multiple_tasks,
            [capitalize, uppercase],
            ["Emiliano", "ZAPATA"],
            does_not_raise(),
        ),
        (daisy_chain, [increment], [44], does_not_raise()),
        (wf_with_git_deps, [generate_graph], None, does_not_raise()),
        (arg_test, [task_arg_test], None, does_not_raise()),
        (unhashable_constants, [join_strings], ["emilianozapata"], does_not_raise()),
        (resources_invocation, [capitalize_resourced], None, does_not_raise()),
        (two_task_outputs, [two_outputs], None, does_not_raise()),
        (three_task_outputs, [three_outputs], None, does_not_raise()),
        (wf_object_id, [get_object_id], None, does_not_raise()),
        (wf_objects_id, [get_objects_id], None, does_not_raise()),
        (wf_pass_callable, [invoke_callable], [43], does_not_raise()),
        (
            wf_pass_callable_from_task,
            [generate_simple_callable, invoke_callable],
            [43],
            does_not_raise(),
        ),
        (wf_pass_callables, [invoke_callables], [[43, 44]], does_not_raise()),
        (
            wf_pass_callables_from_task,
            [generate_simple_callables, invoke_callables],
            [[43, 44]],
            does_not_raise(),
        ),
        (wf_pass_obj_with_num, [get_object_num], [1], does_not_raise()),
        (
            wf_pass_obj_with_num_from_task,
            [get_object_num, generate_object_with_num],
            [1],
            does_not_raise(),
        ),
        # Test case for unsupported feature, to embed an artifact future object
        # in a container
        (
            wf_pass_callable_from_task_inside_cont_should_fail,
            [generate_simple_callable, invoke_callables],
            [[43]],
            pytest.raises(exceptions.WorkflowSyntaxError),
        ),
        (
            wf_futures_in_container_should_fail,
            [make_a_string, join_strings],
            ["hellohello"],
            pytest.raises(exceptions.WorkflowSyntaxError),
        ),
    ],
    ids=_id_from_wf,
)
class TestWorkflowsTasksProperties:
    @staticmethod
    def test_wf_imports_are_task_imports(
        workflow_template: _workflow.WorkflowTemplate,
        task_defs: t.Sequence[_dsl.TaskDef],
        outputs: t.List,
        expectation: ContextManager,
    ):
        """
        Import IDs refered from task defs should be part of wf def IR.
        """
        with expectation:
            wf = workflow_template.model
            wf_import_ids = set(wf.imports.keys())

            # refers to tasks embedded inside workflow model
            task_import_ids_embedded = {
                import_id
                for task in wf.tasks.values()
                for import_id in [
                    task.source_import_id,
                    *(task.dependency_import_ids or []),
                ]
            }

            assert task_import_ids_embedded == wf_import_ids

    @staticmethod
    def test_task_ids_match_invocations(
        workflow_template: _workflow.WorkflowTemplate,
        task_defs: t.Sequence[_dsl.TaskDef],
        outputs: t.List,
        expectation: ContextManager,
    ):
        """
        Task def referenced from invocation should be part of the workflow def.
        """
        with expectation:
            wf = workflow_template.model
            task_def_ids = set(wf.tasks.keys())

            for inv in wf.task_invocations.values():
                assert inv.task_id in task_def_ids

    @staticmethod
    def test_number_of_invocations(
        workflow_template: _workflow.WorkflowTemplate,
        task_defs: t.Sequence[_dsl.TaskDef],
        outputs: t.List,
        expectation: ContextManager,
    ):
        """
        Number of task defs shouldn't exceed the number of invocations. Reasoning: each
        task def should be invoked at least once.
        """
        with expectation:
            wf = workflow_template.model
            assert len(wf.tasks) <= len(wf.task_invocations)

    @staticmethod
    def test_invocation_outputs_are_unique(
        workflow_template: _workflow.WorkflowTemplate,
        task_defs: t.Sequence[_dsl.TaskDef],
        outputs: t.List,
        expectation: ContextManager,
    ):
        """
        A given node ID should be only produced by a single task invocation.
        """
        with expectation:
            wf = workflow_template.model
            seen_ids: t.Set[ir.ArtifactNodeId] = set()
            for invocation in wf.task_invocations.values():
                assert set(invocation.output_ids).isdisjoint(seen_ids)
                seen_ids.update(invocation.output_ids)

    @staticmethod
    def test_n_invocation_outputs_matches_task_def(
        workflow_template: _workflow.WorkflowTemplate,
        task_defs: t.Sequence[_dsl.TaskDef],
        outputs: t.List,
        expectation: ContextManager,
    ):
        """
        Number of task invocation output IDs should correspond to task def's outputs.
        """
        with expectation:
            wf_def = workflow_template.model
            for inv in wf_def.task_invocations.values():
                task_def_model = wf_def.tasks[inv.task_id]
                task_def_obj = dispatch.locate_fn_ref(task_def_model.fn_ref)
                # We assume that `fn_ref` points to a @task() decorated function.
                assert hasattr(task_def_obj, "_output_metadata")
                output_metadata = getattr(task_def_obj, "_output_metadata")
                assert hasattr(output_metadata, "is_subscriptable")
                assert hasattr(output_metadata, "n_outputs")

                if getattr(output_metadata, "is_subscriptable"):
                    # n + 1 artifacts for n-output task def:
                    # - one artifact for each output to handle unpacking
                    # - one artifact overall to handle using non-unpacked future
                    assert (
                        len(inv.output_ids) == getattr(output_metadata, "n_outputs") + 1
                    )
                else:
                    assert len(inv.output_ids) == 1

    @staticmethod
    def test_no_hanging_inputs(
        workflow_template: _workflow.WorkflowTemplate,
        task_defs: t.Sequence[_dsl.TaskDef],
        outputs: t.List,
        expectation: ContextManager,
    ):
        """
        Node IDs used as task inputs should point to constants or task outputs.
        """
        with expectation:
            wf = workflow_template.model
            constant_ids = set(wf.constant_nodes.keys())
            output_ids = {
                output_id
                for invocation in wf.task_invocations.values()
                for output_id in invocation.output_ids
            }
            filled_ids = constant_ids.union(output_ids)

            arg_input_ids = {
                input_id
                for invocation in wf.task_invocations.values()
                for input_id in invocation.args_ids
            }
            assert arg_input_ids.issubset(filled_ids)

            kwarg_input_ids = {
                input_id
                for invocation in wf.task_invocations.values()
                for input_id in invocation.kwargs_ids.values()
            }
            assert kwarg_input_ids.issubset(filled_ids)

    @staticmethod
    def test_constants_can_be_read(
        workflow_template: _workflow.WorkflowTemplate,
        task_defs: t.Sequence[_dsl.TaskDef],
        outputs: t.List,
        expectation: ContextManager,
    ):
        """
        It should be possible to deserialize constant nodes embedded in the IR using
        ``serde``.
        """
        with expectation:
            wf = workflow_template.model
            for constant in wf.constant_nodes.values():
                constant_dict = json.loads(constant.json())
                _ = serde.value_from_result_dict(constant_dict)

    @staticmethod
    def test_no_hanging_wf_outputs(
        workflow_template: _workflow.WorkflowTemplate,
        task_defs: t.Sequence[_dsl.TaskDef],
        outputs: t.List,
        expectation: ContextManager,
    ):
        """
        IDs of the workflow output artifacts should be a subset of task invocation
        output IDs.
        """
        with expectation:
            wf = workflow_template.model
            task_output_ids = {
                output_id
                for invocation in wf.task_invocations.values()
                for output_id in invocation.output_ids
            }
            assert set(wf.output_ids).issubset(task_output_ids)


CAPITALIZE_TASK_DEF = ir.TaskDef(
    id=AnyMatchingStr(r"task-capitalize-\w{10}"),
    fn_ref=ir.InlineFunctionRef(
        function_name="capitalize",
        encoded_function=[AnyMatchingStr(".*")],
    ),
    output_metadata=ir.TaskOutputMetadata(is_subscriptable=False, n_outputs=1),
    parameters=[
        ir.TaskParameter(name="text", kind=ir.ParameterKind.POSITIONAL_OR_KEYWORD)
    ],
    source_import_id=AnyMatchingStr(r"inline-import-\w{1}"),
    custom_image=None,
)

CAPITALIZE_INLINE_TASK_DEF = ir.TaskDef(
    id=AnyMatchingStr(r"task-capitalize-inline-\w{10}"),
    fn_ref=ir.InlineFunctionRef(
        function_name="capitalize_inline",
        encoded_function=[AnyMatchingStr(r".*")],  # dont test actual encoding here
    ),
    output_metadata=ir.TaskOutputMetadata(is_subscriptable=False, n_outputs=1),
    parameters=[
        ir.TaskParameter(name="text", kind=ir.ParameterKind.POSITIONAL_OR_KEYWORD)
    ],
    source_import_id=AnyMatchingStr(r"inline-import-\w{1}"),
    custom_image=None,
)

GIT_TASK_DEF = ir.TaskDef(
    id=AnyMatchingStr(r"task-git-task-\w{10}"),
    fn_ref=ir.ModuleFunctionRef(
        module="tests.sdk.test_traversal",
        function_name="git_task",
        file_path="tests/sdk/test_traversal.py",
        line_number=AnyPositiveInt(),
    ),
    output_metadata=ir.TaskOutputMetadata(is_subscriptable=False, n_outputs=1),
    parameters=[],
    source_import_id=AnyMatchingStr(r"git-\w{10}_hello"),
    custom_image=None,
)


GENERATE_GRAPH_TASK_DEF = ir.TaskDef(
    id=AnyMatchingStr(r"task-generate-graph-\w{10}"),
    fn_ref=ir.InlineFunctionRef(
        function_name="generate_graph",
        encoded_function=[AnyMatchingStr(r".*")],  # dont test actual encoding here
    ),
    output_metadata=ir.TaskOutputMetadata(is_subscriptable=False, n_outputs=1),
    parameters=[],
    source_import_id=AnyMatchingStr(r"inline-import-\w{1}"),
    dependency_import_ids=[
        AnyMatchingStr(r"git-\w{10}_github_com_zapatacomputing_orquestra_workflow_sdk")
    ],
    custom_image=None,
)

PYTHON_IMPORTS_MANUAL_TASK_DEF = ir.TaskDef(
    id=AnyMatchingStr(r"task-python-imports-manual-\w{10}"),
    fn_ref=ir.InlineFunctionRef(
        function_name="python_imports_manual",
        encoded_function=[AnyMatchingStr(r".*")],
        type="INLINE_FUNCTION_REF",
    ),
    output_metadata=ir.TaskOutputMetadata(is_subscriptable=False, n_outputs=1),
    parameters=[
        ir.TaskParameter(name="text", kind=ir.ParameterKind.POSITIONAL_OR_KEYWORD)
    ],
    source_import_id=AnyMatchingStr(r"inline-import-\w{1}"),
    dependency_import_ids=[AnyMatchingStr(r"python-import-\w{10}")],
    custom_image=None,
)


@pytest.mark.parametrize(
    "task_def, has_arg, expected_model",
    [
        (capitalize, True, CAPITALIZE_TASK_DEF),
        (git_task, False, GIT_TASK_DEF),
        (generate_graph, False, GENERATE_GRAPH_TASK_DEF),
        (capitalize_inline, True, CAPITALIZE_INLINE_TASK_DEF),
        (python_imports_manual, True, PYTHON_IMPORTS_MANUAL_TASK_DEF),
    ],
)
def test_individual_task_models(task_def, has_arg, expected_model):
    @_workflow.workflow
    def wf():
        if has_arg:
            return task_def("w/e")
        else:
            return task_def()

    wf_model = wf.model
    task_def = list(wf_model.tasks.values())[0]

    assert task_def == expected_model.dict()


@pytest.mark.parametrize(
    "repo_url,index,expected_id",
    [
        (
            "https://github.com/zapatacomputing/orquestra-workflow-sdk.git",
            0,
            "git-0_github_com_zapatacomputing_orquestra_workflow_sdk",
        ),
        ("git@remote:repo.git", 1, "git-1_remote_repo"),
        ("some&&weird:/URI", 100, "git-100_some_weird_URI"),
    ],
)
def test_make_import_id_gitimport(repo_url, index, expected_id):
    imp = _dsl.GitImport(repo_url=repo_url, git_ref="")
    assert _traversal._make_import_id(imp, index) == expected_id


def test_make_import_id_invalid_type():
    with pytest.raises(TypeError):
        _ = _traversal._make_import_id("not an import", "_")  # type: ignore


def test_make_import_model_inline_import():
    imp1 = _dsl.InlineImport()
    imp2 = _dsl.InlineImport()
    assert _traversal._make_import_model(imp1) != _traversal._make_import_model(imp2)


def test_make_import_model_git_import_with_auth():
    original_url = "https://example.com/example/repo.git"
    git_ref = "main"
    user = "emilianozapata"
    secret_name = "my_secret"
    imp = _dsl.GitImportWithAuth(
        repo_url=original_url,
        git_ref=git_ref,
        username=user,
        auth_secret=_dsl.Secret(secret_name),
    )
    imp_model = _traversal._make_import_model(imp)
    assert isinstance(imp_model, ir.GitImport)
    assert imp_model.git_ref == git_ref
    assert imp_model.repo_url.original_url == original_url
    assert imp_model.repo_url.user == user
    assert imp_model.repo_url.password is not None
    assert imp_model.repo_url.password.secret_name == secret_name


@_dsl.task()
def controller_step(*_):
    pass


@_dsl.task()
def optimiser_step(_):
    pass


@_workflow.workflow()
def workflow():
    n_trials = 100
    n_concurrent_searches = 10
    assigned_trials = 0
    current_trials = [None] * n_concurrent_searches
    controller_data = controller_step(*current_trials)
    while assigned_trials < n_trials:
        current_trials = [
            optimiser_step(controller_data) for _ in range(n_concurrent_searches)
        ]
        controller_data = controller_step(*current_trials)
        assigned_trials += n_concurrent_searches
    return [controller_data]


def test_large_number_of_constant_nodes():
    # Test workflow with big number of constant node traversal. Based on
    # real workflow example (simplified to minimum).
    # Time set here is arbitrary. Long enough that it should pass on any machine
    # Short enough that it makes it "reasonable" to wait for results
    start = time.time()
    wf = workflow.model
    end = time.time()
    assert wf is not None
    assert len(wf.constant_nodes) == 1
    assert end - start < 5


@pytest.mark.parametrize(
    "task_name, argument, expected",
    [
        ("x{arg}", 1, "name-x1-invocation-0-task-task"),
        (
            "uPpErCasE with SpaCeS",
            "",
            "name-uppercase-with-spaces-invocation-0-task-task",
        ),
        # max length is 128 characters
        ("x" * 140, "", "name-" + ("x" * 123)),
        ("!@#$%^&*()", "", "name------------invocation-0-task-task"),
        ("{arg:.2f}", 1 / 3, "name-0-33-invocation-0-task-task"),
        ("kwargs:{arg}", {"arg": "x"}, "name-kwargs-x-invocation-0-task-task"),
    ],
)
def test_custom_task_names(task_name, argument, expected):
    @_dsl.task(custom_name=task_name)
    def task(arg):
        ...

    @_workflow.workflow()
    def workflow():
        # quick hack to test kwargs without creating new test function
        if isinstance(argument, dict):
            ret = task(**argument)
        else:
            ret = task(argument)
        return [ret]

    assert list(workflow.model.task_invocations.keys())[0] == expected


class TestNumberOfFetchesOnInferRepos:
    @pytest.fixture()
    def setup_fetch(self, monkeypatch):
        fake_fetch = Mock()
        monkeypatch.setattr(Remote, "fetch", fake_fetch)

        yield fake_fetch

    @pytest.fixture()
    def mock_repo(self, monkeypatch):
        # Avoid "DirtyRepo" errors in tests
        monkeypatch.setattr(git.Repo, "is_dirty", Mock(return_value=False))

        # Avoid "detached head" warnings in tests
        fake_head = Mock()
        fake_head.is_detached = False
        fake_head.ref.name = "my_branch"
        monkeypatch.setattr(git.Repo, "head", fake_head)

    def test_one_task_multiple_invocations(self, setup_fetch, mock_repo):
        @_dsl.task(source_import=_dsl.GitImport.infer())
        def infer_task_1(prev: t.Optional[int]):
            return (prev or 0) + 1

        @_workflow.workflow()
        def wf_1():
            prev = None
            for _ in range(10):
                prev = infer_task_1(prev)
            return [prev]

        _ = wf_1.model
        assert setup_fetch.call_count == 1

    def test_multi_task_multiple_invocations(self, setup_fetch, mock_repo):
        @_dsl.task(source_import=_dsl.GitImport.infer())
        def infer_task_1(prev: t.Optional[int]):
            return (prev or 0) + 1

        @_dsl.task(source_import=_dsl.GitImport.infer())
        def infer_task_2(prev: t.Optional[int]):
            return (prev or 0) + 1

        @_workflow.workflow()
        def wf_1():
            prev = None
            for _ in range(10):
                prev = infer_task_1(prev)
                prev = infer_task_2(prev)
            return [prev]

        _ = wf_1.model
        assert setup_fetch.call_count == 1

    def test_one_task_multiple_wfs(self, setup_fetch, mock_repo):
        @_dsl.task(source_import=_dsl.GitImport.infer())
        def infer_task_1(prev: t.Optional[int]):
            return (prev or 0) + 1

        @_workflow.workflow()
        def wf_1():
            prev = None
            for _ in range(10):
                prev = infer_task_1(prev)
            return [prev]

        @_workflow.workflow()
        def wf_2():
            prev = None
            for _ in range(10):
                prev = infer_task_1(prev)
            return [prev]

        _ = wf_1.model
        _ = wf_2.model
        assert setup_fetch.call_count == 2

    def test_different_infer_parameters(self, setup_fetch, mock_repo):
        @_dsl.task(
            source_import=_dsl.DeferredGitImport(
                local_repo_path=".", git_ref="origin/main"
            )
        )
        def infer_task_1(prev: t.Optional[int]):
            return (prev or 0) + 1

        @_dsl.task(
            source_import=_dsl.DeferredGitImport(local_repo_path=".", git_ref="main")
        )
        def infer_task_2(prev: t.Optional[int]):
            return (prev or 0) + 1

        @_workflow.workflow()
        def wf_1():
            prev = None
            for _ in range(10):
                prev = infer_task_1(prev)
                prev = infer_task_2(prev)
            return [prev]

        _ = wf_1.model
        assert setup_fetch.call_count == 2


class SampleClass:
    def __init__(self, foo, bar, baz):
        self.foo = foo
        self._bar = bar
        self.__baz = baz


@pytest.mark.parametrize(
    "root,needle_type,expected",
    [
        # empty
        ([], str, []),
        # 1st level
        (["a"], str, ["a"]),
        (["a", 42, "b"], str, ["b", "a"]),
        # nested
        ([["a"]], str, ["a"]),
        ([["a"], 42, "b", ["c", 21, [37, "d"]]], str, ["d", "c", "b", "a"]),
        # dict
        ({"foo": 42, "bar": "baz"}, str, ["baz", "bar", "foo"]),
        # custom type
        (SampleClass("a", 2, {21, frozenset(["b", 37])}), str, ["b", "a"]),
        # mixed types
        ({"foo": 42, "bar": "baz"}, str, ["baz", "bar", "foo"]),
    ],
)
def test_find_obj_in_nested_fields(root, needle_type, expected):
    def _predicate(o):
        return isinstance(o, needle_type)

    assert _traversal._find_nested_objs_in_fields(root, _predicate) == expected


def test_workflow_with_secret():
    wf = workflow_with_secret().model
    assert len(wf.secret_nodes) == 1
    assert len(wf.constant_nodes) == 0
    assert wf.secret_nodes["secret-0"].secret_name == "my_secret"


def test_metadata_on_release(monkeypatch: pytest.MonkeyPatch):
    # Given
    mocked_installed_version = Mock(return_value="22.42.0")
    monkeypatch.setattr(_versions, "get_installed_version", mocked_installed_version)

    # When
    wf = workflow().model

    # Then
    assert wf.metadata is not None
    assert wf.metadata.sdk_version.major == 22
    assert wf.metadata.sdk_version.minor == 42
    assert wf.metadata.sdk_version.patch == 0
    assert not wf.metadata.sdk_version.is_prerelease


def test_metadata_on_dev(monkeypatch: pytest.MonkeyPatch):
    # Given
    mocked_installed_version = Mock(return_value="22.42.0.dev1+gitAABBCC.20230101")
    monkeypatch.setattr(_versions, "get_installed_version", mocked_installed_version)

    # When
    wf = workflow().model

    # Then
    assert wf.metadata is not None
    assert wf.metadata.sdk_version.major == 22
    assert wf.metadata.sdk_version.minor == 42
    assert wf.metadata.sdk_version.patch == 0
    assert wf.metadata.sdk_version.is_prerelease


def test_warning_when_nodes_passed_to_task():
    @_dsl.task(resources=_dsl.Resources(nodes=100))
    def _task():
        return

    @_workflow.workflow
    def _wf():
        return _task()

    with pytest.warns(exceptions.NodesInTaskResourcesWarning):
        _ = _wf().model


class TestDefaultImports:
    @_dsl.task
    def no_overwrite_task(self):
        return 21

    @_dsl.task(
        source_import=_dsl.GitImport(repo_url="overwrite_source", git_ref="source"),
        dependency_imports=[_dsl.GitImport(repo_url="overwrite", git_ref="dep")],
    )
    def do_overwrite_task(self):
        return 21

    @_dsl.task(source_import=_dsl.GitImport(repo_url="another", git_ref="source"))
    def overwrite_source_only_task(self):
        return 21

    @pytest.mark.parametrize(
        "task, expectation",
        [
            (no_overwrite_task, ("2", "1", "3", "6")),
            (do_overwrite_task, ("dep", "overwrite", "source", "overwrite_source")),
            (overwrite_source_only_task, ("2", "1", "source", "another")),
        ],
    )
    def test_wf_default_imports(self, task, expectation):
        @_workflow.workflow(
            default_source_import=_dsl.GitImport(repo_url="6", git_ref="3"),
            default_dependency_imports=[_dsl.GitImport(repo_url="1", git_ref="2")],
        )
        def wf_with_default_imports():
            return [
                task(None),
            ]

        wf_model = wf_with_default_imports.model
        task_model = list(wf_model.tasks.values())[0]
        assert task_model.dependency_import_ids
        dep_import = wf_model.imports[task_model.dependency_import_ids[0]]
        source_import = wf_model.imports[task_model.source_import_id]

        assert isinstance(dep_import, ir.GitImport)
        assert isinstance(source_import, ir.GitImport)
        assert dep_import.git_ref == expectation[0]
        assert dep_import.repo_url.original_url == expectation[1]
        assert source_import.git_ref == expectation[2]
        assert source_import.repo_url.original_url == expectation[3]


class TestGraphTraversal:
    """
    Unit tests for GraphTraversal class. Contains edge cases identified while
    integrating with other components.
    """

    @staticmethod
    def test_all_used():
        """
        In this case the artifact indices were funky.
        """
        # Given
        graph = _traversal.GraphTraversal()
        futures = _traversal.extract_root_futures(two_task_outputs_all_used())

        # When
        graph.traverse(futures)

        # Then
        assert list(graph.artifacts) == [
            ir.ArtifactNode(
                id="artifact-0-two-outputs",
                artifact_index=0,
            ),
            ir.ArtifactNode(
                id="artifact-1-two-outputs",
                artifact_index=1,
            ),
            # We expect a non-unpacked artifact node even though we instanteneously
            # unpack it to other futures.
            ir.ArtifactNode(
                id="artifact-2-two-outputs",
                artifact_index=None,
            ),
        ]

    @staticmethod
    def test_packed_and_unpacked():
        # Given
        graph = _traversal.GraphTraversal()
        wf = two_task_outputs_packed_returned()
        futures = _traversal.extract_root_futures(wf)

        # When
        graph.traverse(futures)

        # Then
        assert list(graph.artifacts) == [
            ir.ArtifactNode(
                id="artifact-0-two-outputs",
                artifact_index=0,
            ),
            ir.ArtifactNode(
                id="artifact-1-two-outputs",
                artifact_index=1,
            ),
            ir.ArtifactNode(
                id="artifact-2-two-outputs",
                artifact_index=None,
            ),
        ]

    @staticmethod
    def test_non_subscriptable_output():
        # Given
        graph = _traversal.GraphTraversal()
        wf = single_invocation()
        futures = _traversal.extract_root_futures(wf)

        # When
        graph.traverse(futures)

        # Then
        assert list(graph.artifacts) == [
            ir.ArtifactNode(
                id="artifact-0-capitalize",
                artifact_index=None,
            ),
        ]
