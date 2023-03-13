################################################################################
# © Copyright 2021-2022 Zapata Computing Inc.
################################################################################
import importlib.machinery
import importlib.util
import os
import subprocess
import sys
from contextlib import suppress as do_not_raise
from pathlib import Path

import git
import pip_api.exceptions
import pytest

import orquestra.sdk as sdk
from orquestra.sdk._base import _dsl, loader
from orquestra.sdk.exceptions import DirtyGitRepo, InvalidTaskDefinitionError

DEFAULT_LOCAL_REPO_PATH = Path(__file__).parent.resolve()


@sdk.task
def _an_empty_task():
    pass


@sdk.task(resources=_dsl.Resources("1000m", "1Gi", "10Gi", "1"))
def _resourced_task():
    pass


def undecorated():
    pass


def undecorated_task_wf():
    return [undecorated()]


task = loader.FakeImportedAttribute()


def faked_task_wf():
    return [task()]


class TestArtifactFuturePublicMethods:
    """Test the public methods of ArtifactFuture"""

    tasks = [
        _resourced_task,
        _an_empty_task,
    ]
    resources_options = [  # Updated arg, expect new value
        ("cpu", {"cpu": "2000m"}),
        ("memory", {"memory": "100Gi"}),
        ("disk", {"disk": "99Gi"}),
        ("gpu", {"gpu": "1"}),
        # None arg, expect None
        ("cpu", {"cpu": None}),
        ("memory", {"memory": None}),
        ("disk", {"disk": None}),
        ("gpu", {"gpu": None}),
        # Empty args, expect default value
        ("cpu", {}),
        ("memory", {}),
        ("disk", {}),
        ("gpu", {}),
    ]

    custom_image_options = [  # Updated arg, expect new value
        (
            "custom_image",
            {"custom_image": "zapatacomputing/orquestra-not-default"},
        ),
        # None arg, expect None
        ("custom_image", {"custom_image": None}),
        # Empty args, expect default value
        ("custom_image", {}),
    ]

    @pytest.mark.parametrize(
        "task",
        tasks,
    )
    @pytest.mark.parametrize(
        "attr,kwargs",
        resources_options + custom_image_options,
    )
    def test_with_invocation_meta(self, attr, kwargs, task):
        """Check proper assignation of invocation metadata"""
        future = task()

        new_future = future.with_invocation_meta(**kwargs)
        if len(kwargs):
            expected_value = kwargs[attr]
            if attr != "custom_image":
                assert getattr(new_future.invocation.resources, attr) == expected_value
            else:
                assert getattr(new_future.invocation, attr) == expected_value
        else:
            if attr != "custom_image":
                if future.invocation.resources is None:
                    assert (
                        new_future.invocation.resources == future.invocation.resources
                    )
                else:
                    assert getattr(new_future.invocation.resources, attr) == getattr(
                        future.invocation.resources, attr
                    )
            else:
                assert getattr(new_future.invocation, attr) == getattr(
                    future.invocation, attr
                )

    @pytest.mark.parametrize(
        "task",
        tasks,
    )
    @pytest.mark.parametrize(
        "attr,kwargs",
        resources_options,
    )
    def test_with_resources(self, attr, kwargs, task):
        """Check proper assignation of invocation resources"""
        future = task()

        new_future = future.with_resources(**kwargs)
        if len(kwargs) > 0:
            expected_value = kwargs[attr]
            assert getattr(new_future.invocation.resources, attr) == expected_value
        else:
            if future.invocation.resources is None:
                assert new_future.invocation.resources == future.invocation.resources
            else:
                assert getattr(new_future.invocation.resources, attr) == getattr(
                    future.invocation.resources, attr
                )

    @pytest.mark.parametrize(
        "task",
        tasks,
    )
    @pytest.mark.parametrize(
        "attr,kwargs",
        custom_image_options,
    )
    def test_with_custom_image(self, attr, kwargs, task):
        """Check proper assignation of invocation custom image"""
        future = task()

        new_future = future.with_custom_image(**kwargs)
        if len(kwargs):
            expected_value = kwargs[attr]
            assert getattr(new_future.invocation, attr) == expected_value
        else:
            assert getattr(new_future.invocation, attr) == getattr(
                future.invocation, attr
            )


def test_task_invocation_as_dict():
    future = _an_empty_task()
    expected = {
        "task": future.invocation.task,
        "args": future.invocation.args,
        "kwargs": future.invocation.kwargs,
        "resources": future.invocation.resources,
        "custom_name": future.invocation.custom_name,
        "custom_image": future.invocation.custom_image,
        "type": future.invocation.type,
    }
    assert future.invocation._asdict() == expected


def task_no_source():
    pass


@pytest.mark.skipif(
    sys.version_info < (3, 8),
    reason="__code__.replace() is available from Python 3.8",
)
def test_task_no_linenumber_if_source_inaccessible():
    task_no_source.__code__ = task_no_source.__code__.replace(co_filename="fake")
    local_task = _dsl.task(task_no_source)
    assert isinstance(local_task.fn_ref, _dsl.ModuleFunctionRef)
    assert local_task.fn_ref.line_number is None


@pytest.mark.parametrize("num_outputs", [1, 100, 2])
def test_returning_multiple_outputs_from_tasks(num_outputs):
    @sdk.task(n_outputs=num_outputs)
    def _local_task():
        return ("hello" for _ in range(num_outputs))

    futures = [f for f in _local_task()]
    assert len(futures) == num_outputs
    for i, f in enumerate(futures):
        assert f.output_index == i


def test_explicit_tuple_unpacking_for_task():
    @sdk.task(n_outputs=2)
    def _local_task():
        return "hello", "there"

    hello, there = _local_task()
    assert hello.output_index == 0
    assert there.output_index == 1


@pytest.mark.parametrize("num_outputs", [0, -1])
def test_task_n_outputs_non_positive(num_outputs):
    with pytest.raises(ValueError):

        @sdk.task(n_outputs=num_outputs)
        def _local_task():
            return "hello", "there"


def test_accessing_deprecated_n_outputs():
    @sdk.task
    def _local_task():
        return 0, 1

    with pytest.warns(DeprecationWarning):
        assert _local_task.n_outputs == _local_task.output_metadata.n_outputs == 2


def test_iter_artifact_future():
    @sdk.task
    def _local_task():
        return [1, 2, 3]

    results = _local_task()
    result_list = list(results)
    assert len(result_list) == 3
    assert result_list[0].output_index == 0
    assert result_list[1].output_index == 1
    assert result_list[2].output_index == 2


def test_iter_artifact_future_non_iter():
    @sdk.task
    def _local_task():
        return None

    results = _local_task()
    with pytest.raises(TypeError):
        list(results)


def test_subscript_artifact_future():
    @sdk.task
    def _local_task():
        return [1, 2, 3]

    results = _local_task()
    assert results[0] is not None
    assert results[0].output_index == 0
    assert results[1].output_index == 1
    assert results[2].output_index == 2


def test_subscript_artifact_future_non_subscriptable():
    @sdk.task
    def _local_task():
        return None

    results = _local_task()
    with pytest.raises(TypeError):
        results[0]


def test_subscript_artifact_future_out_of_range():
    @sdk.task
    def _local_task():
        return [1, 2, 3]

    results = _local_task()
    with pytest.raises(IndexError):
        results[3]


# Making a slice a special case as it's usually expected with lists
def test_subscript_artifact_future_with_slice():
    @sdk.task
    def _local_task():
        return [1, 2, 3]

    results = _local_task()
    with pytest.raises(TypeError):
        results[:3]


@pytest.mark.parametrize(
    "index",
    [
        ("str"),
        (10.0),
        (None),
    ],
)
def test_subscript_artifact_future_with_non_int(index):
    @sdk.task
    def _local_task():
        return [1, 2, 3]

    results = _local_task()
    with pytest.raises(TypeError):
        results[index]


def test_resourced_task_with_custom_gpu_image():
    @sdk.task(
        resources=_dsl.Resources("1000m", "1Gi", "10Gi", "1"),
        custom_image=_dsl.GPU_IMAGE,
    )
    def _local_task():
        return "hello"

    hello = _local_task()
    assert hello.invocation.task.custom_image == _dsl.GPU_IMAGE


def test_resourced_task_with_default_gpu_image():
    future = _resourced_task()

    assert future.invocation.task.custom_image == _dsl.GPU_IMAGE


def test_task_with_default_image():
    @sdk.task()
    def _local_task():
        return "hello"

    hello = _local_task()
    assert hello.invocation.task.custom_image == _dsl.DEFAULT_IMAGE


class TestModelsSerializeProperly:
    """The properties are Pydantic models. They would raise ValidationError if the
    serialization went wrong.
    """

    def test_task_model(self):
        _an_empty_task.model

    def test_task_import_models(self):
        _an_empty_task.import_models


@pytest.fixture
def my_fake_repo_setup(tmp_path):
    """Setup a fake repo containing a commit."""

    repo_dir = tmp_path / "non-repo"
    # Create a non-bare repository where we can make our commits.
    my_fake_repo = git.Repo.init(repo_dir, bare=False)

    # Create remote repo
    url = my_fake_repo.working_tree_dir
    assert url is not None
    _ = my_fake_repo.create_remote("origin", url=str(url))

    file_name = os.path.join(repo_dir, "new-file")
    # This function just creates an empty file ...
    open(file_name, "wb").close()
    # Add and commit changes
    my_fake_repo.index.add([file_name])
    my_fake_repo.index.commit("initial commit")

    yield my_fake_repo


def test_infer_returns_deferred_import():
    local_repo = _dsl.GitImport.infer(DEFAULT_LOCAL_REPO_PATH)
    deferred_git_import = _dsl.DeferredGitImport(DEFAULT_LOCAL_REPO_PATH)
    assert local_repo.local_repo_path == deferred_git_import.local_repo_path
    assert local_repo.git_ref == deferred_git_import.git_ref


def test_deferred_git_import(my_fake_repo_setup):
    my_fake_repo = my_fake_repo_setup
    deferred_git_import = _dsl.DeferredGitImport(my_fake_repo.working_dir)
    assert deferred_git_import.local_repo_path == my_fake_repo.working_dir
    assert deferred_git_import.git_ref is None


def test_deferred_git_import_invalid_repo():
    with pytest.raises(_dsl.NotGitRepo):
        _ = _dsl.DeferredGitImport("path_to_git_repo", "my_branch").resolved()


@pytest.mark.filterwarnings("ignore: You have uncommitted")
def test_deferred_git_import_resolved_dirty_repo_does_not_raise(my_fake_repo_setup):
    my_fake_repo = my_fake_repo_setup
    # Create an empty file and add it to repo
    file_name = my_fake_repo.working_dir + "new-file"
    open(file_name, "wb").close()
    my_fake_repo.index.add([file_name])
    _ = _dsl.DeferredGitImport(my_fake_repo.working_dir).resolved()


def test_deferred_git_import_resolved_dirty_repo_warning(my_fake_repo_setup):
    my_fake_repo = my_fake_repo_setup
    # Create an empty file and add it to repo
    file_name = my_fake_repo.working_dir + "new-file"
    open(file_name, "wb").close()
    my_fake_repo.index.add([file_name])
    with pytest.warns(DirtyGitRepo):
        _ = _dsl.DeferredGitImport(my_fake_repo.working_dir).resolved()


def test_deferred_git_import_resolved_no_remote(tmp_path):
    # Initialize a bare repo
    my_fake_repo = git.Repo.init(tmp_path / "bare-repo", bare=True)
    path = my_fake_repo.working_dir
    assert path is not None

    with pytest.raises(_dsl.NoRemoteRepo):
        _ = _dsl.DeferredGitImport(str(path)).resolved()


def test_deferred_git_import_resolved_detached_head(my_fake_repo_setup):
    my_fake_repo = my_fake_repo_setup
    # Detach HEAD
    my_fake_repo.head.reference = my_fake_repo.commit("HEAD~0")

    with pytest.warns(UserWarning):
        resolved = _dsl.DeferredGitImport(my_fake_repo.working_dir).resolved()

    assert resolved.git_ref == my_fake_repo.head.object.hexsha


def test_deferred_git_import_resolved(my_fake_repo_setup):
    my_fake_repo = my_fake_repo_setup
    resolved = _dsl.DeferredGitImport(my_fake_repo.working_dir).resolved()
    assert resolved.git_ref == my_fake_repo.active_branch.name


@pytest.mark.parametrize(
    "username,personal_access_token",
    [
        (None, None),
        ("emiliano_zapata", _dsl.Secret("my_secret")),
    ],
)
def test_github_import_is_git_import_with_auth(username, personal_access_token):
    imp = _dsl.GithubImport(
        "zapatacomputing/orquestra-workflow-sdk",
        "main",
        username=username,
        personal_access_token=personal_access_token,
    )
    assert imp == _dsl.GitImportWithAuth(
        repo_url="https://github.com/zapatacomputing/orquestra-workflow-sdk.git",
        git_ref="main",
        username=username,
        auth_secret=personal_access_token,
    )


@pytest.mark.parametrize(
    "name, args, expected",
    [
        ("name", "", "name"),
        ("{args}", "abc", "abc"),
        ("name{args}name", "abc", "nameabcname"),
        ("float {args}", 1 / 3, "float 0.3333333333333333"),
        ("formatted {args:.2f}", 1 / 3, "formatted 0.33"),
    ],
)
def test_simple_custom_names_of_tasks(name, args, expected):
    @sdk.task(custom_name=name)
    def _local_task(args="default"):
        ...

    x = _local_task(args)
    assert x.invocation.custom_name == expected


def test_simple_custom_names_of_tasks_default_argument():
    @sdk.task(custom_name="{args}")
    def _local_task(args="default"):
        ...

    x = _local_task()
    assert x.invocation.custom_name == "default"


@pytest.mark.parametrize(
    "task_name",
    [
        "{x}",
        "{args}{x}",
        "{args}normal_text{args}{argss}",
    ],
)
def test_error_case_custom_names_of_tasks(task_name):
    @sdk.task(custom_name=task_name)
    def _local_task(args):
        ...

    with pytest.raises(_dsl.InvalidPlaceholderInCustomTaskNameError):
        _ = _local_task("")


def test_artifact_node_custom_names():
    @sdk.task
    def _local_task():
        ...

    @sdk.task(custom_name="My_custom_name_{x}")
    def _local_task_1(x):
        ...

    ret = _local_task()
    with pytest.warns(Warning) as warns:
        x = _local_task_1(ret)
        # don't check for specific custom name, but make sure it consists
        # dependent task name and that it invokes warning for the user
        assert _local_task.__name__ in x.invocation.custom_name
        assert len(warns.list) == 1


def test_default_for_interactive_mode(monkeypatch):
    monkeypatch.setattr(_dsl, "_is_interactive", lambda: True)

    @_dsl.task
    def task():
        ...

    @_dsl.task(source_import=_dsl.InlineImport())
    def inline_task():
        ...

    # type comparison to check if in interactive mode default is INLINE
    assert type(task.import_models[0]) is type(inline_task.import_models[0])  # noqa


def test_default_for_non_interactive_mode(monkeypatch):
    monkeypatch.setattr(_dsl, "_is_interactive", lambda: False)

    @_dsl.task
    def task():
        ...

    @_dsl.task(source_import=_dsl.LocalImport("__main__"))
    def local_task():
        ...

    # type comparison to check if in  non-interactive mode default is INLINE
    assert type(task.import_models[0]) is type(local_task.import_models[0])  # noqa


@pytest.mark.parametrize(
    "python_imports, expected_req, raises",
    [
        (sdk.PythonImports(file="Nope"), "", pytest.raises(FileNotFoundError)),
        (
            sdk.PythonImports(file="tests/sdk/v2/data/bad_requirements.txt"),
            "",
            pytest.raises(pip_api.exceptions.PipError),
        ),
        (
            sdk.PythonImports(file="tests/sdk/v2/data/requirements.txt"),
            ["joblib==1.2.0", "numpy==1.22.0"],
            do_not_raise(),
        ),
        (
            sdk.PythonImports("joblib==1.1.0", "numpy==1.21.5"),
            ["joblib==1.1.0", "numpy==1.21.5"],
            do_not_raise(),
        ),
        (
            sdk.PythonImports("joblib==1.1.0", "joblib==3.1.0"),
            "",
            pytest.raises(pip_api.exceptions.PipError),
        ),
        # conflicting requirements in file and manually passed
        (
            sdk.PythonImports(
                "joblib==3.1.0", file="tests/sdk/v2/data/requirements.txt"
            ),
            "",
            pytest.raises(pip_api.exceptions.PipError),
        ),
        # merge manually passed requirements with those in file
        (
            sdk.PythonImports(
                "scipy==1.7.3", file="tests/sdk/v2/data/requirements.txt"
            ),
            ["joblib==1.2.0", "numpy==1.22.0", "scipy==1.7.3"],
            do_not_raise(),
        ),
        (
            sdk.PythonImports(file="tests/sdk/v2/data/requirements_with_extras.txt"),
            # all features of requirement file language shown on one example
            ['requests[security,tests]==2.8.*,>=2.8.1; python_version > "2.7"'],
            do_not_raise(),
        ),
    ],
)
def test_python_imports_deps(python_imports, expected_req, raises):
    with raises:
        # concat name of requirement with its specifier
        parsed = map(lambda x: str(x), python_imports.resolved())
        assert list(parsed) == expected_req


def test_ref_to_main_in_task_error():
    path_to_workflows = (
        os.path.dirname(os.path.abspath(__file__)) + "/data/sample_project/"
    )
    # add path to locate helper imports
    sys.path.append(path_to_workflows)
    # prepare file to be executed as __main__
    loader = importlib.machinery.SourceFileLoader(
        "__main__", path_to_workflows + "workflow_defs.py"
    )
    spec = importlib.util.spec_from_loader(loader.name, loader)
    assert spec is not None
    mod = importlib.util.module_from_spec(spec)

    with pytest.raises(sdk.exceptions.InvalidTaskDefinitionError):
        loader.exec_module(mod)


def test_python_310_importlib_abc_bug():
    """
    In Python 3.10, there seems to be a bug in importlib that causes `importlib.abc` to
    fail to resolve properly following `import importlib`. As a result,

    ```bash
    python -c "import orquestra.sdk as sdk"
    ```

    can raise an AttributeError if we've used that pattern anywhere, for example:

    ```
    File "<string>", line 1, in <module>
    File "/Users/benjaminmummery/Documents/Projects/orquestra-sdk/src/orquestra/sdk/v2/__init__.py", line 23, in <module>
        from ._workflow import NotATaskWarning, WorkflowDef, WorkflowTemplate, workflow
    File "/Users/benjaminmummery/Documents/Projects/orquestra-sdk/src/orquestra/sdk/v2/_workflow.py", line 25, in <module>
        from . import _api, _dsl, loader
    File "/Users/benjaminmummery/Documents/Projects/orquestra-sdk/src/orquestra/sdk/v2/loader.py", line 34, in <module>
        class ImportFaker(importlib.abc.MetaPathFinder):
    AttributeError: module 'importlib' has no attribute 'abc'. Did you mean: '_abc'?
    ```

    If this test is failing, we'll need to track down the file where we're using `abc`
    import it explicitly instead:

    ```python
    from importlib import abc
    ...

    and then reference `abc` rather than `importlib.abc`.

    """  # noqa E501
    command = f'{str(sys.executable)} -c "import orquestra.sdk as sdk"'
    proc = subprocess.run(command, shell=True, capture_output=True)
    assert proc.returncode == 0, proc.stderr.decode()
