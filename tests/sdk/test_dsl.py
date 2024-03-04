################################################################################
# © Copyright 2021-2023 Zapata Computing Inc.
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
from orquestra.sdk import exceptions
from orquestra.sdk._base import _dsl, loader
from orquestra.sdk._base.serde import deserialize_pickle, serialize_pickle

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
                        new_future.invocation.resources == future.invocation._resources
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
                assert new_future.invocation.resources == future.invocation._resources
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
        assert _local_task.n_outputs == _local_task._output_metadata.n_outputs == 2


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
        custom_image="zapatacomputing/custom_image_not_real",
    )
    def _local_task():
        return "hello"

    hello = _local_task()
    assert (
        hello.invocation.task._custom_image == "zapatacomputing/custom_image_not_real"
    )


def test_task_with_default_image():
    @sdk.task()
    def _local_task():
        return "hello"

    hello = _local_task()
    assert hello.invocation.task._custom_image is None


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
    with pytest.warns(exceptions.DirtyGitRepo):
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
        ("emiliano_zapata", _dsl.Secret("my_secret", workspace_id="ws")),
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


def test_warns_when_no_workspace_provided():
    with pytest.warns(FutureWarning):
        _dsl.GithubImport(
            "zapatacomputing/orquestra-workflow-sdk",
            "main",
            username="UN",
            personal_access_token=sdk.Secret("MY PAT"),
        )


class TestGithubImportRaisesTypeErrorForNonSecretPAT:
    @staticmethod
    def test_str_pat():
        # note: we disable mypy checking here as we're explicitly looking for how a
        # case where we pass an argument with the wrong type is handled.
        with pytest.raises(TypeError) as e:
            _ = _dsl.GithubImport(
                "zapatacomputing/orquestra-workflow-sdk",
                "main",
                username="foo",
                personal_access_token="bar",  # type: ignore
            )
        assert (
            e.exconly()
            == """TypeError: You passed a string as `personal_access_token = "..."`. Please pass `personal_access_token = sdk.Secret(name="...")` instead. It might seem verbose, but it's a precaution against committing plain-text credentials to your git repo, or leaking secret values as part of the workflow definition.
Suggested fix:
  personal_access_token = sdk.Secret(name="paste secret name here")"""  # noqa: E501
        )

    @staticmethod
    @pytest.mark.parametrize("pat, pat_type", [(0, "int"), (0.1, "float")])
    def test_non_secret_pat(pat, pat_type):
        with pytest.raises(TypeError) as e:
            _ = _dsl.GithubImport(
                "zapatacomputing/orquestra-workflow-sdk",
                "main",
                username="foo",
                personal_access_token=pat,
            )
        assert (
            e.exconly()
            == f"TypeError: `personal_access_token` must be of type `sdk.Secret`, not {pat_type}."  # noqa: E501
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


def test_default_import_type(monkeypatch):
    @_dsl.task
    def task():
        ...

    @_dsl.task(source_import=_dsl.InlineImport())
    def inline_task():
        ...

    # type comparison to check if in interactive mode default is INLINE
    assert type(task._source_import) is type(inline_task._source_import)  # noqa


@pytest.mark.parametrize(
    "python_imports, expected_req, raises",
    [
        (sdk.PythonImports(file="Nope"), "", pytest.raises(FileNotFoundError)),
        (
            sdk.PythonImports(file="tests/sdk/data/bad_requirements.txt"),
            "",
            pytest.raises(pip_api.exceptions.PipError),
        ),
        (
            sdk.PythonImports(file="tests/sdk/data/requirements.txt"),
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
            sdk.PythonImports("joblib==3.1.0", file="tests/sdk/data/requirements.txt"),
            "",
            pytest.raises(pip_api.exceptions.PipError),
        ),
        # merge manually passed requirements with those in file
        (
            sdk.PythonImports("scipy==1.7.3", file="tests/sdk/data/requirements.txt"),
            ["joblib==1.2.0", "numpy==1.22.0", "scipy==1.7.3"],
            do_not_raise(),
        ),
        (
            sdk.PythonImports(file="tests/sdk/data/requirements_with_extras.txt"),
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


class TestRefToMain:
    @pytest.mark.parametrize(
        "workflow_defs_file, raises",
        [
            (
                "workflow_defs.py",
                pytest.raises(exceptions.InvalidTaskDefinitionError),
            ),
            ("workflow_defs_no_raise.py", do_not_raise()),
        ],
    )
    def test_ref_to_main_in_task(self, workflow_defs_file, raises):
        path_to_workflows = (
            os.path.dirname(os.path.abspath(__file__)) + "/data/sample_project/"
        )
        # add path to locate helper imports
        sys.path.append(path_to_workflows)
        # prepare file to be executed as __main__
        loader = importlib.machinery.SourceFileLoader(
            "__main__", path_to_workflows + workflow_defs_file
        )
        spec = importlib.util.spec_from_loader(loader.name, loader)
        assert spec is not None
        mod = importlib.util.module_from_spec(spec)

        with raises:
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


@pytest.mark.parametrize(
    "obj",
    [
        sdk.GitImport(
            git_ref="https://github.com/zapatacomputing/orquestra-workflow-sdk.git",
            repo_url="main",
        ),
        sdk.GithubImport("zapatacomputing/orquestra-workflow-sdk"),
        sdk.PythonImports("numpy"),
        sdk.LocalImport("module"),
        sdk.InlineImport(),
    ],
)
def test_dsl_imports_not_iterable(obj):
    with pytest.raises(TypeError):
        [a for a in obj]


@pytest.mark.parametrize(
    "dependency_imports, expected_imports",
    [
        (None, None),
        (sdk.InlineImport(), (sdk.InlineImport(),)),
        (sdk.LocalImport("mod"), (sdk.LocalImport("mod"),)),
        (
            sdk.GitImport(repo_url="abc", git_ref="xyz"),
            (sdk.GitImport(repo_url="abc", git_ref="xyz"),),
        ),
        (
            sdk.GithubImport("abc"),
            (sdk.GithubImport("abc"),),
        ),
        (
            sdk.PythonImports("abc"),
            (sdk.PythonImports("abc"),),
        ),
    ],
)
def test_dependency_imports(dependency_imports, expected_imports):
    @sdk.task(dependency_imports=dependency_imports)
    def my_task():
        pass

    assert my_task._dependency_imports == expected_imports


class TestResources:
    @pytest.mark.parametrize(
        "cpu", ["1001m", "1500m", "1.5", "2001m", "1.0001k", "1500000u"]
    )
    def test_invalud_cpu_resources(self, cpu):
        @sdk.task(resources=sdk.Resources(cpu=cpu))
        def t():
            ...

        @sdk.workflow
        def wf():
            return t()

        with pytest.raises(exceptions.InvalidTaskDefinitionError):
            wf().model

    @pytest.mark.parametrize("cpu", ["1000m", "500m", "1.0", "3.0", "1", "1k"])
    def test_valid_cpu_resources(self, cpu):
        @sdk.task(resources=sdk.Resources(cpu=cpu))
        def t():
            ...

        @sdk.workflow
        def wf():
            return t()

        # should not raise
        wf().model

    @pytest.mark.parametrize("gpu", ["1", "1.0", "10.0"])
    def test_valid_gpu_resources(self, gpu):
        @sdk.task(resources=sdk.Resources(gpu=gpu))
        def t():
            ...

        @sdk.workflow
        def wf():
            return t()

        # should not raise
        wf().model


def test_secret_pickles():
    secret = sdk.Secret("name", config_name="cfg", workspace_id="workspace")

    pkl = serialize_pickle(secret)
    de_pkl = deserialize_pickle(pkl)

    assert secret == de_pkl


class TestSecretAsString:
    def test_basic_secret_as_string_usage(self):
        @sdk.workflow()
        def wf():
            my_secret = sdk.secrets.get("w/e", workspace_id="w/e")
            my_secret.split()

        with pytest.raises(AttributeError) as e:
            wf().model

        assert "Invalid usage of a Secret object" in str(e)

    def test_secret_subscribe_as_string(self):
        @sdk.workflow()
        def wf():
            sdk.secrets.get("w/e", workspace_id="w/e")[0]

        with pytest.raises(AttributeError) as e:
            wf().model

        assert "Invalid usage of a Secret object" in str(e)

    def test_secret_translated_to_string(self):
        @sdk.workflow()
        def wf():
            my_secret = sdk.secrets.get("w/e", workspace_id="w/e")
            print(my_secret)

        with pytest.raises(AttributeError) as e:
            wf().model

        assert "Invalid usage of a Secret object" in str(e)

    def test_secret_as_iterable(self):
        @sdk.workflow()
        def wf():
            my_secrets = sdk.secrets.get("w/e", workspace_id="w/e")
            for letter in my_secrets:
                print(letter)

        with pytest.raises(AttributeError) as e:
            wf().model

        assert "Invalid usage of a Secret object" in str(e)
