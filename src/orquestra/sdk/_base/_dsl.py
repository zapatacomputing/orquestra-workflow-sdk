################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
from __future__ import annotations

import ast
import inspect
import os
import pathlib
import re
import traceback
import warnings
from collections import OrderedDict
from enum import Enum
from pathlib import Path
from string import Formatter
from tempfile import NamedTemporaryFile
from types import BuiltinFunctionType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Tuple,
    TypeVar,
    Union,
    cast,
    overload,
)

from typing_extensions import ParamSpec

if TYPE_CHECKING:
    import pip_api

import wrapt  # type: ignore

import orquestra.sdk.schema.ir as ir

from ..exceptions import DirtyGitRepo, InvalidTaskDefinitionError, WorkflowSyntaxError
from . import _ast

DIRECT_EXECUTION = False

DEFAULT_IMAGE = "zapatacomputing/orquestra-default:v0.0.0"
GPU_IMAGE = "zapatacomputing/orquestra-nvidia:v0.0.0"


# ----- SDK exceptions  -----


class NoRemoteRepo(Exception):
    pass


class NotGitRepo(Exception):
    pass


class InvalidPlaceholderInCustomTaskNameError(Exception):
    pass


class UnknownPlaceholderInCustomNameWarning(Warning):
    pass


# ----- data structures -----

# Typealias used to mark variables expected to hold raw constant values.
Constant = Any
# Typelias used to mark variables that can be used as task arguments. These are the
# graph nodes that can represent data (contrary to task invocations that represent
# function calls).
Argument = Union[Constant, "ArtifactFuture", "Secret"]


class Secret(NamedTuple):
    name: str
    # Config name is only used for the local runtimes where we can't infer the location
    # where we get a secret's value from.
    # This matches the behaviour of `sdk.secrets.get` where the config name is used to
    # get a secret when running locally.
    config_name: Optional[str] = None


class GitImportWithAuth(NamedTuple):
    """
    A task import that uses a private Git repo

    Please use a helper method, such as GithubImport, instead of this class
    """

    repo_url: str
    git_ref: str
    username: Optional[str]
    auth_secret: Optional[Secret]


class GitImport(NamedTuple):
    """A task import that uses a Git repository"""

    repo_url: str
    git_ref: str

    @staticmethod
    def infer(
        local_repo_path: Union[str, os.PathLike] = Path("."),
        git_ref: Optional[str] = None,
    ) -> "DeferredGitImport":
        """Get local Git info for a specific local repo.
           The current git repo info is retrieved as default.

        Args:
            local_repo_path - path to the local repo
            git_ref - branch/commit/tag

        Usage:
            GitImport.infer()
        """
        return DeferredGitImport(local_repo_path, git_ref)


def GithubImport(
    repo: str,
    git_ref: str = "main",
    username: Optional[str] = None,
    personal_access_token: Optional[Secret] = None,
):
    """Helper to create GitImports from Github repos"""
    return GitImportWithAuth(
        repo_url=f"https://github.com/{repo}.git",
        git_ref=git_ref,
        username=username,
        auth_secret=personal_access_token,
    )


class DeferredGitImport:
    def __init__(
        self, local_repo_path: Union[str, os.PathLike], git_ref: Optional[str] = None
    ):
        self.local_repo_path = local_repo_path
        self.git_ref = git_ref

    def resolved(self):
        """Resolve remote URL and git ref based on local git repository.

        Raises:
                NotAGitRepo: when the local directory is not a valid git repo
                NoRemoteRepo: when the remote git repo doesn't exist
                DirtyGitRep: when there is uncommitted change or unpushed commit
                in a git repo
        """
        import git

        try:
            repo = git.Repo(self.local_repo_path, search_parent_directories=True)
        except (git.InvalidGitRepositoryError, git.NoSuchPathError) as e:
            raise NotGitRepo(f"This is not git repo: {self.local_repo_path}", e)

        # Check if the remote repo exists.
        try:
            remote = repo.remote("origin")
            remote.fetch()
        except (git.GitCommandError, ValueError) as e:
            raise NoRemoteRepo(f"The remote repo {repo} doesn't exist.") from e

        # Check if there is uncommitted change or unpushed commit in a git repo.
        if repo.is_dirty():
            warnings.warn(
                "You have uncommitted changes or unpushed commits in the local repo: "
                f"{repo.working_dir}.",
                DirtyGitRepo,
            )

        repo_url = repo.remotes.origin.url.replace(
            "https://github.com/", "git@github.com:"
        )

        # Check if branch is in the DETACHED HEAD state.
        if self.git_ref is None and repo.head.is_detached:
            warnings.warn("You're working on detached HEAD.")
            self.git_ref = repo.head.object.hexsha
        if self.git_ref is None:
            self.git_ref = repo.head.ref.name

        return GitImport(repo_url=repo_url, git_ref=self.git_ref)


class PythonImports:
    """A task import that uses Python packages"""

    def __init__(
        self,
        # Required packages
        *packages: Union[str, None],
        # Path to a `requirements.txt` file
        file: Optional[str] = None,
    ):
        self.use_file_path = False
        if file is not None:
            self.file = pathlib.Path(file)
            self.use_file_path = True
        self.packages = packages

    def resolved(self) -> List[pip_api.Requirement]:
        import pip_api

        # on Windows file cannot be reopened when it's opened with delete=True
        # So the temp file is closed first and then deleted manually.
        tmp_file = NamedTemporaryFile(mode="w+", delete=False)
        if self.use_file_path:
            # gather all requirements from file
            with open(self.file) as file:
                lines = file.readlines()
            for line in lines:
                tmp_file.write(line)
        # gather all requirements passed by the user
        for package in self.packages:
            tmp_file.write(f"{package}\n")
        tmp_file.flush()
        tmp_file.close()
        # reading all requirements as one parse to avoid conflicts
        requirements = pip_api.parse_requirements(
            pathlib.Path(tmp_file.name), include_invalid=False
        )
        os.unlink(tmp_file.name)

        # with include_invalid - parsing will never return invalid_requirement type,
        # but mypy doesn't detect that, so this list comp. is to satisfy mypy
        # (and make it type-safe just-in-case
        return [
            req for req in requirements.values() if isinstance(req, pip_api.Requirement)
        ]


class LocalImport(NamedTuple):
    """Used to specify that the source code is only available locally.
    e.g. not committed to any git repo or in a Python package
    """

    module: str


class InlineImport(NamedTuple):
    """
    A task import that stores the function "inline" with the workflow definition.
    """

    pass


Import = Union[
    LocalImport,
    GitImport,
    GitImportWithAuth,
    DeferredGitImport,
    PythonImports,
    InlineImport,
]
"""Type that includes all possible task imports"""


class Resources(NamedTuple):
    """The computational resources a workflow task requires"""

    cpu: Optional[str] = None
    memory: Optional[str] = None
    disk: Optional[str] = None
    gpu: Optional[str] = None

    def is_empty(self) -> bool:
        # find out if all the Resources are None
        return all(resource_value is None for resource_value in self._asdict().values())


class DataAggregation(NamedTuple):
    """
    A class representing information for data aggregation task

    run - a boolean specifying whether data aggregation should be run (default to True)
    resources - desired resources for data aggregation step (default to:
    cpu=1 memory=2Gi disk =10Gi
    """

    run: Optional[bool] = None
    resources: Resources = Resources()


class ModuleFunctionRef(NamedTuple):
    # Required to dereference function for execution.
    module: str
    function_name: str

    # Needed by Orquestra Studio to power jump-to-definition.
    file_path: Optional[str] = None
    line_number: Optional[int] = None


class FileFunctionRef(NamedTuple):
    # Required to dereference function for execution.
    file_path: str
    function_name: str

    # Needed by Orquestra Studio to power jump-to-definition. `file_path` can
    # be used for both execution and jump-to-definition.
    line_number: Optional[int] = None


class InlineFunctionRef(NamedTuple):
    function_name: str
    fn: Callable


FunctionRef = Union[ModuleFunctionRef, FileFunctionRef, InlineFunctionRef]


class ParameterKind(Enum):
    # Currently this uses the same naming as Python.
    # We only use a subset of Python's kinds, however.
    POSITIONAL_OR_KEYWORD = "POSITIONAL_OR_KEYWORD"
    VAR_POSITIONAL = "VAR_POSITIONAL"
    VAR_KEYWORD = "VAR_KEYWORD"


class TaskParameter(NamedTuple):
    name: str
    kind: ParameterKind


class TaskOutputMetadata(NamedTuple):
    is_subscriptable: bool
    n_outputs: int


def _get_placeholders_from_string(input_string: str) -> List[str]:
    return [i[1] for i in Formatter().parse(input_string) if i[1] is not None]


def parse_custom_name(
    custom_name: Optional[str], signature: inspect.BoundArguments
) -> Optional[str]:
    if custom_name is None:
        return None

    signature.apply_defaults()
    placeholders = _get_placeholders_from_string(custom_name)

    # check if all placeholders are in the arguments of the function
    # We do this by checking all unique placeholders have a corresponding
    # argument in the function's signature
    if diff := set(placeholders) - set(signature.arguments):
        raise InvalidPlaceholderInCustomTaskNameError(
            f"Custom name contains placeholders that" f" aren't parameters: {diff}"
        )
    # replacement string used when value of wildcard is unknown during submit
    replacement_string = "{0}"
    format_dict = {}
    for ph in placeholders:
        if isinstance(signature.arguments[ph], ArtifactFuture):
            fnc = signature.arguments[ph].invocation.task.fn_ref.function_name
            format_dict[ph] = replacement_string.format(fnc)
            warnings.warn(
                "Custom name contains placeholder with value"
                f" unknown during submission time. Placeholder's value: {ph}"
                f" will be replaced by {replacement_string.format(fnc)}",
                category=UnknownPlaceholderInCustomNameWarning,
            )
        else:
            format_dict[ph] = signature.arguments[ph]
    return custom_name.format(**format_dict)


_P = ParamSpec("_P")
_R = TypeVar("_R")


class TaskDef(Generic[_P, _R], wrapt.ObjectProxy):
    """A function exposed to Orquestra. This is the result of applying the @task
    decorator.

    We do some magic to transform Python code in the workflow function decorated
    function to build the computational workflow graph. Use the `.model` property to
    get the serializable form.
    """

    def __init__(
        self,
        fn: Callable[_P, _R],
        fn_ref: FunctionRef,
        source_import: Import,
        output_metadata: TaskOutputMetadata,
        parameters: Optional[OrderedDict] = None,
        dependency_imports: Optional[Tuple[Import, ...]] = None,
        resources: Resources = Resources(),
        custom_image: Optional[str] = None,
        custom_name: Optional[str] = None,
    ):
        if isinstance(fn, BuiltinFunctionType):
            raise NotImplementedError("Built-in functions are not supported as Tasks")
        super(TaskDef, self).__init__(fn)
        self.__sdk_task_body = fn
        self.fn_ref = fn_ref
        self.source_import = source_import
        self.output_metadata = output_metadata
        self.parameters = parameters
        self.dependency_imports = dependency_imports
        self.resources = resources
        self.custom_image = custom_image
        self.custom_name = custom_name

        if self.custom_image is None:
            if resources.gpu:
                self.custom_image = GPU_IMAGE
            else:
                self.custom_image = DEFAULT_IMAGE

    @property
    def n_outputs(self):
        warnings.warn(
            '"n_outputs" is deprecated. Please use "output_metadata".',
            DeprecationWarning,
        )
        return self.output_metadata.n_outputs

    def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> _R:
        # In case of local run the workflow is executed as a python script
        if DIRECT_EXECUTION:
            return self.__sdk_task_body(*args, **kwargs)
        if (
            not isinstance(self.source_import, InlineImport)
            and isinstance(self.fn_ref, ModuleFunctionRef)
            and self.fn_ref.module == "__main__"
        ):
            err = (
                f"function {self.fn_ref.function_name} is defined inside __main__ "
                "module. Please move task function to different file and import, "
                "it or mark this task function as inline import \n\n"
                "example: \n"
                "@sdk.task(source_import=sdk.InlineImport())\n"
                f"def {self.fn_ref.function_name}(): ..."
            )
            raise InvalidTaskDefinitionError(err)
        try:
            signature = inspect.signature(self.__sdk_task_body).bind(*args, **kwargs)
        except TypeError as exc:
            # Check if an error is generated when the args and kwargs of the task call
            # are bonded to the args and kwargs of the task function.
            summary = traceback.StackSummary.extract(
                traceback.walk_stack(None)
            ).format()[1]
            error_message = (
                f"Error message: {exc}\nThe following assignment could not"
                f" be performed:\n {summary}"
            )
            # Check if the error message informs about not passing a value
            # for the `self` argument. If the `self` argument is missing tells the
            # user to not use methods as task functions.
            missing_self_arg = r".*(missing a required argument:)[^a-zA-Z\d]*(self).*"
            if re.match(missing_self_arg, str(exc)):
                error_message = (
                    f"The task {self.fn_ref.function_name} seems to be a method, if so"
                    " modify it to not be a method.\n"
                ) + error_message
            raise WorkflowSyntaxError(error_message) from exc

        return cast(
            _R,
            ArtifactFuture(
                TaskInvocation(
                    self,
                    args=args,
                    kwargs=tuple(kwargs.items()),
                    resources=self.resources,
                    custom_name=parse_custom_name(self.custom_name, signature),
                    custom_image=self.custom_image,
                )
            ),
        )

    @property
    def model(self) -> ir.TaskDef:
        """Serializable form of the task def (intermediate representation).

        returns:
            Pydantic model.
        """
        # hack for circular imports
        from orquestra.sdk._base import _traversal

        return _traversal.get_model_from_task_def(self)

    @property
    def import_models(self) -> List[ir.Import]:
        """All Orquestra Imports required by this task, in a serializable form.

        returns:
            Pydantic models.
        """
        # hack for circular imports
        from orquestra.sdk._base import _traversal

        return _traversal.get_model_imports_from_task_def(self)


# TaskInvocation is using a Plain Old Python Object on purpose:
# Using POPO instead of a NamedTuple means each instance of TaskInvocation
# is unique, even if they share the exact same attributes. This is required
# for the traversal of the graph.
class TaskInvocation:
    task: TaskDef

    args: Tuple[Argument, ...]

    # NOTE: Some time ago we needed this to be hashable, hence we went with a tuple of
    # key-value pairs instead of a mapping. It can be changed to a dict.
    kwargs: Tuple[Tuple[str, Argument], ...]

    type: str = "task_invocation"

    # invocation metadata below
    resources: Resources = Resources()
    custom_image: Optional[str] = None

    def __init__(
        self,
        task: TaskDef,
        args: Tuple[Argument, ...],
        kwargs: Tuple[Tuple[str, Argument], ...],
        type: str = "task_invocation",
        resources: Resources = Resources(),
        custom_name: Optional[str] = None,
        custom_image: Optional[str] = None,
    ):
        self.task = task
        self.args = args
        self.kwargs = kwargs
        self.resources = resources
        self.type = type
        self.custom_name = custom_name
        self.custom_image = custom_image

    def _asdict(self) -> Dict[str, Any]:
        return {
            "task": self.task,
            "args": self.args,
            "kwargs": self.kwargs,
            "type": self.type,
            "resources": self.resources,
            "custom_name": self.custom_name,
            "custom_image": self.custom_image,
        }


class Sentinel(Enum):
    NO_UPDATE = object()


class ArtifactFormat(Enum):
    # At the moment, we infer artifact serialization format from the object type at run
    # time. See orquestra.sdk._base for details.
    #
    # Values are uppercase here to keep easy interoperability with the workflow json
    # schema.
    # See: orquestra.sdk.schema.ir.ArtifactFormat
    #
    # Add more cases here when we allow users to specify artifact format explicitly.
    # This has to be accompanied by corresponding implementation in orquestra.sdk._base.
    AUTO = "AUTO"


class ArtifactFuture:
    DEFAULT_CUSTOM_NAME = None
    DEFAULT_SERIALIZATION_FORMAT = ArtifactFormat.AUTO

    def __init__(
        self,
        invocation: TaskInvocation,
        output_index: Optional[int] = None,
        custom_name: Optional[str] = DEFAULT_CUSTOM_NAME,
        serialization_format: ArtifactFormat = DEFAULT_SERIALIZATION_FORMAT,
    ):
        self.invocation = invocation
        # if the invocation returns multiple values, this the index in the output
        # sequence
        self.output_index = output_index
        # metadata below
        self.custom_name = custom_name
        self.serialization_format = serialization_format

    def __repr__(self):
        return (
            "ArtifactFuture("
            f"invocation={self.invocation}, "
            f"output_index={self.output_index}, "
            f"custom_name={self.custom_name}, "
            f"serialization_format={self.serialization_format}"
            ")"
        )

    def __getitem__(self, index):
        if not self.invocation.task.output_metadata.is_subscriptable:
            raise TypeError("This ArtifactFuture is not subscriptable")
        if not isinstance(index, int):
            raise TypeError("ArtifactFuture indices must be integers")
        if index >= self.invocation.task.output_metadata.n_outputs:
            raise IndexError("ArtifactFuture index out of range")
        return ArtifactFuture(
            invocation=self.invocation,
            output_index=index,
            custom_name=self.custom_name,
            serialization_format=self.serialization_format,
        )

    def __iter__(self):
        if not self.invocation.task.output_metadata.is_subscriptable:
            raise TypeError("This ArtifactFuture is not iterable")
        futures = [
            ArtifactFuture(
                invocation=self.invocation,
                output_index=next_index,
                custom_name=self.custom_name,
                serialization_format=self.serialization_format,
            )
            for next_index in range(self.invocation.task.output_metadata.n_outputs)
        ]
        return iter(futures)

    def __reduce_ex__(self, protocol):
        """
        Handles Pickling for ArtifactFuture

        We currently do not support pickling ArtifactFutures and instead have a
        different format for passing artifacts between tasks.

        The cases where ArtifactFutures can be pickled include when an artifact is
        inside a container (for example `dict()` or `list()`) and we currently do not
        support this pattern.

        Raising an exception here allows us to catch unsupported attempts at pickling
        and let the user know before execution time.
        """
        raise NotImplementedError("ArtifactFuture cannot be pickled")

    def with_invocation_meta(
        self,
        *,
        cpu: Optional[Union[str, Sentinel]] = Sentinel.NO_UPDATE,
        memory: Optional[Union[str, Sentinel]] = Sentinel.NO_UPDATE,
        disk: Optional[Union[str, Sentinel]] = Sentinel.NO_UPDATE,
        gpu: Optional[Union[str, Sentinel]] = Sentinel.NO_UPDATE,
        custom_image: Optional[Union[str, Sentinel]] = Sentinel.NO_UPDATE,
    ) -> "ArtifactFuture":
        """Assigns optional metadata related to task invocation used to generate this
        artifact.

        Doesn't modify existing invocations, returns a new one.

        Example usage:
            text = capitalize("hello").with_invocation_meta(
                cpu="1000m",custom_image="zapatacomputing/orquestra-qml:v0.1.0-cuda"
                )

        Args:
            cpu: amount of cpu assigned to the task invocation
            memory: amount of memory assigned to the task invocation
            disk: amount of disk assigned to the task invocation
            gpu: amount of gpu assigned to the task invocation
            custom_image: docker image used to run the task invocation
        """
        self._check_if_destructured(
            fn_name=self.invocation.task.fn_ref.function_name,
            assign_type="invocation metadata",
        )

        # Only use the new properties if they have not been changed.
        # None is a valid option, so we are using the Sentinel object pattern:
        # https://python-patterns.guide/python/sentinel-object/

        invocation = self.invocation

        resources = invocation.resources
        new_resources = Resources(
            cpu=resources.cpu if cpu is Sentinel.NO_UPDATE else cpu,
            gpu=resources.gpu if gpu is Sentinel.NO_UPDATE else gpu,
            memory=resources.memory if memory is Sentinel.NO_UPDATE else memory,
            disk=resources.disk if disk is Sentinel.NO_UPDATE else disk,
        )

        new_custom_image: Optional[str]

        if custom_image is not Sentinel.NO_UPDATE:
            new_custom_image = custom_image
        else:
            new_custom_image = invocation.custom_image

        new_invocation = TaskInvocation(
            task=invocation.task,
            args=invocation.args,
            kwargs=invocation.kwargs,
            resources=new_resources,
            custom_name=invocation.custom_name,
            custom_image=new_custom_image,
        )

        return ArtifactFuture(
            invocation=new_invocation,
            output_index=self.output_index,
            custom_name=self.custom_name,
            serialization_format=self.serialization_format,
        )

    def with_resources(
        self,
        *,
        cpu: Optional[Union[str, Sentinel]] = Sentinel.NO_UPDATE,
        memory: Optional[Union[str, Sentinel]] = Sentinel.NO_UPDATE,
        disk: Optional[Union[str, Sentinel]] = Sentinel.NO_UPDATE,
        gpu: Optional[Union[str, Sentinel]] = Sentinel.NO_UPDATE,
    ) -> "ArtifactFuture":
        """Assigns optional metadata related to task invocation used to generate this
        artifact.

        Doesn't modify existing invocations, returns a new one.

        Example usage:
            text = capitalize("hello").with_resources(cpu="1000m")

        Args:
            cpu: amount of cpu assigned to the task invocation
            memory: amount of memory assigned to the task invocation
            disk: amount of disk assigned to the task invocation
            gpu: amount of gpu assigned to the task invocation
        """
        self._check_if_destructured(
            fn_name=self.invocation.task.fn_ref.function_name,
            assign_type="resources",
        )

        return self.with_invocation_meta(cpu=cpu, memory=memory, disk=disk, gpu=gpu)

    def with_custom_image(
        self,
        custom_image: Optional[Union[str, Sentinel]] = Sentinel.NO_UPDATE,
    ) -> "ArtifactFuture":
        """Assigns optional metadata related to task invocation used to generate this
        artifact.

        Doesn't modify existing invocations, returns a new one.

        Example usage:
            text = capitalize("hello").with_custom_image(
                "zapatacomputing/orquestra-qml:v0.1.0-cuda"
                )

        Args:
            custom_image: docker image used to run the task invocation
        """
        self._check_if_destructured(
            fn_name=self.invocation.task.fn_ref.function_name,
            assign_type="custom image",
        )

        return self.with_invocation_meta(custom_image=custom_image)

    def _check_if_destructured(self, fn_name: str, assign_type: str):
        """
        Check if an ArtifactFuture has been destructured and raise a
        WorkflowSyntaxError with the appropriate error message if so
        """
        if self.output_index is not None:
            summary = traceback.StackSummary.extract(
                traceback.walk_stack(None)
            ).format()[2]
            if assign_type == "custom image":
                assign_type = "a " + assign_type
            raise WorkflowSyntaxError(
                f"Can't assign {assign_type} to an artifact that"
                " has been destructured.\n"
                f"To assign {assign_type} to a call of the task "
                f"{fn_name}"
                " make sure NOT to destructure its outputs. "
                f"The following assignment could not be performed:\n {summary}"
            )


# ----- decorators -----


def _resolve_module_path(fn):
    if (abs_path := inspect.getsourcefile(fn)) is None:
        # This can happen when the `fn` is defined in a binary module. We need to check
        # it explicitly to appease mypy. It would be nice to have a test case for it.
        return None
    try:
        relative_path = os.path.relpath(abs_path)
    except ValueError:
        relative_path = os.path.abspath(abs_path)
    return pathlib.Path(relative_path).as_posix()


def _get_fn_line_number(fn):
    line_number: Optional[int]
    try:
        line_number = inspect.getsourcelines(fn)[1]
    except OSError:
        line_number = None
    return line_number


def _is_interactive():
    # This seems to be a "good" way of checking if this code is being run in an
    # interactive session, i.e. Jupyter notebook, interactive REPL, etc.
    import __main__ as main

    return not hasattr(main, "__file__")


def get_fn_ref(fn) -> FunctionRef:
    return ModuleFunctionRef(
        module=fn.__module__,
        function_name=fn.__name__,
        file_path=_resolve_module_path(fn),
        line_number=_get_fn_line_number(fn),
    )


def _parameter_kind_from_inspect(kind: inspect._ParameterKind) -> ParameterKind:
    if kind == inspect._ParameterKind.VAR_POSITIONAL:
        return ParameterKind.VAR_POSITIONAL
    elif kind == inspect._ParameterKind.VAR_KEYWORD:
        return ParameterKind.VAR_KEYWORD
    else:
        return ParameterKind.POSITIONAL_OR_KEYWORD


def _get_parameters(fn: Callable) -> OrderedDict:
    sig = inspect.signature(fn)
    # A dict is used in order to easily reference Parameters by name
    # However, parameters are strictly ordered: An OrderedDict must be used
    parameters = OrderedDict()
    for param in sig.parameters.values():
        task_parameter = TaskParameter(
            name=param.name, kind=_parameter_kind_from_inspect(param.kind)
        )
        parameters[param.name] = task_parameter
    return parameters


def _get_number_of_outputs(fn: Callable) -> TaskOutputMetadata:
    try:
        source = inspect.getsource(fn)
    except OSError:
        # Unable to find source, assume 1 output
        return TaskOutputMetadata(is_subscriptable=False, n_outputs=1)
    fn_body = ast.parse(_ast.normalize_indents(source))
    visitor = _ast.OutputCounterVisitor()
    visitor.visit(fn_body)

    n_different_returns = len(visitor.outputs)
    if n_different_returns == 0:
        # No outputs detected - probably a void function with not return statement
        return TaskOutputMetadata(is_subscriptable=False, n_outputs=1)
    elif n_different_returns == 1:
        (ast_outputs,) = visitor.outputs
        return TaskOutputMetadata(
            is_subscriptable=ast_outputs.is_subscriptable,
            n_outputs=ast_outputs.n_outputs,
        )
    else:
        warnings.warn(
            "Complex function detected, falling back to a single output. "
            f"Hypotheses: {visitor.outputs}"
        )
        return TaskOutputMetadata(is_subscriptable=False, n_outputs=1)


@overload
def task(fn: Callable[_P, _R]) -> TaskDef[_P, _R]:
    ...


@overload
def task(
    *,
    source_import: Optional[Import] = None,
    dependency_imports: Optional[Iterable[Import]] = None,
    resources: Resources = Resources(),
    n_outputs: Optional[int] = None,
    custom_image: Optional[str] = None,
    custom_name: Optional[str] = None,
) -> Callable[[Callable[_P, _R]], TaskDef[_P, _R]]:
    ...


@overload
def task(
    fn: Callable[_P, _R],
    *,
    source_import: Optional[Import] = None,
    dependency_imports: Optional[Iterable[Import]] = None,
    resources: Resources = Resources(),
    n_outputs: Optional[int] = None,
    custom_image: Optional[str] = None,
    custom_name: Optional[str] = None,
) -> TaskDef[_P, _R]:
    ...


def task(
    fn: Optional[Callable[_P, _R]] = None,
    *,
    source_import: Optional[Import] = None,
    dependency_imports: Optional[Iterable[Import]] = None,
    resources: Resources = Resources(),
    n_outputs: Optional[int] = None,
    custom_image: Optional[str] = None,
    custom_name: Optional[str] = None,
) -> Union[TaskDef[_P, _R], Callable[[Callable[_P, _R]], TaskDef[_P, _R]]]:
    """Wraps a function into an SDK Task.

    Args:
        fn: A function definition to expose to Orquestra.
        source_import: Tells Orquestra what git repo to clone to run this task
            Only matters when running workflows remotely (on Quantum Engine).
        dependency_imports: Tells Orquestra what other git repos need to be
            cloned and installed before running this task. Use it only when
            your dependencies aren't installable from PyPI. Only matters when
            running workflows remotely (on Quantum Engine).
        resources: hints Orquestra what computational resources are required to
            run this task. Only matters when running workflows remotely (on Quantum
            Engine).
        n_outputs: tells Orquestra how many outputs this task produces. If omitted,
            the SDK magically infers this information from the task function's source
            code by analyzing the Abstract Syntax Tree (AST).
        custom_image: tell the runtime to run this task in a docker container
            preloaded with a custom docker image. If the runtime doesn't support
            it, this field is ignored. Currently, this field only has effect
            when running the workflow using QERuntime.
        custom_name: changes name for invocation of this task. Supports python
            formatting in brackets {} using task parameters. Currently, supports only
            values known at submit time. If parameter is unknown at submit time (e.g.
            result of other task) - it will be placeholded. Due to the QE limitations
            every char that is non-alphanumeric will be changed to dash ("-").
            Also only first 128 characters of the name will be used
    """
    task_dependency_imports: Optional[Tuple[Import, ...]] = (
        tuple(dependency_imports) if dependency_imports else None
    )

    if n_outputs is not None:
        if n_outputs <= 0:
            raise ValueError("A task should have at least one output")

    def _inner(fn: Callable[_P, _R]):
        # Assume if a user has specified the number of outputs, then this output is
        # subscriptable
        output_metadata: TaskOutputMetadata
        if n_outputs is not None:
            output_metadata = TaskOutputMetadata(
                is_subscriptable=True, n_outputs=n_outputs
            )
        else:
            output_metadata = _get_number_of_outputs(fn)

        # Set the default Import based on if the session is interactive
        task_source_import: Import
        if source_import is not None:
            task_source_import = source_import
        elif _is_interactive():
            task_source_import = InlineImport()
        else:
            task_source_import = LocalImport(module=fn.__module__)

        # This is a little awkward
        # but to avoid complexity in get_fn_ref, I kept the complexity here.
        fn_ref = (
            InlineFunctionRef(fn.__name__, fn)
            if isinstance(task_source_import, InlineImport)
            else get_fn_ref(fn)
        )

        task_def = TaskDef(
            fn=fn,
            fn_ref=fn_ref,
            source_import=task_source_import,
            dependency_imports=task_dependency_imports,
            resources=resources,
            parameters=_get_parameters(fn),
            output_metadata=output_metadata,
            custom_image=custom_image,
            custom_name=custom_name,
        )

        return task_def

    if fn is None:
        return _inner
    else:
        return _inner(fn)
