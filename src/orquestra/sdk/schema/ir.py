################################################################################
# © Copyright 2021-2022 Zapata Computing Inc.
################################################################################
"""Workflow Intermediate Representation.

The classes here are used only for purposes of schema definition. Every data
structure here is JSON-serializable.
"""

import enum
import typing as t

import pydantic
from pydantic import BaseModel

ImportId = str
SecretNodeId = str


class SecretNode(BaseModel):
    """
    A reference to a secret stored in an external secret/config service.
    """

    # Workflow-scope unique ID used to refer from task invocations
    id: SecretNodeId

    # Serialized value
    secret_name: str

    # Secret config
    # This is only used locally, and we expect this to be None (or ignored) on remote
    # runtimes.
    secret_config: t.Optional[str] = None


class GitURL(BaseModel):
    original_url: str
    protocol: str
    user: t.Optional[str]
    password: t.Optional[SecretNode]
    host: str
    port: t.Optional[int]
    path: str
    query: t.Optional[str]


class GitImport(BaseModel):
    id: ImportId
    repo_url: GitURL
    git_ref: str

    # we need this in the JSON to know which class to use when deserializing
    type: str = pydantic.Field("GIT_IMPORT", const=True)

    @pydantic.validator("repo_url", pre=True)
    def _backwards_compatible_repo_url(cls, v):
        """Allows older models with a string URL to be imported"""
        # Prevent circular imports
        from .._base._git_url_utils import parse_git_url

        if not isinstance(v, str):
            return v

        return parse_git_url(v)


class LocalImport(BaseModel):
    """Used to specify that the source code is only available locally (e.g. not
    committed to any git repo)
    """

    id: ImportId

    # we need this in the JSON to know which class to use when deserializing
    type: str = pydantic.Field("LOCAL_IMPORT", const=True)


class InlineImport(BaseModel):
    id: ImportId
    type: str = pydantic.Field("INLINE_IMPORT", const=True)


class PackageSpec(BaseModel):
    """Representation of single package import

    The fields in this class are based on:
    https://packaging.pypa.io/en/latest/requirements.html#packaging.requirements.Requirement   # noqa

    Look there for more information about valid string values.
    """

    name: str
    extras: t.List[str]
    version_constraints: t.List[str]
    environment_markers: str


class PythonImports(BaseModel):
    """List of imports for given task"""

    id: ImportId

    # List of pip packages
    packages: t.List[PackageSpec]
    # List of pip options to put at start of the requirements
    pip_options: t.List[str]

    type: str = pydantic.Field("PYTHON_IMPORT", const=True)


# If we need more import types, add them here.
Import = t.Union[GitImport, LocalImport, PythonImports, InlineImport]


TaskDefId = str


class ModuleFunctionRef(BaseModel):
    # Required to dereference function for execution.
    module: str
    function_name: str

    # Needed by Orquestra Studio to power jump-to-definition.
    file_path: t.Optional[str] = None
    line_number: t.Optional[int] = None

    # We need this in the JSON to know which class to use when deserializing
    type: str = pydantic.Field("MODULE_FUNCTION_REF", const=True)


class FileFunctionRef(BaseModel):
    # Required to dereference function for execution.
    file_path: str
    function_name: str

    # Needed by Orquestra Studio to power jump-to-definition. `file_path` can
    # be used for both execution and jump-to-definition.
    line_number: t.Optional[int] = None

    # We need this in the JSON to know which class to use when deserializing
    type: str = pydantic.Field("FILE_FUNCTION_REF", const=True)


class InlineFunctionRef(BaseModel):
    function_name: str
    # Required to dereference function for execution. The function object is serialized
    # using `dill`, base64-encoded, and chunked to workaround JSON string length limits.
    encoded_function: t.List[str]

    # We need this in the JSON to know which class to use when deserializing
    type: str = pydantic.Field("INLINE_FUNCTION_REF", const=True)


FunctionRef = t.Union[ModuleFunctionRef, FileFunctionRef, InlineFunctionRef]


class Resources(BaseModel):
    cpu: t.Optional[str] = None
    memory: t.Optional[str] = None
    disk: t.Optional[str] = None
    gpu: t.Optional[str] = None


class DataAggregation(BaseModel):
    run: t.Optional[bool] = None
    resources: t.Optional[Resources] = None


ParameterName = str


class ParameterKind(str, enum.Enum):
    # Currently this uses the same naming as Python.
    # We only use a subset of Python's kinds, however.
    # See: https://docs.python.org/3/library/inspect.html#inspect.Parameter.kind
    POSITIONAL_OR_KEYWORD = "POSITIONAL_OR_KEYWORD"
    VAR_POSITIONAL = "VAR_POSITIONAL"
    VAR_KEYWORD = "VAR_KEYWORD"


class TaskParameter(BaseModel):
    name: ParameterName
    kind: ParameterKind
    # If we need more metadata related to parameters, like type hints or default values,
    # it should be added here.


class TaskDef(BaseModel):
    # workflow-unique ID used to refer from task invocations
    id: TaskDefId

    fn_ref: FunctionRef

    # `source_import_id` & `dependency_import_ids` are references to imports defined in
    # Workflow.imports.

    # List of abstract inputs that the task requires, but without the values yet.
    # Kinda like function signature.
    # None means we do not know the parameters for this Task (e.g. an external task)
    # An empty list [] means a Task with no parameters
    parameters: t.Optional[t.List[TaskParameter]]

    # ID of the import that contains the callable function
    source_import_id: ImportId

    # IDs of the imports that are needed at runtime to allow running the callable
    dependency_import_ids: t.Optional[t.List[ImportId]] = None

    resources: t.Optional[Resources] = None

    # Hints the runtime to run this task in a docker container with this image. Has no
    # effect if the runtime doesn't support it.
    custom_image: t.Optional[str] = None


class ArtifactFormat(str, enum.Enum):
    """List of formats we allow for serializing constants & artifacts."""

    # Try to figure out best serialization format based on object type at run time.
    AUTO = "AUTO"

    # Pickle -> Base64 string
    ENCODED_PICKLE = "ENCODED_PICKLE"

    # The artifact is serialized to a JSON string. Artifact value must be
    # JSON-serializable.
    JSON = "JSON"

    # Unsupported yet. This is for demonstration purposes only at the moment.
    NUMPY_ARRAY = "NUMPY_ARRAY"


TaskInvocationId = str
TaskNodeId = str
ArtifactNodeId = str
ConstantNodeId = str


class ArtifactNode(BaseModel):
    # Workflow-scope unique ID used to refer from task invocations. If the task has
    # multiple outputs they will have distinct `id`s.
    id: ArtifactNodeId

    # artifact metadata below

    # Optional name that can be used if human-readable strings are needed. If present,
    # it's an addition to `id`, doesn't replace it.
    custom_name: t.Optional[str] = None

    # Tells runtime how to serialize artifact value after task invocation returns it.
    # At the moment it's unknown where exactly the artifacts will be persisted, but some
    # serialization will likely be needed somewhere.
    serialization_format: ArtifactFormat = ArtifactFormat.AUTO

    # Index of the variable when destructuring task result in the workflow function. If
    # the workflow contained `foo, bar = my_task()`, `foo`'s index is 0 and `bar`'s
    # index is 1. This is used by some runtimes to extract the artifact value from the
    # output tuple.
    #
    # `None` if the task result isn't destructured in the workflow function.
    artifact_index: t.Optional[int] = None


class ConstantNodeJSON(BaseModel):
    """Piece of data that already exists at workflow submission time.

    The value is directly embedded in the workflow. To support arbitrary data shapes we
    keep the value in a serialized form. This Node contains only JSON-serializable
    values.
    """

    # Workflow-scope unique ID used to refer from task invocations
    id: ConstantNodeId

    # Serialized value
    value: str
    serialization_format: ArtifactFormat = pydantic.Field(
        ArtifactFormat.JSON,
        const=True,
    )

    # Human-readable string that can be rendered on the UI to represent the value.
    value_preview: pydantic.constr(max_length=12)  # type: ignore


class ConstantNodePickle(BaseModel):
    """Piece of data that already exists at workflow submission time.

    The value is directly embedded in the workflow. To support arbitrary data shapes we
    keep the value in a serialized form. Pickling constants is a fallback for storing
    values that are not JSON-serializable.
    """

    # Workflow-scope unique ID used to refer from task invocations
    id: ConstantNodeId

    # Serialized value
    chunks: t.List[str]
    serialization_format: ArtifactFormat = pydantic.Field(
        ArtifactFormat.ENCODED_PICKLE,
        const=True,
    )

    # Human-readable string that can be rendered on the UI to represent the value.
    value_preview: pydantic.constr(max_length=12)  # type: ignore


# General ConstantNode that can hold constants that are not JSON-serializable
ConstantNode = t.Union[ConstantNodeJSON, ConstantNodePickle]
# ID of a node that can be a task argument. Multiple node types can be task inputs.
# This is contrary to the outputs; only artifact nodes can be task outputs.
ArgumentId = t.Union[ArtifactNodeId, ConstantNodeId, SecretNodeId]


class TaskInvocation(BaseModel):
    id: TaskInvocationId

    # What task should be executed.
    task_id: TaskDefId

    args_ids: t.List[ArgumentId]
    # Key: task parameter name. Should match one of parameters of the `Task` referenced
    #     by `task_id`.
    # Value: id of the constant or artifact that will contain the value at runtime.
    kwargs_ids: t.Dict[ParameterName, ArgumentId]

    # Where to store the returned values.
    output_ids: t.List[ArtifactNodeId]

    # TaskInvocation specific resources
    resources: t.Optional[Resources]

    # Specification for custom image more scoped than TaskDef custom_image field
    # if not set, will fall back to TaskDef custom_image
    custom_image: t.Optional[str]


WorkflowDefName = str


class WorkflowDef(BaseModel):
    """The main data structure for intermediate workflow representation.

    The structure is as flat as possible with relation based on "id"s, e.g. a single
    task invocation has a relation to the task it invokes, argument artifacts or
    constants and returned artifacts.
    """

    name: WorkflowDefName

    # Used by Orquestra Studio to power jump-to-definition (file_path + line_number).
    fn_ref: FunctionRef

    # nodes with IDs & metadata
    imports: t.Dict[ImportId, Import]
    tasks: t.Dict[TaskDefId, TaskDef]
    artifact_nodes: t.Dict[ArtifactNodeId, ArtifactNode]
    constant_nodes: t.Dict[ConstantNodeId, ConstantNode]
    secret_nodes: t.Dict[SecretNodeId, SecretNode] = {}

    # The actual graph in form node clusters ("invocations"). Each task invocation is a
    # small subgraph with TaskNode in the center and connections to data nodes
    # (ArtifactNode or ConstantNode). This representation allows to easily specify order
    # of arguments or returned values.
    task_invocations: t.Dict[TaskInvocationId, TaskInvocation]

    # IDs of the nodes that are considered as workflow outputs. The referenced nodes
    # can be constants or artifacts.
    output_ids: t.List[ArgumentId]

    data_aggregation: t.Optional[DataAggregation]
