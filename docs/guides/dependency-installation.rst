Dependency Installation
=======================

Many tasks will require external sources and dependencies in order to function. These are specified on a per-task basis by passing import arguments to the ``@sdk.task()`` decorator. Multiple dependency import arguments can be used simultaneously.

For convenience the table below summarises which importer should be used in various contexts. The following sections give a more complete explanation of these importers and their usage.

.. list-table:: Quick Reference
    :widths: 25 25 25 25
    :header-rows: 1

    * - Importing From
      - Importer
      - Runtime
      - Notes
    * - GitHub repo
      - ``GitHubImport``
      - remote
      - On Compute Engine, secrets can be used to access private repos.
    * - Non-GitHub git repo
      - ``GitImport``
      - remote
      -
    * - PyPi
      - ``PythonImports``
      - remote
      -
    * - Jupyter Notebook
      - ``InlineImport``
      - remote, local
      - Actually having to specify an ``InlineImport`` is rare as this is the default import when running in a Jupyter Notebook. There are occasional cases where this should be used outside of Notebooks, see the section below for details.
    * - Local - file within this repo.
      - None
      - local
      - This is the default in all non-interactive sessions, so does not need to specified explicitly.

Good Practice
-------------

For workflows running remotely the ``GitHubImport`` importer should be used unless there is a specific reason to do otherwise (i.e. the code is hosted somewhere other than GitHub). ``GitImport.infer()`` is highly user-specific and so should never be used in a collaborative context.

For workflows running locally, the task decorator can typically be used without specifying the source importer as the SDK will set this appropriately for the context:

.. code-block:: python
    @sdk.task
    def my_task():
        pass

will perform a local import unless running in a Jupyter Notebook (or other interactive session) in which case am inline import will be performed.


Source vs Dependency
--------------------

The ``@sdk.task`` decorator accepts two types of import arguments - source and dependency. Both accept any of the import types specified here. The importer passed to ``source_import`` should contain the source code for the task, while external dependencies should be specified to ``dependency_imports``.

For illustration, the following example specifies a task that imports its source code from a GitHub repo, and depends on several python modules and a GitLab repo.

.. code-block:: python

    import orquestra.sdk as sdk

    @sdk.task(
        source_import = sdk.GitHubImport(
            "zapatacomputing/my_source_repo",
            git_ref = "feat/my-experimental-feature",
        ),
        dependency_imports = [
            sdk.PythonImports("typing_extensions==4.5.0", "pytest~=6.2.5"),
            sdk.GitImport(
                repo_url = "https://example.com/some_person/some_repo.git",
                git_ref = "v1.0.1",
            )
        ]
    )
    def demo_task():
        pass

How sources and dependencies are treated depends on the runtime.

The examples below omit ``dependency_imports`` for clarity as the call pattern for an importer is identical regardless of whether it is importing a source or a dependency.

``GitHubImport``
----------------

The ``GitHubImport`` importer is the preferred option where the source or dependency is stored in a GitHub repo. The following minimal example shows a source import from a (fictional) repo located at ``https://github.com/zapatacomputing/my_source_repo``.

.. code-block:: python
    import orquestra.sdk as sdk

    @sdk.task(
        source_import=sdk.GitHubImport("zapatacomputing/my_source_repo")
    )
    def demo_task():
        pass

By default ``GitHubImport`` will clone the ``main`` branch of the repo. For additional control a git reference (branch name, tag, or commit) may be specified:

.. code-block:: python
    import orquestra.sdk as sdk

    @sdk.task(
        source_import=sdk.GitHubImport(
            "zapatacomputing/my_source_repo",
            git_ref = "feat/my-feature-branch",
        )
    )
    def demo_task():
        pass

The contents of the repo are pip installed at execution time.

``GitHubImport`` from a private repo
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``GitHubImport`` importer supports using the ``sdk.Secret`` functionality to allow runtimes to import from private repos when using the Compute Engine runtime. To use this functionality, the following steps must be carried out:

1. Create a personal access token (PAT) in GitHub with permission to access the private repo.
2. Create a new secret in the orquestra portal containing the PAT. For this example we have named our secret "my_pat".
3. Use the name of the portal secret to specify the ``personal_access_token`` argument for your import.

.. code-block:: python
    import orquestra.sdk as sdk

    @sdk.task(
        source_import=sdk.GitHubImport(
            "zapatacomputing/my_source_repo",
            git_ref = "feat/my-feature-branch",
            username = "my-github-username",
            personal_access_token = sdk.Secret("my_pat")
        )
    )
    def demo_task():
        pass

The PAT is ingested from the portal secrets management facility at run time, and installation of the import proceeds as usual.

``GitImport``
-------------

For sources or dependencies stored in a git repo that is hosted somewhere other than GitHub, the ``GitImport`` importer should be used. Unlike ``GitHubImport`` this requires the full URL of the repo.

.. code-block:: python
    import orquestra.sdk as sdk

    @sdk.task(
        source_import=sdk.GitImport(
            repo_url = "https://example.com/some_person/some_repo.git",
            git_ref = "v1.0.1",
        )
    )
    def demo_task():
        pass

As in the case of ``GitHubImport``, the code ingested from the repo is pip installed at execution time.

``GitImport.infer``
~~~~~~~~~~~~~~~~~~~

The ``GitImport.infer`` importer is a shortcut for ``GitImport`` that tries to extrapolate the URL and reference from a local clone of the repo. It takes as its argument the path to a local git repo with, optionally, a git reference. The path should be relative to the current working directory from which the workflow is submitted. During traversal, the `origin` remote of the specified repo will be used to infer the URL. Thereafter this functions identically to a ``GitImport`` call.

.. code-block:: python
    import orquestra.sdk as sdk

    @sdk.task(
        source_import=sdk.GitImport.infer("path/to/local/repo/clone")
    )
    def demo_task():
        pass

This utility can save time during prototyping, however its dependence on the individual user#s filesystem makes it unsuitable for collaborative projects. These should use a fully specified ``GitImport`` or ``GitHubImport`` instead.


``PythonImports``
-----------------

As the name suggests, the ``PythonImports`` importer handles ingestion of Python modules, specifically from PyPI.

The required modules can be specified as arguments to the importer, or listed in a requirements.txt file specified by the ``file`` argument.

The examples below use ``PythonImports`` as the source importer for consistency with the other examples in this section. In actual usage, however, Python imports will most likely be a dependency rather than a source.

.. code-block:: python
    import orquestra.sdk as sdk

    # Individually specified as arguments
    @sdk.task(
        source_import=sdk.PythonImports(
            "typing_extensions==4.5.0", "pytest~=6.2.5", "gpustat>=1.0.0"
        )
    )
    def demo_task_1():
        pass

    # Read from file
    @sdk.task(source_import=sdk.PythonImports(file = "./requirements.txt"))
    def demo_task_2():
        pass

    # Both
    @sdk.task(
        source_import=sdk.PythonImports(
            "typing_extensions==4.5.0", "pytest~=6.2.5", "gpustat>=1.0.0",
            file = "./requirements.txt",
        )
    )
    def demo_task_1():
        pass

The specified modules will be pip installed at execution time.

``InlineImport``
----------------

Cases where the ``InlineImport`` importer must be specified explicitly are rare, as the Workflow SDK will set it as the default source importer in contexts where it applies. We include it here for completeness.

The ``InlineImport`` importer ingests sources or dependencies that are defined in the ``__main__`` of the file that defines the workflow. This highly specific use case occurs almost exclusively in Jupyter Notebooks and interactive Python sessions, however it is occasionally useful in rapid development or prototyping contexts. In general, the Workflow SDK's help messages will flag up instances where it should be used.

The local importer
------------------

The local importer is the default source importer where no other importer is specified. It imports the current repo as a module, making its methods available to tasks in local workflow runs.
