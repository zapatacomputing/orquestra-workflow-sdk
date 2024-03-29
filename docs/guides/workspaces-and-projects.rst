=======================
Workspaces and Projects
=======================

Compute Engine (CE) groups user-submitted entities like workflow runs, or secrets into *workspaces*.
The main reason for associating entities with workspaces is sharing data with other users.
You can invite other users to a workspace via Orquestra Portal to let them see your stuff.

Additionally, workflow runs on CE have another level of organisation: they're grouped by *projects*.
This categorisation is related to the Orquestra Studio's UI.

When you're using Orquestra Workflow SDK from Orquestra Studio, the workspace and project are set automatically.
When you're on a local machine and you're interacting with CE, you need to specify workspace and project you're interested in.

.. note::
   Orquestra workspaces and projects are purely server-side concepts.
   There's no built-in 1:1 correspondence with a git repo or your local IDE project.

Quick Reference
===============

The following table summarises the behaviour of the CLI and API under different combinations of explicit workspace and project ID arguments, and environment variables.

+--------------+------------+-----------+-----------+--------------------------------------------------------+--------------------------------------+
| Explicit Arguments        | Environment Variables | Behaviour                                                                                     |
+--------------+------------+-----------+-----------+--------------------------------------------------------+--------------------------------------+
| Workspace ID | Project ID | Workspace | Project   | CLI                                                    | API                                  |
+==============+============+===========+===========+========================================================+======================================+
| Yes                       | Any                   | Use explicit arg values                                                                       |
+--------------+------------+-----------+-----------+--------------------------------------------------------+--------------------------------------+
| Yes          | No         | Any       | Yes       | explicit arg workspace value, env var project value    | ProjectInvalidError                  |
+              +            +           +-----------+--------------------------------------------------------+                                      +
|              |            |           | No        | explicit arg workspace value, prompt for project value |                                      |
+--------------+------------+-----------+-----------+--------------------------------------------------------+                                      +
| No           | Yes        | Yes       | Any       | env var workspace value, explicit arg project value    |                                      |
+              +            +-----------+           +--------------------------------------------------------+                                      +
|              |            | No        |           | prompt for workspace value, explicit arg project value |                                      |
+--------------+------------+-----------+-----------+--------------------------------------------------------+--------------------------------------+
| No                                                | Prompt for workspace and project values                | Env vars ignored, no values inferred |
+--------------+------------+-----------+-----------+--------------------------------------------------------+                                      +
| No                        | Yes       | No        | env var Workspace value, prompt for project value      |                                      |
+                           +-----------+-----------+--------------------------------------------------------+                                      +
|                           | No        | Yes       | prompt for workspace value, env var project value      |                                      |
+                           +-----------+-----------+--------------------------------------------------------+--------------------------------------+
|                           | Yes                   | Use env var values                                                                            |
+--------------+------------+-----------+-----------+--------------------------------------------------------+--------------------------------------+


Orquestra CLI
=============

There are three mechanisms by which workspaces and projects can be specified when using the CLI.
In descending order of priority these are:


Explicit CLI Arguments
----------------------

When interacting with CE from the CLI, managing workspaces and projects is typically achieved by setting the ``-w`` and ``-p`` arguments respectively when using the relevant commands.


Environment Variables
---------------------

Where explicit arguments are not supplied, ``orq`` CLI looks for two environment variables, ``ORQ_CURRENT_WORKSPACE`` and ``ORQ_CURRENT_PROJECT``.
These are unset by default, but can be controlled using:

.. code-block::

    export ORQ_CURRENT_WORKSPACE=my_workspace
    export ORQ_CURRENT_PROJECT=my_project

These variables define a default value for the workspace and/or project that ``orq`` will use unless overruled by an explicit CLI argument.

The "vanilla" Orquestra behavior can be restored by simply unsetting these variables:

.. code-block::

    unset ORQ_CURRENT_WORKSPACE
    unset ORQ_CURRENT_PROJECT


Interactive Prompters
---------------------

Where the workspace and project arguments are not provided and one or both of the environment variables are not set, the CLI has a system of interactive prompters to allow the user to select the appropriate unspecified values.


Orquestra Python API
====================

The python API operates similarly to the CLI, taking explicit arguments to be authoritative and looking for environment variables where explicit arguments are not provided.
The primary difference is that the API does not permit combinations of explicit arguments and environment variables.
Specifying one but not both of the arguments provides insufficient information to uniquely identify a project.
Under these conditions, the API will raise a ``ProjectInvalidError``.
Similarly, when neither of the arguments are specified, and only one of the ``ORQ_CURRENT_WORKSPACE`` and ``ORQ_CURRENT_PROJECT`` environment variables are set, the API will ignore the set value rather than use an incomplete project reference.
