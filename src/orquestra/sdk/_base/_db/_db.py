################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import os
import sqlite3
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Union

from orquestra.sdk._base._db._migration import (
    migrate_project_db_to_shared_db,
    run_migrations,
)
from orquestra.sdk._base._env import DB_PATH_ENV
from orquestra.sdk._base.abc import WorkflowRepo
from orquestra.sdk.exceptions import WorkflowRunNotFoundError
from orquestra.sdk.schema.ir import WorkflowDef
from orquestra.sdk.schema.local_database import StoredWorkflowRun
from orquestra.sdk.schema.workflow_run import WorkflowRunId


def _create_workflow_table(db: sqlite3.Connection):
    with db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS workflow_runs (workflow_run_id, config_name, workflow_def)"  # noqa: E501
        )
        db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS workflow_runs_id ON workflow_runs(workflow_run_id)"  # noqa: E501
        )
        run_migrations(db)


def _get_default_db_location() -> Path:
    try:
        return Path(os.environ[DB_PATH_ENV])
    except KeyError:
        return Path.home() / ".orquestra" / "workflows.db"


class WorkflowDB(WorkflowRepo, AbstractContextManager):
    """
    SQLite storage for workflow runs
    """

    @classmethod
    def open_project_db(cls, project_dir: Union[Path, str]):
        old_db_location = Path(project_dir) / ".orquestra" / "workflows.db"
        if old_db_location != _get_default_db_location():
            migrate_project_db_to_shared_db(Path(project_dir))
        return cls.open_db()

    @classmethod
    def open_db(cls):
        db_location = _get_default_db_location()
        db_location.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(db_location, isolation_level="EXCLUSIVE")
        _create_workflow_table(db)
        return WorkflowDB(db)

    def __init__(self, db: sqlite3.Connection):
        self._db = db

    def __enter__(self):
        return self

    def __exit__(self, __exc_type, __exc_value, __traceback):
        self._db.commit()
        self._db.close()

    def save_workflow_run(
        self,
        workflow_run: StoredWorkflowRun,
    ):
        with self._db:
            self._db.execute(
                "INSERT INTO workflow_runs VALUES (?, ?, ?, ?)",
                (
                    workflow_run.workflow_run_id,
                    workflow_run.config_name,
                    workflow_run.workflow_def.json(),
                    workflow_run.is_qe,
                ),
            )

    def get_workflow_run(self, workflow_run_id: WorkflowRunId) -> StoredWorkflowRun:
        """Return the StoredWorkflowRun of a previous workflow with the specified ID.

        Args:
            workflow_run_id: the ID of the workflow to be returned.

        Raises:
            WorkflowRunNotFoundError: raised when no matching workflow exists in the
                database.

        Returns:
            StoredWorkflowRun: the details of the stored workflow.
        """
        with self._db:
            cur = self._db.cursor()
            cur.execute(
                "SELECT * FROM workflow_runs WHERE workflow_run_id=?",
                (workflow_run_id,),
            )
            result = cur.fetchone()
            if result is None or len(result) == 0:
                raise WorkflowRunNotFoundError(
                    f"Workflow run with ID {workflow_run_id} not found"
                )
            return StoredWorkflowRun(
                workflow_run_id=result[0],
                config_name=result[1],
                workflow_def=WorkflowDef.parse_raw(result[2]),
                is_qe=result[3] or True,
            )
