################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Backwards-compatibility layer for accessing models generated with previous SDK versions.
"""
from . import ir


def n_outputs(task_def: ir.TaskDef, task_inv: ir.TaskInvocation) -> int:
    """
    Figures out number of task invocation outputs.

    Nowadays, we base this information on the task definition's metadata. With
    orquestra-sdk<=0.45.1 we relied on the task invocation.
    """
    if (meta := task_def.output_metadata) is not None:
        return meta.n_outputs
    else:
        return len(task_inv.output_ids)


def result_is_packed(task_def: ir.TaskDef) -> bool:
    """
    Tasks can return one or multiple values. We call multi-valued result "packed"
    because it can be unpacked in the workflow function.

    Nowadays, we base this information on the task definition's metadata. With
    orquestra-sdk<=0.45.1 the behavior was unreliable.
    """
    if (meta := task_def.output_metadata) is not None:
        return meta.is_subscriptable
    else:
        # We can't be sure. It's safer to assume the output is a single value so we
        # don't attempt to subscript it.
        return False
