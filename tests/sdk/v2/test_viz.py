################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
from orquestra.sdk._base import _viz

from . import sample_wfs


class TestWfDefGraph:
    """
    Test boundary: [ir.WorkflowDef]-[_viz.DiGraph]
    """

    @staticmethod
    def test_example_nodes_edges():
        """ """
        wf_def = sample_wfs.wf(123).model
        graph = _viz.wf_def_graph(wf_def)

        assert graph.nodes1 == [
            _viz.Node(
                id="invocation-0-task-inc",
                caption=["tests.sdk.v2.sample_wfs.inc():17", "invocation-0-task-inc"],
            ),
            _viz.Node(
                id="invocation-1-task-inc-2",
                caption=["[inlined].inc_2()", "invocation-1-task-inc-2"],
            ),
            _viz.Node(
                id="invocation-2-task-integer-division",
                caption=[
                    "tests.sdk.v2.sample_wfs.integer_division():27",
                    "invocation-2-task-integer-division",
                ],
            ),
            _viz.Node(
                id="invocation-3-task-add",
                caption=["tests.sdk.v2.sample_wfs.add():12", "invocation-3-task-add"],
            ),
        ]
        assert graph.nodes2 == [
            _viz.Node(id="artifact-0-inc", caption=[]),
            _viz.Node(id="artifact-1-inc-2", caption=[]),
            _viz.Node(id="artifact-2-integer-division", caption=[]),
            _viz.Node(id="artifact-3-integer-division", caption=[]),
            _viz.Node(id="artifact-4-add", caption=[]),
            _viz.Node(id="constant-0", caption=["3"]),
            _viz.Node(id="constant-1", caption=["123"]),
            _viz.Node(id="constant-2", caption=["6"]),
        ]
        assert graph.edges == [
            ("artifact-1-inc-2", "invocation-0-task-inc"),
            ("invocation-0-task-inc", "artifact-0-inc"),
            ("artifact-3-integer-division", "invocation-1-task-inc-2"),
            ("invocation-1-task-inc-2", "artifact-1-inc-2"),
            ("constant-1", "invocation-2-task-integer-division"),
            ("constant-0", "invocation-2-task-integer-division"),
            ("invocation-2-task-integer-division", "artifact-2-integer-division"),
            ("invocation-2-task-integer-division", "artifact-3-integer-division"),
            ("artifact-2-integer-division", "invocation-3-task-add"),
            ("constant-2", "invocation-3-task-add"),
            ("invocation-3-task-add", "artifact-4-add"),
        ]
