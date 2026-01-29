"""Worker integration for oq-workflows."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .orchestrator import create_orchestrator_handler
from .steps import create_step_handler

if TYPE_CHECKING:
    from oq import Queue, Worker

    from .workflow import Workflow


def register_workflow(
    worker: Worker,
    workflow: Workflow,
    queue: Queue,
) -> None:
    """
    Register all workflow handlers with an oq worker.

    This registers:
    1. The orchestrator task handler (workflow.orchestrate)
    2. Step task handlers for each step (workflow.step:{workflow_name}:{step_name})

    Args:
        worker: The oq worker
        workflow: The workflow definition
        queue: The oq queue

    Example:
        import oq
        from oq_workflows import Workflow, WorkflowClient, register_workflow

        # Define workflow
        wf = Workflow("order_fulfillment")

        @wf.step("validate")
        async def validate(ctx):
            return {"valid": True}

        @wf.step("process", depends_on=["validate"])
        async def process(ctx):
            return {"processed": True}

        # Set up worker
        queue = await oq.connect()
        worker = oq.Worker(queue, "worker-1", queue.all_shards())
        register_workflow(worker, wf, queue)

        # Run worker
        await worker.run()
    """
    # Validate workflow
    errors = workflow.validate()
    if errors:
        msg = f"Invalid workflow: {errors}"
        raise ValueError(msg)

    # Register orchestrator
    orchestrator_handler = create_orchestrator_handler(workflow, queue)
    worker.task("workflow.orchestrate")(orchestrator_handler)

    # Register step handlers
    for step_name in workflow.steps:
        task_type = f"workflow.step:{workflow.name}:{step_name}"
        step_handler = create_step_handler(workflow, step_name, queue)
        worker.task(task_type)(step_handler)


def register_workflows(
    worker: Worker,
    workflows: list[Workflow],
    queue: Queue,
) -> None:
    """
    Register multiple workflows with an oq worker.

    Args:
        worker: The oq worker
        workflows: List of workflow definitions
        queue: The oq queue
    """
    for workflow in workflows:
        register_workflow(worker, workflow, queue)
