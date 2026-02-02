# Type stubs for the buquet_workflow native module.

# =============================================================================
# Enums
# =============================================================================


class WorkflowStatus:
    Pending: WorkflowStatus
    Running: WorkflowStatus
    WaitingSignal: WorkflowStatus
    Compensating: WorkflowStatus
    Paused: WorkflowStatus
    Completed: WorkflowStatus
    Failed: WorkflowStatus
    Cancelled: WorkflowStatus

    def is_terminal(self) -> bool: ...
    def is_active(self) -> bool: ...


class StepStatus:
    Pending: StepStatus
    Running: StepStatus
    Completed: StepStatus
    Failed: StepStatus
    Cancelled: StepStatus


class OnFailure:
    FailWorkflow: OnFailure
    PauseWorkflow: OnFailure
    Continue: OnFailure
    Compensate: OnFailure


# =============================================================================
# Type Classes
# =============================================================================


class WorkflowErrorInfo:
    def __init__(
        self,
        type_: str,
        message: str,
        step: str | None = None,
        attempt: int | None = None,
        at: str | None = None,
    ) -> None: ...
    @property
    def type_(self) -> str: ...
    @property
    def message(self) -> str: ...
    @property
    def step(self) -> str | None: ...
    @property
    def attempt(self) -> int | None: ...
    @property
    def at(self) -> str | None: ...
    def to_dict(self) -> dict[str, object]: ...


class SignalCursor:
    def __init__(self, cursor: str | None = None) -> None: ...
    @property
    def cursor(self) -> str | None: ...
    def to_dict(self) -> dict[str, object]: ...


class StepState:
    def __init__(
        self,
        status: StepStatus = ...,
        started_at: str | None = None,
        completed_at: str | None = None,
        result: object = None,
        error: str | None = None,
        attempt: int = 0,
        task_id: str | None = None,
    ) -> None: ...
    @property
    def status(self) -> StepStatus: ...
    @property
    def started_at(self) -> str | None: ...
    @property
    def completed_at(self) -> str | None: ...
    @property
    def result(self) -> object: ...
    @property
    def error(self) -> str | None: ...
    @property
    def attempt(self) -> int: ...
    @property
    def task_id(self) -> str | None: ...
    def to_dict(self) -> dict[str, object]: ...


class WorkflowState:
    def __init__(
        self,
        id: str,  # noqa: A002
        workflow_type: str,
        definition_hash: str,
        data: object,
        created_at: str,
        updated_at: str,
        status: WorkflowStatus = ...,
        current_steps: list[str] | None = None,
        error: WorkflowErrorInfo | None = None,
        orchestrator_task_id: str | None = None,
    ) -> None: ...
    @property
    def id(self) -> str: ...
    @property
    def workflow_type(self) -> str: ...
    @property
    def definition_hash(self) -> str: ...
    @property
    def status(self) -> WorkflowStatus: ...
    @property
    def current_steps(self) -> list[str]: ...
    @property
    def data(self) -> object: ...
    @property
    def steps(self) -> dict[str, StepState]: ...
    @property
    def signals(self) -> dict[str, SignalCursor]: ...
    @property
    def error(self) -> WorkflowErrorInfo | None: ...
    @property
    def created_at(self) -> str: ...
    @property
    def updated_at(self) -> str: ...
    @property
    def orchestrator_task_id(self) -> str | None: ...
    def set_step(self, name: str, state: StepState) -> None: ...
    def set_signal(self, name: str, cursor: str | None = None) -> None: ...
    def to_dict(self) -> dict[str, object]: ...


class Signal:
    def __init__(
        self,
        id: str,  # noqa: A002
        name: str,
        payload: object,
        created_at: str,
    ) -> None: ...
    @property
    def id(self) -> str: ...
    @property
    def name(self) -> str: ...
    @property
    def payload(self) -> object: ...
    @property
    def created_at(self) -> str: ...
    def to_dict(self) -> dict[str, object]: ...


class StepDef:
    def __init__(
        self,
        name: str,
        depends_on: list[str] | None = None,
        retries: int = 3,
        timeout: int = 300,
        on_failure: OnFailure = ...,
        has_compensation: bool = False,
    ) -> None: ...
    @property
    def name(self) -> str: ...
    @property
    def depends_on(self) -> list[str]: ...
    @property
    def retries(self) -> int: ...
    @property
    def timeout(self) -> int: ...
    @property
    def on_failure(self) -> OnFailure: ...
    @property
    def has_compensation(self) -> bool: ...
    def to_dict(self) -> dict[str, object]: ...


class WorkflowRun:
    def __init__(
        self,
        id: str,  # noqa: A002
        status: WorkflowStatus,
        workflow_type: str | None = None,
        data: object = None,
        current_steps: list[str] | None = None,
        error: WorkflowErrorInfo | None = None,
    ) -> None: ...
    @property
    def id(self) -> str: ...
    @property
    def status(self) -> WorkflowStatus: ...
    @property
    def workflow_type(self) -> str | None: ...
    @property
    def data(self) -> object: ...
    @property
    def current_steps(self) -> list[str] | None: ...
    @property
    def error(self) -> WorkflowErrorInfo | None: ...
    def to_dict(self) -> dict[str, object]: ...
    @staticmethod
    def from_state(state: WorkflowState) -> WorkflowRun: ...


# =============================================================================
# Managers
# =============================================================================


class StateManager:
    async def get(self, wf_id: str) -> tuple[WorkflowState, str]: ...
    async def update(
        self,
        wf_id: str,
        state: WorkflowState,
        etag: str,
        now: str,
    ) -> str: ...
    async def create(self, wf_id: str, state: WorkflowState) -> str: ...
    async def exists(self, wf_id: str) -> bool: ...
    async def delete(self, wf_id: str) -> None: ...
    async def list(self, prefix: str = "", limit: int = 1000) -> list[str]: ...
    async def save_step_result(
        self,
        wf_id: str,
        step: str,
        result: bytes,
    ) -> str: ...
    async def get_step_result(
        self,
        wf_id: str,
        step: str,
    ) -> tuple[bytes, str] | None: ...


class SignalManager:
    async def send(
        self,
        wf_id: str,
        name: str,
        payload: object,
        now: str,
    ) -> str: ...
    async def list(
        self,
        wf_id: str,
        name: str,
        cursor: str | None = None,
        limit: int = 100,
    ) -> list[tuple[str, Signal]]: ...
    async def get_next(
        self,
        wf_id: str,
        name: str,
        cursor: str | None = None,
    ) -> tuple[str, Signal] | None: ...
    async def count(
        self,
        wf_id: str,
        name: str,
        cursor: str | None = None,
    ) -> int: ...
    async def delete_all(self, wf_id: str, name: str) -> int: ...


# =============================================================================
# Engine
# =============================================================================


class OrchestrateResult:
    @property
    def result_type(self) -> str: ...
    @property
    def workflow_id(self) -> str: ...
    @property
    def status(self) -> str | None: ...
    @property
    def expected_hash(self) -> str | None: ...
    @property
    def actual_hash(self) -> str | None: ...
    @property
    def reason(self) -> str | None: ...
    @property
    def current_steps(self) -> list[str] | None: ...
    @property
    def submitted_steps(self) -> list[str] | None: ...


class WorkflowEngine:
    @staticmethod
    async def create(
        bucket: str,
        endpoint: str | None = None,
        region: str = "us-east-1",
    ) -> WorkflowEngine: ...
    @property
    def state(self) -> StateManager: ...
    @property
    def signals(self) -> SignalManager: ...
    async def start_workflow(
        self,
        workflow_id: str,
        workflow_type: str,
        definition_hash: str,
        data: object,
        now: str,
    ) -> tuple[bool, WorkflowState]: ...
    async def orchestrate(
        self,
        workflow_id: str,
        steps: list[StepDef],
        definition_hash: str,
        now: str,
    ) -> tuple[OrchestrateResult, WorkflowState | None]: ...
    async def mark_steps_running(
        self,
        workflow_id: str,
        step_names: list[str],
        now: str,
    ) -> None: ...
    async def complete_step(
        self,
        workflow_id: str,
        step_name: str,
        result: object,
        now: str,
    ) -> None: ...
    async def fail_step(
        self,
        workflow_id: str,
        step_name: str,
        error: str,
        now: str,
    ) -> None: ...
    async def cancel_step(
        self,
        workflow_id: str,
        step_name: str,
        reason: str,
        now: str,
    ) -> None: ...
    async def consume_signal(
        self,
        workflow_id: str,
        signal_name: str,
        now: str,
    ) -> Signal | None: ...


# =============================================================================
# Sweeper
# =============================================================================


class RecoveryReason:
    @property
    def reason(self) -> str: ...


class WorkflowNeedsRecovery:
    @property
    def workflow_id(self) -> str: ...
    @property
    def state(self) -> WorkflowState: ...
    @property
    def reason(self) -> RecoveryReason: ...


class SweepResult:
    @property
    def scanned(self) -> int: ...
    @property
    def needs_recovery(self) -> list[WorkflowNeedsRecovery]: ...
    @property
    def terminal(self) -> int: ...
    @property
    def healthy(self) -> int: ...


class WorkflowSweeper:
    async def scan_simple(
        self,
        prefix: str = "",
        limit: int = 1000,
    ) -> SweepResult: ...


# =============================================================================
# DAG Functions
# =============================================================================


def compute_ready_steps(steps: list[StepDef], state: WorkflowState) -> list[str]: ...
def is_workflow_complete(steps: list[StepDef], state: WorkflowState) -> bool: ...
def has_unrecoverable_failure(steps: list[StepDef], state: WorkflowState) -> bool: ...
def should_pause_workflow(steps: list[StepDef], state: WorkflowState) -> bool: ...
def get_compensation_chain(steps: list[StepDef], state: WorkflowState) -> list[str]: ...
def get_blocked_steps(steps: list[StepDef], state: WorkflowState) -> list[str]: ...
def topological_sort(steps: list[StepDef]) -> list[str]: ...
def validate_dag(steps: list[StepDef]) -> list[str]: ...
def compute_definition_hash(steps: list[StepDef]) -> str: ...
def parse_signal_key(key: str) -> tuple[str, str, str, str] | None: ...
