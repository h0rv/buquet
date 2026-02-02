# Type stubs for the buquet native module.

from collections.abc import Callable, Coroutine, Sequence
from typing import Any

from buquet.typing import TaskSchema

# Workflow submodule
class workflow:  # noqa: N801
    # Enums
    class WorkflowStatus:
        Pending: workflow.WorkflowStatus
        Running: workflow.WorkflowStatus
        WaitingSignal: workflow.WorkflowStatus
        Compensating: workflow.WorkflowStatus
        Paused: workflow.WorkflowStatus
        Completed: workflow.WorkflowStatus
        Failed: workflow.WorkflowStatus
        Cancelled: workflow.WorkflowStatus
        def is_terminal(self) -> bool: ...
        def is_active(self) -> bool: ...

    class StepStatus:
        Pending: workflow.StepStatus
        Running: workflow.StepStatus
        Completed: workflow.StepStatus
        Failed: workflow.StepStatus
        Cancelled: workflow.StepStatus

    class OnFailure:
        FailWorkflow: workflow.OnFailure
        PauseWorkflow: workflow.OnFailure
        Continue: workflow.OnFailure
        Compensate: workflow.OnFailure

    # Type classes
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
            status: workflow.StepStatus = ...,
            started_at: str | None = None,
            completed_at: str | None = None,
            result: object = None,
            error: str | None = None,
            attempt: int = 0,
            task_id: str | None = None,
        ) -> None: ...
        @property
        def status(self) -> workflow.StepStatus: ...
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
            status: workflow.WorkflowStatus = ...,
            current_steps: list[str] | None = None,
            error: workflow.WorkflowErrorInfo | None = None,
            orchestrator_task_id: str | None = None,
        ) -> None: ...
        @property
        def id(self) -> str: ...
        @property
        def workflow_type(self) -> str: ...
        @property
        def definition_hash(self) -> str: ...
        @property
        def status(self) -> workflow.WorkflowStatus: ...
        @property
        def current_steps(self) -> list[str]: ...
        @property
        def data(self) -> object: ...
        @property
        def steps(self) -> dict[str, workflow.StepState]: ...
        @property
        def signals(self) -> dict[str, workflow.SignalCursor]: ...
        @property
        def error(self) -> workflow.WorkflowErrorInfo | None: ...
        @property
        def created_at(self) -> str: ...
        @property
        def updated_at(self) -> str: ...
        @property
        def orchestrator_task_id(self) -> str | None: ...
        def set_step(self, name: str, state: workflow.StepState) -> None: ...
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
            on_failure: workflow.OnFailure = ...,
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
        def on_failure(self) -> workflow.OnFailure: ...
        @property
        def has_compensation(self) -> bool: ...
        def to_dict(self) -> dict[str, object]: ...

    class WorkflowRun:
        def __init__(
            self,
            id: str,  # noqa: A002
            status: workflow.WorkflowStatus,
            workflow_type: str | None = None,
            data: object = None,
            current_steps: list[str] | None = None,
            error: workflow.WorkflowErrorInfo | None = None,
        ) -> None: ...
        @property
        def id(self) -> str: ...
        @property
        def status(self) -> workflow.WorkflowStatus: ...
        @property
        def workflow_type(self) -> str | None: ...
        @property
        def data(self) -> object: ...
        @property
        def current_steps(self) -> list[str] | None: ...
        @property
        def error(self) -> workflow.WorkflowErrorInfo | None: ...
        def to_dict(self) -> dict[str, object]: ...
        @staticmethod
        def from_state(state: workflow.WorkflowState) -> workflow.WorkflowRun: ...

    # Manager classes
    class StateManager:
        async def get(self, wf_id: str) -> tuple[workflow.WorkflowState, str]: ...
        async def update(
            self,
            wf_id: str,
            state: workflow.WorkflowState,
            etag: str,
            now: str,
        ) -> str: ...
        async def create(self, wf_id: str, state: workflow.WorkflowState) -> str: ...
        async def exists(self, wf_id: str) -> bool: ...
        async def delete(self, wf_id: str) -> None: ...
        async def list(self, prefix: str = "", limit: int = 1000) -> list[str]: ...
        async def save_step_result(
            self,
            wf_id: str,
            step: str,
            result: bytes,
        ) -> str | None: ...
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
        ) -> list[tuple[str, workflow.Signal]]: ...
        async def get_next(
            self,
            wf_id: str,
            name: str,
            cursor: str | None = None,
        ) -> tuple[str, workflow.Signal] | None: ...
        async def count(
            self,
            wf_id: str,
            name: str,
            cursor: str | None = None,
        ) -> int: ...
        async def delete_all(self, wf_id: str, name: str) -> int: ...

    # Engine classes
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
        ) -> workflow.WorkflowEngine: ...
        @property
        def state(self) -> workflow.StateManager: ...
        @property
        def signals(self) -> workflow.SignalManager: ...
        async def start_workflow(
            self,
            workflow_id: str,
            workflow_type: str,
            definition_hash: str,
            data: object,
            now: str,
        ) -> tuple[bool, workflow.WorkflowState]: ...
        async def orchestrate(
            self,
            workflow_id: str,
            steps: list[workflow.StepDef],
            definition_hash: str,
            now: str,
        ) -> tuple[workflow.OrchestrateResult, workflow.WorkflowState | None]: ...
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
        ) -> workflow.Signal | None: ...

    # Sweeper classes
    class RecoveryReason:
        @property
        def reason(self) -> str: ...

    class WorkflowNeedsRecovery:
        @property
        def workflow_id(self) -> str: ...
        @property
        def state(self) -> workflow.WorkflowState: ...
        @property
        def reason(self) -> workflow.RecoveryReason: ...

    class SweepResult:
        @property
        def scanned(self) -> int: ...
        @property
        def needs_recovery(self) -> list[workflow.WorkflowNeedsRecovery]: ...
        @property
        def terminal(self) -> int: ...
        @property
        def healthy(self) -> int: ...

    class WorkflowSweeper:
        async def scan_simple(
            self,
            prefix: str = "",
            limit: int = 1000,
        ) -> workflow.SweepResult: ...

    # DAG functions
    @staticmethod
    def compute_ready_steps(
        steps: list[workflow.StepDef],
        state: workflow.WorkflowState,
    ) -> list[str]: ...
    @staticmethod
    def is_workflow_complete(
        steps: list[workflow.StepDef],
        state: workflow.WorkflowState,
    ) -> bool: ...
    @staticmethod
    def has_unrecoverable_failure(
        steps: list[workflow.StepDef],
        state: workflow.WorkflowState,
    ) -> bool: ...
    @staticmethod
    def should_pause_workflow(
        steps: list[workflow.StepDef],
        state: workflow.WorkflowState,
    ) -> bool: ...
    @staticmethod
    def get_compensation_chain(
        steps: list[workflow.StepDef],
        state: workflow.WorkflowState,
    ) -> list[str]: ...
    @staticmethod
    def get_blocked_steps(
        steps: list[workflow.StepDef],
        state: workflow.WorkflowState,
    ) -> list[str]: ...
    @staticmethod
    def topological_sort(steps: list[workflow.StepDef]) -> list[str]: ...
    @staticmethod
    def validate_dag(steps: list[workflow.StepDef]) -> list[str]: ...
    @staticmethod
    def compute_definition_hash(steps: list[workflow.StepDef]) -> str: ...
    @staticmethod
    def parse_signal_key(key: str) -> tuple[str, str, str, str] | None: ...

class WorkerConfig:
    @property
    def poll_interval_ms(self) -> int | None: ...
    @property
    def index_mode(self) -> str | None: ...
    @property
    def shards(self) -> list[str] | None: ...

class MonitorConfig:
    @property
    def check_interval_secs(self) -> int | None: ...
    @property
    def sweep_interval_secs(self) -> int | None: ...

class Config:
    @property
    def bucket(self) -> str: ...
    @property
    def endpoint(self) -> str | None: ...
    @property
    def region(self) -> str: ...
    @property
    def worker(self) -> WorkerConfig: ...
    @property
    def monitor(self) -> MonitorConfig: ...

def py_load_config(profile: str | None = None) -> Config: ...

class PollingStrategy:
    @staticmethod
    def fixed(interval_ms: int = 500) -> PollingStrategy: ...
    @staticmethod
    def adaptive(
        min_interval_ms: int = 100,
        max_interval_ms: int = 5000,
        backoff_multiplier: float = 2.0,
    ) -> PollingStrategy: ...

class ShardLeaseConfig:
    @staticmethod
    def disabled() -> ShardLeaseConfig: ...
    @staticmethod
    def enabled() -> ShardLeaseConfig: ...
    @staticmethod
    def custom(
        shards_per_worker: int = 16,
        lease_ttl_secs: int = 30,
        renewal_interval_secs: int = 10,
    ) -> ShardLeaseConfig: ...
    @property
    def is_enabled(self) -> bool: ...
    @property
    def shards_per_worker(self) -> int: ...
    @property
    def lease_ttl_secs(self) -> int: ...
    @property
    def renewal_interval_secs(self) -> int: ...

class Schedule:
    @property
    def id(self) -> str: ...
    @property
    def task_type(self) -> str: ...
    @property
    def input(self) -> object: ...
    @property
    def cron(self) -> str: ...
    @property
    def enabled(self) -> bool: ...
    @property
    def timeout_seconds(self) -> int | None: ...
    @property
    def max_retries(self) -> int | None: ...
    @property
    def created_at(self) -> str: ...
    @property
    def updated_at(self) -> str: ...
    def to_dict(self) -> dict[str, object]: ...

class ScheduleLastRun:
    @property
    def schedule_id(self) -> str: ...
    @property
    def last_run_at(self) -> str: ...
    @property
    def last_task_id(self) -> str: ...
    @property
    def next_run_at(self) -> str: ...

class TaskStatus:
    Pending: TaskStatus
    Running: TaskStatus
    Completed: TaskStatus
    Failed: TaskStatus
    Cancelled: TaskStatus
    Archived: TaskStatus
    Expired: TaskStatus

class RetryPolicy:
    def __init__(
        self,
        initial_interval_ms: int = 1000,
        max_interval_ms: int = 60000,
        multiplier: float = 2.0,
        jitter_percent: float = 0.25,
    ) -> None: ...
    @property
    def initial_interval_ms(self) -> int: ...
    @property
    def max_interval_ms(self) -> int: ...
    @property
    def multiplier(self) -> float: ...
    @property
    def jitter_percent(self) -> float: ...
    def calculate_delay_ms(self, attempt: int) -> int: ...

class TaskContext:
    @property
    def task_id(self) -> str: ...
    async def extend_lease(self, additional_secs: int) -> None: ...
    async def lease_expires_at(self) -> str | None: ...
    async def refresh(self) -> None: ...
    async def is_cancellation_requested(self) -> bool: ...

class Task:
    @property
    def id(self) -> str: ...
    @property
    def shard(self) -> str: ...
    @property
    def task_type(self) -> str: ...
    @property
    def input(self) -> object: ...
    @property
    def output(self) -> object | None: ...
    @property
    def status(self) -> TaskStatus: ...
    @property
    def timeout_seconds(self) -> int: ...
    @property
    def max_retries(self) -> int: ...
    @property
    def retry_count(self) -> int: ...
    @property
    def retry_policy(self) -> RetryPolicy: ...
    @property
    def created_at(self) -> str: ...
    @property
    def available_at(self) -> str: ...
    @property
    def lease_expires_at(self) -> str | None: ...
    @property
    def lease_id(self) -> str | None: ...
    @property
    def attempt(self) -> int: ...
    @property
    def updated_at(self) -> str: ...
    @property
    def completed_at(self) -> str | None: ...
    @property
    def worker_id(self) -> str | None: ...
    @property
    def last_error(self) -> str | None: ...
    @property
    def input_ref(self) -> str | None: ...
    @property
    def output_ref(self) -> str | None: ...
    @property
    def reschedule_count(self) -> int: ...
    @property
    def max_reschedules(self) -> int | None: ...
    @property
    def cancel_requested(self) -> bool: ...
    @property
    def cancelled_at(self) -> str | None: ...
    @property
    def cancelled_by(self) -> str | None: ...
    @property
    def expires_at(self) -> str | None: ...
    @property
    def expired_at(self) -> str | None: ...
    def can_retry(self) -> bool: ...
    def is_available_at(self, now_rfc3339: str) -> bool: ...
    def is_lease_expired_at(self, now_rfc3339: str) -> bool: ...
    def is_timed_out_at(self, now_rfc3339: str) -> bool: ...
    def is_expired_at(self, now_rfc3339: str) -> bool: ...
    def to_dict(self) -> dict[str, object]: ...

class CancelByTypeResult:
    @property
    def cancelled(self) -> list[Task]: ...
    @property
    def failed(self) -> list[tuple[str, str]]: ...

class StorageClient:
    async def get(self, key: str) -> tuple[bytes, str]: ...
    async def put(
        self,
        key: str,
        data: bytes,
        if_match: str | None = None,
    ) -> str: ...
    async def delete(self, key: str) -> None: ...
    async def list(
        self,
        prefix: str,
        start_after: str | None = None,
        limit: int = 1000,
    ) -> list[str]: ...
    async def exists(self, key: str) -> bool: ...

class Queue:
    async def submit(
        self,
        task_type: str,
        input: object,  # noqa: A002
        timeout_seconds: int | None = None,
        max_retries: int | None = None,
        retry_policy: RetryPolicy | None = None,
        schedule_at: object | None = None,
        use_payload_refs: bool = False,
        payload_ref_threshold_bytes: int | None = None,
        idempotency_key: str | None = None,
        idempotency_ttl_days: int | None = None,
        idempotency_scope: str | None = None,
        max_reschedules: int | None = None,
        ttl_seconds: int | None = None,
        expires_at: object | None = None,
    ) -> Task: ...
    async def now(self) -> str: ...
    async def submit_many(
        self,
        tasks: Sequence[tuple[str, object]],
        timeout_seconds: int | None = None,
        max_retries: int | None = None,
        retry_policy: RetryPolicy | None = None,
        max_reschedules: int | None = None,
        ttl_seconds: int | None = None,
        expires_at: object | None = None,
    ) -> list[Task]: ...
    async def get(self, task_id: str) -> Task | None: ...
    async def list(
        self,
        shard: str,
        status: TaskStatus | None = None,
        limit: int = 100,
    ) -> list[Task]: ...
    async def list_ready(
        self,
        shard: str,
        limit: int = 100,
    ) -> list[str]: ...
    async def get_history(self, task_id: str) -> list[Task]: ...
    async def load_input(self, task: Task) -> object: ...
    async def load_output(self, task: Task) -> object | None: ...
    async def publish_schema(
        self,
        task_type: str,
        schema: TaskSchema,
    ) -> None: ...
    async def get_schema(self, task_type: str) -> TaskSchema | None: ...
    async def list_schemas(self) -> list[str]: ...
    async def delete_schema(self, task_type: str) -> None: ...
    def validate_input(self, schema: TaskSchema, data: object) -> None: ...
    def validate_output(self, schema: TaskSchema, data: object) -> None: ...
    @property
    def shard_prefix_len(self) -> int: ...
    @property
    def storage(self) -> StorageClient: ...
    def all_shards(self) -> list[str]: ...
    async def create_schedule(
        self,
        id: str,  # noqa: A002
        task_type: str,
        input: object,  # noqa: A002
        cron: str,
        timeout_seconds: int | None = None,
        max_retries: int | None = None,
    ) -> Schedule: ...
    async def get_schedule(self, id: str) -> Schedule | None: ...  # noqa: A002
    async def list_schedules(self) -> list[Schedule]: ...
    async def delete_schedule(self, id: str) -> None: ...  # noqa: A002
    async def enable_schedule(self, id: str) -> None: ...  # noqa: A002
    async def disable_schedule(self, id: str) -> None: ...  # noqa: A002
    async def trigger_schedule(self, id: str) -> Task: ...  # noqa: A002
    async def get_schedule_last_run(self, id: str) -> ScheduleLastRun | None: ...  # noqa: A002
    async def cancel(
        self,
        task_id: str,
        reason: str | None = None,
        cancelled_by: str | None = None,
    ) -> Task: ...
    async def cancel_many(
        self,
        task_ids: list[str],
        reason: str | None = None,
        cancelled_by: str | None = None,
    ) -> list[Task]: ...
    async def cancel_by_type(
        self,
        task_type: str,
        reason: str | None = None,
        cancelled_by: str | None = None,
    ) -> CancelByTypeResult: ...
    async def request_cancellation(self, task_id: str) -> Task: ...
    async def mark_expired(self, task_id: str) -> Task: ...

class Worker:
    def __init__(
        self,
        queue: Queue,
        worker_id: str,
        shards: list[str],
    ) -> None: ...
    @property
    def worker_id(self) -> str: ...
    @property
    def shards(self) -> list[str]: ...
    def task(
        self,
        task_type: str,
    ) -> Callable[
        [Callable[..., Coroutine[Any, Any, Any]]], Callable[..., Coroutine[Any, Any, Any]]
    ]: ...
    def on_startup(
        self,
        func: Callable[[], Coroutine[Any, Any, None]],
    ) -> Callable[[], Coroutine[Any, Any, None]]: ...
    def on_shutdown(
        self,
        func: Callable[[], Coroutine[Any, Any, None]],
    ) -> Callable[[], Coroutine[Any, Any, None]]: ...
    def on_success(
        self,
        func: Callable[[Task], Coroutine[Any, Any, None]],
    ) -> Callable[[Task], Coroutine[Any, Any, None]]: ...
    def on_error(
        self,
        func: Callable[[Task, str], Coroutine[Any, Any, None]],
    ) -> Callable[[Task, str], Coroutine[Any, Any, None]]: ...
    def registered_task_types(self) -> list[str]: ...
    async def run(
        self,
        options: WorkerRunOptions | None = None,
    ) -> int: ...

class WorkerRunOptions:
    def __init__(
        self,
        polling: PollingStrategy | None = None,
        poll_interval_ms: int = 1000,
        max_tasks: int | None = None,
        with_monitor: bool = True,
        monitor_check_interval_s: int = 30,
        monitor_worker_health_threshold_s: int = 60,
        monitor_sweep_interval_s: int = 300,
        monitor_sweep_page_size: int = 1000,
        shard_lease_config: ShardLeaseConfig | None = None,
        shard_leasing_enabled: bool = False,
        shard_leasing_shards_per_worker: int = 16,
        shard_leasing_ttl_s: int = 30,
        shard_leasing_renewal_interval_s: int = 10,
    ) -> None: ...
    polling: PollingStrategy | None
    poll_interval_ms: int
    max_tasks: int | None
    with_monitor: bool
    monitor_check_interval_s: int
    monitor_worker_health_threshold_s: int
    monitor_sweep_interval_s: int
    monitor_sweep_page_size: int
    shard_lease_config: ShardLeaseConfig | None
    shard_leasing_enabled: bool
    shard_leasing_shards_per_worker: int
    shard_leasing_ttl_s: int
    shard_leasing_renewal_interval_s: int

async def connect(
    endpoint: str | None = None,
    bucket: str | None = None,
    region: str | None = None,
    shard_prefix_len: int | None = None,
) -> Queue: ...

class metrics:  # noqa: N801
    @staticmethod
    def enable_prometheus(port: int = 9000) -> None: ...
    @staticmethod
    def enable_statsd(host: str = "127.0.0.1", port: int = 8125) -> None: ...
    @staticmethod
    def enable_opentelemetry(endpoint: str = "http://localhost:4317") -> None: ...
    @staticmethod
    def auto_configure() -> bool: ...
    @staticmethod
    def current_exporter() -> str | None: ...
