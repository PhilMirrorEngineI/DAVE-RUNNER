from orchestration.engine import OrchestrationEngine
from orchestration.executor import WorkerExecutor, HumanGateExecutionError
from orchestration.providers import DisabledProvider


engine = OrchestrationEngine()

executor = WorkerExecutor(
    engine,
    DisabledProvider(),
)

try:
    executor.execute(
        "persist-chain-1"
    )

    print(
        "FAIL: inference executed at human gate"
    )

except HumanGateExecutionError as err:

    print(
        "PASS:",
        err
    )