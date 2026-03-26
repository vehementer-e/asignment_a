from framework.audit import ControlLogger, DQAuditLogger, RunMetadataBuilder


class StubConfig:
    def __init__(self, version: str):
        self.version = version

    def model_dump(self, mode: str = "json"):
        return {"version": self.version}


pipeline_cfg = StubConfig(version="1.0.0")
env_cfg = StubConfig(version="dev")
rulepacks = [StubConfig(version="common")]

builder = RunMetadataBuilder(
    pipeline_id="clients_curated",
    environment="dev",
    runtime_params={"business_date": "2026-03-26"},
)
metadata = builder.build(
    pipeline_config=pipeline_cfg,
    environment_config=env_cfg,
    rulepacks=rulepacks,
)

control = ControlLogger()
control.log_run_start(metadata)
control.log_step_start(run_id=metadata["run_id"], step_id="read_clients", step_type="read")
control.log_step_end(
    run_id=metadata["run_id"],
    step_id="read_clients",
    status="SUCCESS",
    output_count=100,
)
control.log_run_end(run_id=metadata["run_id"], status="SUCCESS", summary={"steps": 1})

class RuleResult:
    def model_dump(self, mode: str = "json"):
        return {
            "rule_id": "clients_id_not_null",
            "rule_type": "not_null",
            "status": "PASSED",
            "failed_count": 0,
            "row_count": 100,
            "details": {},
        }


dq = DQAuditLogger()
dq.log_result(
    run_id=metadata["run_id"],
    pipeline_id="clients_curated",
    dataset="clients_silver",
    rule_result=RuleResult(),
    stage="silver",
)

print("Run records:", control.run_records)
print("Step records:", control.step_records)
print("DQ records:", dq.records)
