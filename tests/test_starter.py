from __future__ import annotations

import json
from pathlib import Path

import pytest

from student_agent import OUTPUT_SCHEMA_VERSION, VARIANT_ID
from student_agent.cases import CaseSet, load_case_set
from student_agent.contracts import Contracts
from student_agent.submission import build_manifest


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def test_load_case_set_rejects_wrong_variant(tmp_path: Path) -> None:
    write_json(
        tmp_path / "case-set.json",
        {"case_set_version": "test-v1", "variant_id": "l3a", "case_ids": ["CASE_001"]},
    )
    write_json(tmp_path / "inputs" / "CASE_001.json", {"case_id": "CASE_001"})
    with pytest.raises(ValueError, match="expected variant"):
        load_case_set(tmp_path, expected_count=1)


def test_load_case_set_accepts_exact_input_inventory(tmp_path: Path) -> None:
    case_ids = ["CASE_001", "CASE_002"]
    write_json(
        tmp_path / "case-set.json",
        {"case_set_version": "test-v1", "variant_id": VARIANT_ID, "case_ids": case_ids},
    )
    for case_id in case_ids:
        write_json(tmp_path / "inputs" / f"{case_id}.json", {"case_id": case_id})
    loaded = load_case_set(tmp_path, expected_count=2)
    assert loaded.case_ids == tuple(case_ids)


def test_generated_manifest_matches_public_contract() -> None:
    root = Path(__file__).resolve().parents[1]
    contracts = Contracts(root / "contracts" / "schemas")
    case_set = CaseSet("test-v1", VARIANT_ID, ("CASE_001",), {})
    manifest = build_manifest(case_set)
    contracts.validate_manifest(manifest)
    assert manifest["output_schema_version"] == OUTPUT_SCHEMA_VERSION


@pytest.mark.anyio
async def test_solve_case_multi_agent_workflow(tmp_path: Path) -> None:
    from unittest.mock import AsyncMock
    from student_agent.trace import TraceWriter
    from student_agent.workflow import solve_case

    root = Path(__file__).resolve().parents[1]
    contracts = Contracts(root / "contracts" / "schemas")
    trace_path = tmp_path / "trace.jsonl"
    trace = TraceWriter(trace_path, contracts)

    # Mock gateway responses
    mock_gateway = AsyncMock()
    mock_gateway.call.side_effect = lambda tool_name, case_id, **kwargs: {
        "evidence_ref": f"ev_{tool_name}_12345678901234567890",
        "data": {
            "items": [{"item_id": "ITEM_1", "seller_id": "SELLER_1"}],
            "payments": [{"payment_ref": "PAY_1", "amount_brl": 100.0}],
            "shipment_id": "SHIP_1",
            "status": "on_time",
        },
    }

    case = {
        "case_id": "L3B_CASE_001",
        "order_id": "ORDER_100",
        "customer_id": "CUST_555",
        "candidates": ["ORDER_100", "ORDER_999"],
    }

    output = await solve_case(case, mock_gateway, trace)

    # Validate output schema
    contracts.validate_output(output, "workflow output")

    # Verify trace events emitted
    events = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").strip().splitlines()]
    event_types = [evt["event_type"] for evt in events]
    assert "case_received" in event_types
    assert "task_assigned" in event_types
    assert "handoff" in event_types
    assert "tool_result_consumed" in event_types
    assert "policy_decided" in event_types
    assert "verification_completed" in event_types
    assert "case_finalized" in event_types

