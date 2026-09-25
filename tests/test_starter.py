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
    trace_text = trace_path.read_text(encoding="utf-8").strip()
    events = [json.loads(line) for line in trace_text.splitlines()]
    event_types = [evt["event_type"] for evt in events]
    assert "case_received" in event_types
    assert "task_assigned" in event_types
    assert "handoff" in event_types
    assert "tool_result_consumed" in event_types
    assert "policy_decided" in event_types
    assert "verification_completed" in event_types
    assert "case_finalized" in event_types


@pytest.mark.anyio
async def test_cli_resume_preserves_completed_output_and_trace(tmp_path: Path, monkeypatch) -> None:
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock, Mock

    from student_agent import cli

    case_ids = ("CASE_001", "CASE_002")
    cases = {case_id: {"case_id": case_id} for case_id in case_ids}
    monkeypatch.setattr(cli.Settings, "load", lambda root: Mock())
    monkeypatch.setattr(
        cli, "load_case_set", lambda root: CaseSet("test-v1", VARIANT_ID, case_ids, cases)
    )
    monkeypatch.setattr(cli, "Contracts", lambda root: Mock())
    gateway = AsyncMock()
    gateway.list_tools.return_value = ["get_order"]

    @asynccontextmanager
    async def connect(*args):
        yield gateway

    monkeypatch.setattr(cli, "connect_gateway", connect)
    solve = AsyncMock(return_value={"case_id": "CASE_002", "evidence_refs": []})
    monkeypatch.setattr(cli, "solve_case", solve)
    saved = tmp_path / "outputs" / "CASE_001.json"
    write_json(saved, {"case_id": "CASE_001", "evidence_refs": ["ev_saved"]})
    original_output = saved.read_bytes()
    trace = tmp_path / "traces" / "trace.jsonl"
    trace.parent.mkdir()
    original_trace = "".join(
        json.dumps(event) + "\n"
        for event in [
            {
                "case_id": "CASE_001",
                "event_type": "tool_result_consumed",
                "evidence_refs": ["ev_saved"],
            },
            {"case_id": "CASE_001", "event_type": "case_finalized"},
            {"case_id": "CASE_002", "event_type": "case_received"},
        ]
    )
    trace.write_text(original_trace, encoding="utf-8")

    await cli._run(tmp_path, resume=True)

    solve.assert_awaited_once()
    assert solve.call_args.args[0] == cases["CASE_002"]
    assert saved.read_bytes() == original_output
    assert trace.read_text(encoding="utf-8").startswith(original_trace)
    assert (tmp_path / "outputs" / "CASE_002.json").exists()


@pytest.mark.anyio
async def test_reused_order_id_uses_purchase_before_complaint(tmp_path: Path) -> None:
    from unittest.mock import AsyncMock

    from student_agent.trace import TraceWriter
    from student_agent.workflow import CoordinatorAgent, PaymentAgent, ShipmentAgent

    root = Path(__file__).resolve().parents[1]
    trace = TraceWriter(tmp_path / "trace.jsonl", Contracts(root / "contracts" / "schemas"))
    order_id = "a" * 32
    before = "2017-12-20T09:00:00-03:00"
    after = "2018-05-11T09:00:00-03:00"
    case = {
        "case_id": "L3B_CASE_001",
        "opened_at": "2018-01-01T09:00:00-03:00",
        "customer_unique_id_hint": "customer-one",
        "candidate_order_ids": [order_id],
        "customer_request": {
            "claimed_order_id": order_id,
            "claims": [{"claim_id": "claim-1", "topic": "late_delivery_logistics"}],
        },
    }
    responses = {
        "get_customer_history": {
            "orders": [
                {"order_id": order_id, "order_purchase_timestamp": after},
                {
                    "order_id": order_id,
                    "order_purchase_timestamp": before,
                    "order_delivered_carrier_date": "2017-12-22T09:00:00-03:00",
                    "order_delivered_customer_date": "2018-01-04T09:00:00-03:00",
                    "order_estimated_delivery_date": "2017-12-30T09:00:00-03:00",
                },
            ]
        },
        "get_order_payments": [
            {"order_id": order_id, "payment_sequential": "1", "payment_value": "89.00"},
            {"order_id": order_id, "payment_sequential": "1", "payment_value": "16.00"},
        ],
        "get_payment_timeline": {
            "events": [
                {"event_type": "captured", "event_at": after, "amount_brl": "89.00"},
                {"event_type": "captured", "event_at": before, "amount_brl": "16.00"},
            ]
        },
        "get_shipment_summary": {
            "order_id": order_id,
            "delivered_carrier_at": "2018-05-13T09:00:00-03:00",
            "delivered_customer_at": "2018-05-20T09:00:00-03:00",
            "estimated_delivery_at": "2018-05-21T09:00:00-03:00",
            "shipping_limits": [
                {"seller_id": "seller-1", "shipping_limit_at": "2018-05-14T09:00:00-03:00"},
                {"seller_id": "seller-1", "shipping_limit_at": "2017-12-23T09:00:00-03:00"},
            ],
            "events": [
                {
                    "event_type": "delivered_late",
                    "actor": "logistics_provider",
                    "event_at": "2018-01-04T09:00:00-03:00",
                }
            ],
        },
    }
    gateway = AsyncMock()

    async def call(tool_name: str, **kwargs):
        return {
            "evidence_ref": f"ev_{tool_name}_12345678901234567890",
            "data": responses[tool_name],
        }

    gateway.call.side_effect = call
    context = await CoordinatorAgent(trace).run(case, gateway)
    payment = await PaymentAgent(trace).run(context, gateway)
    shipment = await ShipmentAgent(trace).run(context, gateway)

    assert context["matched_customer_order"]["order_purchase_timestamp"] == before
    assert payment["captured_total_brl"] == 16.0
    assert payment["verdict"] == "reconciled"
    assert shipment["verdict"] == "logistics_delay"
    assert shipment["late_seller_ids"] == []
    assert "get_refund_timeline" not in [c.args[0] for c in gateway.call.call_args_list]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "topic",
    [
        "late_delivery_logistics",
        "valid_split_payment",
        "payment_mismatch",
        "duplicate_charge",
        "refund_pending",
        "refund_failed",
        "unsupported_claim",
        "canceled_order_paid",
        "unavailable_order_paid",
        "late_delivery_seller",
    ],
)
async def test_evidence_plan_stays_within_six_calls(topic: str, tmp_path: Path) -> None:
    from unittest.mock import AsyncMock

    from student_agent.trace import TraceWriter
    from student_agent.workflow import solve_case

    root = Path(__file__).resolve().parents[1]
    contracts = Contracts(root / "contracts" / "schemas")
    trace = TraceWriter(tmp_path / "trace.jsonl", contracts)
    order_id = "a" * 32
    before = "2017-12-20T09:00:00-03:00"
    after = "2018-05-11T09:00:00-03:00"
    case = {
        "case_id": "L3B_CASE_001",
        "opened_at": "2018-01-01T09:00:00-03:00",
        "customer_unique_id_hint": "customer-one",
        "candidate_order_ids": [order_id],
        "policy_version": "EC_POLICY_V2",
        "investigation_scope": {"include_product_context": True},
        "customer_request": {
            "claimed_order_id": order_id,
            "claims": [{"claim_id": "claim-1", "topic": topic}],
        },
    }
    data = {
        "get_customer_history": {
            "orders": [
                {"order_id": order_id, "order_purchase_timestamp": after},
                {"order_id": order_id, "order_purchase_timestamp": before},
            ]
        },
        "get_order_items": [
            {"item_id": "item-1", "seller_id": "seller-1", "product_id": "product-1"}
        ],
        "get_product_context": {"product_id": "product-1"},
        "get_order_payments": [
            {"order_id": order_id, "payment_sequential": "1", "payment_value": "89.00"},
            {"order_id": order_id, "payment_sequential": "1", "payment_value": "16.00"},
        ],
        "get_payment_timeline": {
            "events": [
                {"event_type": "captured", "event_at": after, "amount_brl": "89.00"},
                {"event_type": "captured", "event_at": before, "amount_brl": "16.00"},
            ]
        },
        "get_refund_timeline": {"events": []},
        "get_shipment_summary": {"order_id": order_id, "events": []},
        "get_policy": {"rules": {}},
    }
    gateway = AsyncMock()

    async def call(tool_name: str, **kwargs):
        return {
            "evidence_ref": f"ev_{tool_name}_12345678901234567890",
            "data": data[tool_name],
        }

    gateway.call.side_effect = call
    output = await solve_case(case, gateway, trace)

    assert gateway.call.await_count <= 6
    assert output["payment_analysis"]["captured_total_brl"] == 16.0
    assert set(output["evidence_refs"]) == {
        f"ev_{call.args[0]}_12345678901234567890" for call in gateway.call.call_args_list
    }
    contracts.validate_output(output, "six-call output")
