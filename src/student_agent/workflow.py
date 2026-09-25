from __future__ import annotations

import logging
from typing import Any

from .mcp_gateway import EvidenceGateway
from .trace import TraceWriter

logger = logging.getLogger(__name__)


class CoordinatorAgent:
    """Coordinator / Router Agent.

    Receives case input, performs entity resolution (candidate ranking/filtering),
    assigns tasks, and emits handoffs to specialist agents.
    """

    def __init__(self, trace: TraceWriter) -> None:
        self.trace = trace

    async def run(self, case: dict[str, Any], gateway: EvidenceGateway) -> dict[str, Any]:
        case_id = case["case_id"]

        # 1. Emit case_received
        self.trace.emit(
            case_id=case_id,
            event_type="case_received",
            actor="coordinator",
            attributes={"customer_id": str(case.get("customer_id")) if case.get("customer_id") is not None else None},
        )

        # 2. Entity Resolution
        candidates = case.get("candidates") or []
        resolved_order_ids: list[str] = []
        rejected_candidates: list[str] = []

        if case.get("order_id"):
            resolved_order_ids.append(str(case["order_id"]))
        elif candidates:
            resolved_order_ids.append(str(candidates[0]))
            rejected_candidates = [str(c) for c in candidates[1:]]

        customer_unique_id = case.get("customer_unique_id") or case.get("customer_id")
        if customer_unique_id is not None:
            customer_unique_id = str(customer_unique_id)

        # 3. Emit task_assigned & handoff to Specialists
        self.trace.emit(
            case_id=case_id,
            event_type="task_assigned",
            actor="coordinator",
            target="specialist-agents",
            attributes={"resolved_orders_count": len(resolved_order_ids)},
        )

        for specialist in ["order-agent", "payment-agent", "shipment-agent"]:
            self.trace.emit(
                case_id=case_id,
                event_type="handoff",
                actor="coordinator",
                target=specialist,
                decision_code="ROUTED_TO_SPECIALIST",
            )

        return {
            "case_id": case_id,
            "resolved_order_ids": resolved_order_ids,
            "rejected_candidates": rejected_candidates,
            "customer_unique_id": customer_unique_id,
            "case": case,
        }


class OrderItemAgent:
    """Order/Item Specialist Agent.

    Investigates order items, sellers, and product details using MCP tools.
    """

    def __init__(self, trace: TraceWriter) -> None:
        self.trace = trace

    async def run(self, context: dict[str, Any], gateway: EvidenceGateway) -> dict[str, Any]:
        case_id = context["case_id"]
        evidence_refs: list[str] = []
        item_ids: list[str] = []
        seller_ids: list[str] = []

        for order_id in context["resolved_order_ids"]:
            try:
                res = await gateway.call("get_order_items", case_id=case_id, order_id=order_id)
                ref = res.get("evidence_ref")
                if ref:
                    evidence_refs.append(ref)
                    self.trace.emit(
                        case_id=case_id,
                        event_type="tool_result_consumed",
                        actor="order-agent",
                        tool_name="get_order_items",
                        evidence_refs=[ref],
                    )
                items = res.get("data", {}).get("items", [])
                for item in items:
                    if "item_id" in item:
                        item_ids.append(str(item["item_id"]))
                    if "seller_id" in item:
                        seller_ids.append(str(item["seller_id"]))
            except Exception:
                pass

        return {
            "item_ids": sorted(set(item_ids)),
            "seller_ids": sorted(set(seller_ids)),
            "evidence_refs": evidence_refs,
        }


class PaymentAgent:
    """Payment Specialist Agent.

    Investigates payment references, capture/refund totals and verdicts using MCP tools.
    """

    def __init__(self, trace: TraceWriter) -> None:
        self.trace = trace

    async def run(self, context: dict[str, Any], gateway: EvidenceGateway) -> dict[str, Any]:
        case_id = context["case_id"]
        evidence_refs: list[str] = []
        payment_refs: list[str] = []
        captured_total = 0.0
        refunded_total = 0.0
        verdict = "reconciled"

        for order_id in context["resolved_order_ids"]:
            try:
                res = await gateway.call("get_order_payments", case_id=case_id, order_id=order_id)
                ref = res.get("evidence_ref")
                if ref:
                    evidence_refs.append(ref)
                    self.trace.emit(
                        case_id=case_id,
                        event_type="tool_result_consumed",
                        actor="payment-agent",
                        tool_name="get_order_payments",
                        evidence_refs=[ref],
                    )
                payments = res.get("data", {}).get("payments", [])
                for p in payments:
                    if "payment_ref" in p:
                        payment_refs.append(str(p["payment_ref"]))
                    captured_total += float(p.get("amount_brl", 0.0))
            except Exception:
                verdict = "insufficient_evidence"

        return {
            "verdict": verdict,
            "payment_references": sorted(set(payment_refs)),
            "captured_total_brl": captured_total,
            "refunded_total_brl": refunded_total,
            "refundable_total_brl": max(0.0, captured_total - refunded_total),
            "evidence_refs": evidence_refs,
        }


class ShipmentAgent:
    """Shipment Specialist Agent.

    Investigates shipment status, logistics delays, and seller dispatch timelines using MCP tools.
    """

    def __init__(self, trace: TraceWriter) -> None:
        self.trace = trace

    async def run(self, context: dict[str, Any], gateway: EvidenceGateway) -> dict[str, Any]:
        case_id = context["case_id"]
        evidence_refs: list[str] = []
        shipment_ids: list[str] = []
        late_sellers: list[str] = []
        verdict = "on_time"

        for order_id in context["resolved_order_ids"]:
            try:
                res = await gateway.call("get_shipment_summary", case_id=case_id, order_id=order_id)
                ref = res.get("evidence_ref")
                if ref:
                    evidence_refs.append(ref)
                    self.trace.emit(
                        case_id=case_id,
                        event_type="tool_result_consumed",
                        actor="shipment-agent",
                        tool_name="get_shipment_summary",
                        evidence_refs=[ref],
                    )
                data = res.get("data", {})
                if "shipment_id" in data:
                    shipment_ids.append(str(data["shipment_id"]))
                if data.get("status") == "delayed":
                    verdict = "logistics_delay"
            except Exception:
                verdict = "insufficient_evidence"

        return {
            "verdict": verdict,
            "shipment_ids": sorted(set(shipment_ids)),
            "late_seller_ids": late_sellers,
            "timeline_complete": True,
            "evidence_refs": evidence_refs,
        }


class PolicyAgent:
    """Policy Agent.

    Evaluates findings from specialist agents, determines root cause, primary issue,
    financial resolution, and resolution actions, and emits policy_decided.
    """

    def __init__(self, trace: TraceWriter) -> None:
        self.trace = trace

    async def run(
        self,
        context: dict[str, Any],
        order_res: dict[str, Any],
        payment_res: dict[str, Any],
        shipment_res: dict[str, Any],
        gateway: EvidenceGateway,
    ) -> dict[str, Any]:
        case_id = context["case_id"]
        all_evidence = sorted(
            set(order_res["evidence_refs"] + payment_res["evidence_refs"] + shipment_res["evidence_refs"])
        )

        primary_issue = "insufficient_evidence"
        if shipment_res["verdict"] == "logistics_delay":
            primary_issue = "late_delivery_logistics"
        elif payment_res["verdict"] == "reconciled":
            primary_issue = "unsupported_claim"

        self.trace.emit(
            case_id=case_id,
            event_type="policy_decided",
            actor="policy-agent",
            decision_code=primary_issue.upper(),
            evidence_refs=all_evidence if all_evidence else None,
        )

        output: dict[str, Any] = {
            "schema_version": "day09-l3b-output-v2",
            "case_id": case_id,
            "assessment": {
                "primary_issue": primary_issue,
                "secondary_issues": [],
                "case_status": "no_action" if primary_issue == "unsupported_claim" else "action_required",
                "confidence": 0.9 if all_evidence else 0.5,
            },
            "affected_entities": {
                "order_ids": context["resolved_order_ids"],
                "item_ids": order_res["item_ids"],
                "seller_ids": order_res["seller_ids"],
                "payment_references": payment_res["payment_references"],
                "shipment_ids": shipment_res["shipment_ids"],
            },
            "entity_resolution": {
                "status": "resolved" if context["resolved_order_ids"] else "not_found",
                "resolved_order_ids": context["resolved_order_ids"],
                "rejected_candidates": context["rejected_candidates"],
                "confidence": 0.95 if context["resolved_order_ids"] else 0.0,
            },
            "customer_context": {
                "customer_unique_id": context["customer_unique_id"],
                "related_order_ids": context["resolved_order_ids"],
            },
            "shipment_analysis": {
                "verdict": shipment_res["verdict"],
                "late_seller_ids": shipment_res["late_seller_ids"],
                "timeline_complete": shipment_res["timeline_complete"],
            },
            "payment_analysis": {
                "verdict": payment_res["verdict"],
                "captured_total_brl": payment_res["captured_total_brl"],
                "refunded_total_brl": payment_res["refunded_total_brl"],
                "refundable_total_brl": payment_res["refundable_total_brl"],
            },
            "root_cause_analysis": {
                "ranked_causes": [
                    {"cause_code": "LOGISTICS_DELAY" if primary_issue == "late_delivery_logistics" else "NO_FAULT", "rank": 1}
                ],
                "responsible_parties": [
                    {
                        "party_type": "logistics_provider" if primary_issue == "late_delivery_logistics" else "customer",
                        "party_id": None,
                    }
                ],
            },
            "evidence_refs": all_evidence,
            "data_conflicts": [],
            "financial_resolution": {
                "currency": "BRL",
                "recommended_refund_brl": 0.0,
                "refund_lines": [],
            },
            "resolution_actions": ["close_case" if primary_issue == "unsupported_claim" else "notify_customer"],
        }
        return output


class VerifierAgent:
    """Verifier Agent.

    Validates output payload against schema contracts, emits verification_completed
    and case_finalized trace events.
    """

    def __init__(self, trace: TraceWriter) -> None:
        self.trace = trace

    async def run(self, output: dict[str, Any]) -> dict[str, Any]:
        case_id = output["case_id"]

        # Validate with Contracts
        self.trace.contracts.validate_output(output, f"case {case_id} output")

        self.trace.emit(
            case_id=case_id,
            event_type="verification_completed",
            actor="verifier-agent",
            decision_code="PASSED",
        )

        self.trace.emit(
            case_id=case_id,
            event_type="case_finalized",
            actor="verifier-agent",
            decision_code="FINALIZED",
        )

        return output


async def solve_case(
    case: dict[str, Any], gateway: EvidenceGateway, trace: TraceWriter
) -> dict[str, Any]:
    """Execute the A2A multi-agent investigation workflow."""
    coordinator = CoordinatorAgent(trace)
    order_agent = OrderItemAgent(trace)
    payment_agent = PaymentAgent(trace)
    shipment_agent = ShipmentAgent(trace)
    policy_agent = PolicyAgent(trace)
    verifier_agent = VerifierAgent(trace)

    # 1. Coordinator: Router & Entity Resolution
    context = await coordinator.run(case, gateway)

    # 2. Specialist Agents (Order, Payment, Shipment)
    order_res = await order_agent.run(context, gateway)
    payment_res = await payment_agent.run(context, gateway)
    shipment_res = await shipment_agent.run(context, gateway)

    # 3. Policy Agent: Policy evaluation & output generation
    candidate_output = await policy_agent.run(context, order_res, payment_res, shipment_res, gateway)

    # 4. Verifier Agent: Contract validation & finalization
    final_output = await verifier_agent.run(candidate_output)

    return final_output
