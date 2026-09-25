from __future__ import annotations

import logging
from typing import Any

from .mcp_gateway import EvidenceGateway
from .trace import TraceWriter

logger = logging.getLogger(__name__)

MAX_IDS = 20
MAX_EVIDENCE_REFS = 30


def _unique(values: list[str], limit: int = MAX_IDS) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, str) or not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
        if len(result) >= limit:
            break
    return result


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _nested_strings(value: Any, keys: set[str]) -> list[str]:
    found: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key in keys:
                if isinstance(item, str):
                    found.append(item)
                elif isinstance(item, list):
                    found.extend(str(entry) for entry in item if entry is not None)
            found.extend(_nested_strings(item, keys))
    elif isinstance(value, list):
        for item in value:
            found.extend(_nested_strings(item, keys))
    return found


def _nested_numbers(value: Any, keys: set[str]) -> list[float]:
    found: list[float] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key in keys and isinstance(item, int | float):
                found.append(float(item))
            found.extend(_nested_numbers(item, keys))
    elif isinstance(value, list):
        for item in value:
            found.extend(_nested_numbers(item, keys))
    return found


def _case_order_candidates(case: dict[str, Any]) -> list[str]:
    request = case.get("customer_request") if isinstance(case.get("customer_request"), dict) else {}
    candidates = [
        *_as_list(case.get("order_id")),
        *_as_list(case.get("candidate_order_ids")),
        *_as_list(case.get("candidates")),
        *_as_list(request.get("claimed_order_id")),
    ]
    return _unique([str(candidate) for candidate in candidates if candidate is not None])


def _case_customer_hint(case: dict[str, Any]) -> str | None:
    value = (
        case.get("customer_unique_id")
        or case.get("customer_unique_id_hint")
        or case.get("customer_id")
    )
    return str(value) if value is not None else None


def _claim_topics(case: dict[str, Any]) -> list[str]:
    request = case.get("customer_request") if isinstance(case.get("customer_request"), dict) else {}
    claims = request.get("claims") if isinstance(request.get("claims"), list) else []
    return _unique([str(claim.get("topic")) for claim in claims if isinstance(claim, dict)])


async def _consume_evidence(
    gateway: EvidenceGateway,
    trace: TraceWriter,
    *,
    case_id: str,
    actor: str,
    tool_name: str,
    **arguments: str,
) -> dict[str, Any] | None:
    """Call one MCP tool and trace the evidence ref if the call succeeds."""
    try:
        evidence = await gateway.call(tool_name, case_id=case_id, **arguments)
    except (RuntimeError, ValueError, TypeError) as exc:
        logger.info("MCP tool %s failed for %s: %s", tool_name, case_id, exc)
        return None

    evidence_ref = evidence.get("evidence_ref")
    if not isinstance(evidence_ref, str):
        return None

    trace.emit(
        case_id=case_id,
        event_type="tool_result_consumed",
        actor=actor,
        tool_name=tool_name,
        evidence_refs=[evidence_ref],
    )
    return evidence


class CoordinatorAgent:
    """Coordinator / Router Agent."""

    def __init__(self, trace: TraceWriter) -> None:
        self.trace = trace

    async def run(self, case: dict[str, Any], gateway: EvidenceGateway) -> dict[str, Any]:
        case_id = case["case_id"]
        order_candidates = _case_order_candidates(case)
        customer_unique_id = _case_customer_hint(case)
        evidence_refs: list[str] = []
        customer_payload: Any = {}

        self.trace.emit(
            case_id=case_id,
            event_type="case_received",
            actor="coordinator",
            attributes={
                "candidate_order_count": len(order_candidates),
                "has_customer_hint": customer_unique_id is not None,
            },
        )

        if customer_unique_id:
            customer_evidence = await _consume_evidence(
                gateway,
                self.trace,
                case_id=case_id,
                actor="entity-agent",
                tool_name="get_customer_history",
                customer_unique_id=customer_unique_id,
            )
            if customer_evidence:
                evidence_refs.append(customer_evidence["evidence_ref"])
                customer_payload = customer_evidence.get("data", {})

        customer_orders = _nested_strings(customer_payload, {"order_id", "order_ids"})
        resolved_order_ids = [
            candidate for candidate in order_candidates if candidate in customer_orders
        ]
        if not resolved_order_ids and order_candidates:
            resolved_order_ids = [order_candidates[0]]
        rejected_candidates = [
            candidate for candidate in order_candidates if candidate not in resolved_order_ids
        ]

        self.trace.emit(
            case_id=case_id,
            event_type="task_assigned",
            actor="coordinator",
            target="specialist-agents",
            attributes={"resolved_orders_count": len(resolved_order_ids)},
        )
        for specialist in ("order-agent", "payment-agent", "shipment-agent"):
            self.trace.emit(
                case_id=case_id,
                event_type="handoff",
                actor="coordinator",
                target=specialist,
                decision_code="ROUTED_TO_SPECIALIST",
            )

        related_order_ids = _unique([*resolved_order_ids, *customer_orders])
        return {
            "case_id": case_id,
            "case": case,
            "resolved_order_ids": _unique(resolved_order_ids),
            "rejected_candidates": _unique(rejected_candidates),
            "customer_unique_id": customer_unique_id,
            "related_order_ids": related_order_ids,
            "evidence_refs": _unique(evidence_refs, MAX_EVIDENCE_REFS),
        }


class OrderItemAgent:
    """Order/Item Specialist Agent."""

    def __init__(self, trace: TraceWriter) -> None:
        self.trace = trace

    async def run(self, context: dict[str, Any], gateway: EvidenceGateway) -> dict[str, Any]:
        case_id = context["case_id"]
        evidence_refs: list[str] = []
        item_ids: list[str] = []
        seller_ids: list[str] = []
        product_ids: list[str] = []
        order_payloads: list[Any] = []
        item_payloads: list[Any] = []

        for order_id in context["resolved_order_ids"]:
            order_evidence = await _consume_evidence(
                gateway,
                self.trace,
                case_id=case_id,
                actor="order-agent",
                tool_name="get_order",
                order_id=order_id,
            )
            if order_evidence:
                evidence_refs.append(order_evidence["evidence_ref"])
                order_payloads.append(order_evidence.get("data", {}))

            items_evidence = await _consume_evidence(
                gateway,
                self.trace,
                case_id=case_id,
                actor="order-agent",
                tool_name="get_order_items",
                order_id=order_id,
            )
            if items_evidence:
                evidence_refs.append(items_evidence["evidence_ref"])
                item_payloads.append(items_evidence.get("data", {}))

        item_ids = _unique(_nested_strings(item_payloads, {"item_id", "order_item_id"}))
        seller_ids = _unique(_nested_strings(item_payloads, {"seller_id", "seller_ids"}))
        product_ids = _unique(_nested_strings(item_payloads, {"product_id", "product_ids"}))

        for product_id in product_ids[:5]:
            product_evidence = await _consume_evidence(
                gateway,
                self.trace,
                case_id=case_id,
                actor="order-agent",
                tool_name="get_product_context",
                product_id=product_id,
            )
            if product_evidence:
                evidence_refs.append(product_evidence["evidence_ref"])

        for seller_id in seller_ids[:5]:
            seller_evidence = await _consume_evidence(
                gateway,
                self.trace,
                case_id=case_id,
                actor="order-agent",
                tool_name="get_sellers",
                seller_id=seller_id,
            )
            if seller_evidence:
                evidence_refs.append(seller_evidence["evidence_ref"])

        status_text = " ".join(_nested_strings(order_payloads, {"status", "order_status"})).lower()
        return {
            "item_ids": item_ids,
            "seller_ids": seller_ids,
            "product_ids": product_ids,
            "order_status_text": status_text,
            "evidence_refs": _unique(evidence_refs, MAX_EVIDENCE_REFS),
        }


class PaymentAgent:
    """Payment Specialist Agent."""

    def __init__(self, trace: TraceWriter) -> None:
        self.trace = trace

    async def run(self, context: dict[str, Any], gateway: EvidenceGateway) -> dict[str, Any]:
        case_id = context["case_id"]
        evidence_refs: list[str] = []
        payloads: list[Any] = []

        for order_id in context["resolved_order_ids"]:
            for tool_name in (
                "get_order_payments",
                "get_payment_timeline",
                "get_refund_timeline",
            ):
                evidence = await _consume_evidence(
                    gateway,
                    self.trace,
                    case_id=case_id,
                    actor="payment-agent",
                    tool_name=tool_name,
                    order_id=order_id,
                )
                if evidence:
                    evidence_refs.append(evidence["evidence_ref"])
                    payloads.append(evidence.get("data", {}))

        payment_refs = _unique(
            _nested_strings(payloads, {"payment_ref", "payment_reference", "transaction_id"})
        )
        captured_values = _nested_numbers(
            payloads,
            {"captured_total_brl", "paid_amount_brl", "payment_value", "amount_brl"},
        )
        refunded_values = _nested_numbers(
            payloads,
            {"refunded_total_brl", "refund_amount_brl", "refunded_amount_brl"},
        )
        captured_total = round(sum(captured_values), 2) if captured_values else None
        refunded_total = round(sum(refunded_values), 2) if refunded_values else None
        refundable_total = None
        if captured_total is not None:
            refundable_total = round(max(captured_total - (refunded_total or 0.0), 0.0), 2)

        status_text = " ".join(_nested_strings(payloads, {"status", "payment_status"})).lower()
        verdict = "insufficient_evidence"
        if payloads:
            if "duplicate" in status_text:
                verdict = "duplicate_capture"
            elif "capture_mismatch" in status_text or "mismatch" in status_text:
                verdict = "capture_mismatch"
            elif "refund" in status_text and "failed" in status_text:
                verdict = "refund_failed"
            elif "refund" in status_text and "pending" in status_text:
                verdict = "refund_pending"
            elif refunded_total and refundable_total == 0:
                verdict = "refunded"
            else:
                verdict = "reconciled"

        return {
            "verdict": verdict,
            "payment_references": payment_refs,
            "captured_total_brl": captured_total,
            "refunded_total_brl": refunded_total,
            "refundable_total_brl": refundable_total,
            "evidence_refs": _unique(evidence_refs, MAX_EVIDENCE_REFS),
        }


class ShipmentAgent:
    """Shipment Specialist Agent."""

    def __init__(self, trace: TraceWriter) -> None:
        self.trace = trace

    async def run(self, context: dict[str, Any], gateway: EvidenceGateway) -> dict[str, Any]:
        case_id = context["case_id"]
        evidence_refs: list[str] = []
        payloads: list[Any] = []

        for order_id in context["resolved_order_ids"]:
            evidence = await _consume_evidence(
                gateway,
                self.trace,
                case_id=case_id,
                actor="shipment-agent",
                tool_name="get_shipment_summary",
                order_id=order_id,
            )
            if evidence:
                evidence_refs.append(evidence["evidence_ref"])
                payloads.append(evidence.get("data", {}))

        shipment_ids = _unique(_nested_strings(payloads, {"shipment_id", "tracking_id"}))
        late_seller_ids = _unique(_nested_strings(payloads, {"late_seller_id", "seller_id"}))
        status_text = " ".join(_nested_strings(payloads, {"status", "shipment_status"})).lower()
        verdict = "insufficient_evidence"
        if payloads:
            if "lost" in status_text:
                verdict = "lost"
            elif "return" in status_text:
                verdict = "returned"
            elif "seller" in status_text and ("late" in status_text or "delay" in status_text):
                verdict = "seller_delay"
            elif "late" in status_text or "delay" in status_text:
                verdict = "logistics_delay"
            else:
                verdict = "on_time"

        return {
            "verdict": verdict,
            "shipment_ids": shipment_ids,
            "late_seller_ids": late_seller_ids,
            "timeline_complete": bool(payloads),
            "evidence_refs": _unique(evidence_refs, MAX_EVIDENCE_REFS),
        }


class PolicyAgent:
    """Policy Agent."""

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
        policy_evidence = await _consume_evidence(
            gateway,
            self.trace,
            case_id=case_id,
            actor="policy-agent",
            tool_name="get_policy",
            policy_version=str(context["case"].get("policy_version", "EC_POLICY_V2")),
        )

        evidence_refs = _unique(
            [
                *context["evidence_refs"],
                *order_res["evidence_refs"],
                *payment_res["evidence_refs"],
                *shipment_res["evidence_refs"],
                *([policy_evidence["evidence_ref"]] if policy_evidence else []),
            ],
            MAX_EVIDENCE_REFS,
        )
        primary_issue = _primary_issue(context, order_res, payment_res, shipment_res, evidence_refs)
        case_status = (
            "no_action"
            if primary_issue in {"unsupported_claim", "valid_split_payment"}
            else "action_required"
        )
        if primary_issue == "insufficient_evidence":
            case_status = "needs_investigation"

        recommended_refund = payment_res["refundable_total_brl"] or 0.0
        self.trace.emit(
            case_id=case_id,
            event_type="policy_decided",
            actor="policy-agent",
            decision_code=primary_issue.upper(),
            evidence_refs=evidence_refs[:20] or None,
        )

        return {
            "schema_version": "day09-l3b-output-v2",
            "case_id": case_id,
            "assessment": {
                "primary_issue": primary_issue,
                "secondary_issues": _secondary_issues(context, payment_res, shipment_res),
                "case_status": case_status,
                "confidence": _confidence(context, evidence_refs),
            },
            "affected_entities": {
                "order_ids": context["resolved_order_ids"],
                "item_ids": order_res["item_ids"],
                "seller_ids": order_res["seller_ids"],
                "payment_references": payment_res["payment_references"],
                "shipment_ids": shipment_res["shipment_ids"],
            },
            "entity_resolution": {
                "status": _entity_status(context),
                "resolved_order_ids": context["resolved_order_ids"],
                "rejected_candidates": context["rejected_candidates"],
                "confidence": 0.9 if context["resolved_order_ids"] else 0.2,
            },
            "customer_context": {
                "customer_unique_id": context["customer_unique_id"],
                "related_order_ids": context["related_order_ids"],
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
            "root_cause_analysis": _root_cause(primary_issue, order_res, shipment_res),
            "evidence_refs": evidence_refs,
            "data_conflicts": [],
            "financial_resolution": {
                "currency": "BRL",
                "recommended_refund_brl": recommended_refund,
                "refund_lines": (
                    [
                        {
                            "reason_code": primary_issue.upper(),
                            "amount_brl": recommended_refund,
                            "entity_id": context["resolved_order_ids"][0]
                            if context["resolved_order_ids"]
                            else None,
                        }
                    ]
                    if recommended_refund > 0
                    else []
                ),
            },
            "resolution_actions": _resolution_actions(primary_issue, recommended_refund),
        }


class VerifierAgent:
    """Verifier Agent."""

    def __init__(self, trace: TraceWriter) -> None:
        self.trace = trace

    async def run(self, output: dict[str, Any]) -> dict[str, Any]:
        case_id = output["case_id"]
        output["evidence_refs"] = _unique(output["evidence_refs"], MAX_EVIDENCE_REFS)
        for key in ("order_ids", "item_ids", "seller_ids", "payment_references", "shipment_ids"):
            output["affected_entities"][key] = _unique(output["affected_entities"][key])
        output["entity_resolution"]["resolved_order_ids"] = _unique(
            output["entity_resolution"]["resolved_order_ids"]
        )
        output["entity_resolution"]["rejected_candidates"] = _unique(
            output["entity_resolution"]["rejected_candidates"]
        )
        self.trace.contracts.validate_output(output, f"case {case_id} output")
        self.trace.emit(
            case_id=case_id,
            event_type="verification_completed",
            actor="verifier-agent",
            decision_code="PASSED",
            evidence_refs=output["evidence_refs"][:20] or None,
        )
        self.trace.emit(
            case_id=case_id,
            event_type="case_finalized",
            actor="verifier-agent",
            decision_code="FINALIZED",
        )
        return output


def _entity_status(context: dict[str, Any]) -> str:
    if not context["resolved_order_ids"]:
        return "not_found"
    if len(context["resolved_order_ids"]) > 1:
        return "ambiguous"
    return "resolved"


def _primary_issue(
    context: dict[str, Any],
    order_res: dict[str, Any],
    payment_res: dict[str, Any],
    shipment_res: dict[str, Any],
    evidence_refs: list[str],
) -> str:
    topics = set(_claim_topics(context["case"]))
    if not evidence_refs:
        return "insufficient_evidence"
    if "canceled" in order_res["order_status_text"] and payment_res["captured_total_brl"]:
        return "canceled_order_paid"
    if "unavailable" in order_res["order_status_text"] and payment_res["captured_total_brl"]:
        return "unavailable_order_paid"
    if shipment_res["verdict"] == "seller_delay":
        return "late_delivery_seller"
    if shipment_res["verdict"] == "logistics_delay":
        return "late_delivery_logistics"
    if payment_res["verdict"] == "duplicate_capture":
        return "duplicate_charge"
    if payment_res["verdict"] in {
        "capture_mismatch",
        "refund_pending",
        "refund_failed",
        "refunded",
        "insufficient_evidence",
    }:
        return payment_res["verdict"]
    if "valid_split_payment" in topics:
        return "valid_split_payment"
    return "unsupported_claim"


def _secondary_issues(
    context: dict[str, Any],
    payment_res: dict[str, Any],
    shipment_res: dict[str, Any],
) -> list[str]:
    issues = [topic for topic in _claim_topics(context["case"]) if topic != "requested_full_refund"]
    if payment_res["verdict"] not in {"reconciled", "insufficient_evidence"}:
        issues.append(payment_res["verdict"])
    if shipment_res["verdict"] not in {"on_time", "insufficient_evidence"}:
        issues.append(shipment_res["verdict"])
    return _unique(issues, 10)


def _confidence(context: dict[str, Any], evidence_refs: list[str]) -> float:
    if not evidence_refs:
        return 0.35
    if _entity_status(context) == "resolved":
        return 0.82
    if _entity_status(context) == "ambiguous":
        return 0.55
    return 0.4


def _root_cause(
    primary_issue: str,
    order_res: dict[str, Any],
    shipment_res: dict[str, Any],
) -> dict[str, Any]:
    cause_by_issue = {
        "late_delivery_seller": "SELLER_DELAY",
        "late_delivery_logistics": "LOGISTICS_DELAY",
        "duplicate_charge": "DUPLICATE_CAPTURE",
        "payment_mismatch": "PAYMENT_MISMATCH",
        "capture_mismatch": "PAYMENT_MISMATCH",
        "refund_pending": "REFUND_PENDING",
        "refund_failed": "REFUND_FAILED",
        "canceled_order_paid": "CANCELED_ORDER_PAID",
        "unavailable_order_paid": "UNAVAILABLE_ORDER_PAID",
        "valid_split_payment": "VALID_SPLIT_PAYMENT",
        "unsupported_claim": "UNSUPPORTED_CLAIM",
        "insufficient_evidence": "INSUFFICIENT_EVIDENCE",
    }
    party_type = "unknown"
    party_id = None
    if primary_issue == "late_delivery_seller":
        party_type = "seller"
        party_id = shipment_res["late_seller_ids"][0] if shipment_res["late_seller_ids"] else None
    elif primary_issue == "late_delivery_logistics":
        party_type = "logistics_provider"
    elif primary_issue in {"duplicate_charge", "payment_mismatch", "capture_mismatch"}:
        party_type = "payment_provider"
    elif primary_issue == "unsupported_claim":
        party_type = "customer"
    elif order_res["seller_ids"]:
        party_type = "seller"
        party_id = order_res["seller_ids"][0]
    return {
        "ranked_causes": [
            {"cause_code": cause_by_issue.get(primary_issue, "INSUFFICIENT_EVIDENCE"), "rank": 1}
        ],
        "responsible_parties": [{"party_type": party_type, "party_id": party_id}],
    }


def _resolution_actions(primary_issue: str, recommended_refund: float) -> list[str]:
    if primary_issue == "unsupported_claim":
        return ["close_case"]
    if primary_issue == "insufficient_evidence":
        return ["request_additional_review"]
    if recommended_refund > 0:
        return ["initiate_refund", "notify_customer"]
    return ["notify_customer"]


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

    context = await coordinator.run(case, gateway)
    order_res = await order_agent.run(context, gateway)
    payment_res = await payment_agent.run(context, gateway)
    shipment_res = await shipment_agent.run(context, gateway)
    candidate_output = await policy_agent.run(
        context,
        order_res,
        payment_res,
        shipment_res,
        gateway,
    )
    return await verifier_agent.run(candidate_output)
