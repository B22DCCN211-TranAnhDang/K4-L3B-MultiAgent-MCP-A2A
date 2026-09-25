# L3B Architecture Record

Tài liệu thiết kế hệ thống Multi-Agent điều tra khiếu nại thương mại điện tử (L3B).

## 1. System overview

Hệ thống hoạt động theo mô hình phối hợp đa tác tử (Agent-to-Agent - A2A) phân cấp gồm Coordinator/Router, 3 Specialist Agents chạy song song/theo luồng, Policy Agent tổng hợp chính sách và Verifier Agent thẩm định dữ liệu cuối cùng.

```text
                          ┌──────────────────────────┐
                          │   Coordinator / Router   │
                          └─────────────┬────────────┘
                                        │ (Handoff)
         ┌──────────────────────────────┼──────────────────────────────┐
         ▼                              ▼                              ▼
┌──────────────────┐           ┌──────────────────┐           ┌──────────────────┐
│ Order/Item Agent │           │  Payment Agent   │           │  Shipment Agent  │
└────────┬─────────┘           └────────┬─────────┘           └────────┬─────────┘
         │                              │                              │
         └──────────────────────────────┼──────────────────────────────┘
                                        │ (MCP Evidence Collector)
                                        ▼
                               ┌──────────────────┐
                               │   Policy Agent   │
                               └────────┬─────────┘
                                        │
                                        ▼
                               ┌──────────────────┐
                               │  Verifier Agent  │
                               └────────┬─────────┘
                                        │ (Validated Output)
                                        ▼
                                   [END OUTPUT]
```

## 2. Agent ownership

| Actor | Input | Trách nhiệm | Tool permission | Output/handoff |
| --- | --- | --- | --- | --- |
| Coordinator / Router | Case JSON (`case_id`, `customer_id`, `candidates`, `claim`) | Tiếp nhận case, giải quyết entity (Entity Resolution: xếp hạng/bác bỏ order candidates), phân chia công việc cho các Specialist Agents. | `get_customer_history` | `task_assigned` & `handoff` payload chứa resolved order IDs |
| Order/Item Agent | Handoff context từ Coordinator | Điều tra chi tiết đơn hàng, danh sách item, thông tin sản phẩm và người bán. Xác định `affected_entities` (order, item, seller). | `get_order`, `get_order_items`, `get_product_context` | Kết quả phân tích Order/Item & list evidence refs |
| Payment Agent | Handoff context từ Coordinator | Phân tích dòng tiền, lịch sử thanh toán, hoàn tiền. Xác định `payment_analysis` (`verdict`, tổng tiền captured/refunded/refundable BRL). | `get_order_payments`, `get_payment_timeline`, `get_refund_timeline` | Kết quả phân tích Payment & list evidence refs |
| Shipment Agent | Handoff context từ Coordinator | Phân tích hành trình vận chuyển, kiểm tra trễ hạn seller vs trễ hạn logistics. Xác định `shipment_analysis` (`verdict`, late sellers). | `get_shipment_summary`, `get_sellers` | Kết quả phân tích Shipment & list evidence refs |
| Policy Agent | Kết quả tổng hợp từ 3 Specialist Agents | Đánh giá vi phạm chính sách sàn, xác định `primary_issue`, `secondary_issues`, `root_cause_analysis`, `financial_resolution` & `resolution_actions`. | `get_policy` | Dự thảo Case Output JSON & `policy_decided` trace |
| Verifier Agent | Dự thảo Case Output từ Policy Agent | Kiểm tra tính tuân thủ Schema (`l3b-output-v2.schema.json`), kiểm tra tính nhất quán dữ liệu, tính toàn vẹn evidence_refs và calibration confidence. | Không (Pure Validator) | Validated Output JSON (`case_finalized`) |

## 3. Entity resolution và A2A protocol

- **Candidate Resolution**: Coordinator phân tích danh sách `candidates` từ case. Sử dụng `get_customer_history` để khớp đơn hàng thực tế của khách hàng. Chọn ra `resolved_order_ids` và loại bỏ `rejected_candidates` có căn cứ.
- **A2A Handoff**: Chuyển đổi trạng thái qua các sự kiện trace có định dạng chuẩn: `case_received` → `task_assigned` → `handoff` (đến Order, Payment, Shipment) → `policy_decided` → `verification_completed` → `case_finalized`.
- **Trace Context**: Mọi handoff và tiêu thụ tool đều phát ra event `tool_result_consumed` ghi kèm `evidence_refs` tương ứng.

## 4. Evidence và conflict lifecycle

- Mọi phản hồi từ MCP Gateway được kiểm tra theo schema `mcp-evidence-response-v1.schema.json`.
- `evidence_ref` được thu thập chính xác từ kết quả MCP call, tuyệt đối không tự tạo hoặc tái sử dụng giữa các case khác nhau.
- Các mâu thuẫn dữ liệu giữa các nguồn (VD: thời gian giao hàng trên vận đơn vs thông báo khách hàng) được ghi nhận trong `data_conflicts`.

## 5. Failure and efficiency policy

| Failure | Retry budget | Fallback | Trace event/code |
| --- | ---: | --- | --- |
| MCP timeout / network error | 2 retries | `insufficient_evidence` verdict | `tool_result_consumed` với fallback status |
| Entity ambiguous / not found | 1 retry | Chuyển status `ambiguous` / `not_found` | `policy_decided` với fallback resolution |
| Source conflict | 0 retries | Ghi nhận vào `data_conflicts` & chọn nguồn ưu tiên theo Policy | `policy_decided` |

- **Caching & Efficiency**: Dữ liệu MCP thu được từ từng call được lưu cache trong bộ nhớ phạm vi của từng case để tránh gọi lặp lại cùng 1 tool với tham số giống nhau.

## 6. Verification invariants

Trước khi xuất `END OUTPUT`, Verifier Agent phải khẳng định các điều kiện sau:
1. `schema_version` bằng `"day09-l3b-output-v2"`.
2. Đầu ra khớp hoàn toàn với `contracts/schemas/l3b-output-v2.schema.json`.
3. Mọi `evidence_ref` xuất hiện trong output đều đã được cấp bởi MCP trong cùng `case_id`.
4. Các giá trị tài chính (`captured_total_brl`, `refunded_total_brl`, `recommended_refund_brl`) là số không âm.
5. Danh sách `rejected_candidates` và `resolved_order_ids` không trùng lặp.

## 7. Reproducibility

- Python 3.11+
- Dependencies: `mcp`, `jsonschema`, `referencing`, `pydantic`, `httpx2`
- Lệnh chạy: `day09 run`
- Lệnh kiểm tra: `day09 validate`
- Lệnh đóng gói: `day09 package --output dist/submission.zip`

