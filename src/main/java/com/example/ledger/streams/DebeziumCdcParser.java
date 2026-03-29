package com.example.ledger.streams;

import com.example.ledger.model.SyncRunPayloadChange;
import com.example.ledger.model.SyncRunPayloadRecord;
import com.example.ledger.model.SyncRunRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class DebeziumCdcParser {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private DebeziumCdcParser() {
    }

    public static DebeziumChange<SyncRunRecord> parseSyncRunChange(String json) {
        JsonNode root = readTree(json);
        if (root == null) {
            return null;
        }

        String op = text(root, "op");
        JsonNode payloadNode = deleteOperation(op) ? root.path("before") : root.path("after");
        if (payloadNode.isMissingNode() || payloadNode.isNull()) {
            return null;
        }

        SyncRunRecord record = new SyncRunRecord(
                text(payloadNode, "sync_run_id"),
                text(payloadNode, "started_at"),
                text(payloadNode, "finished_at"),
                text(payloadNode, "status"),
                text(payloadNode, "created_at"),
                text(payloadNode, "created_by"),
                text(payloadNode, "updated_at"),
                text(payloadNode, "updated_by"),
                text(payloadNode, "sync_job_id"),
                text(payloadNode, "job_info")
        );
        return new DebeziumChange<>(record.syncRunId(), deleteOperation(op), record);
    }

    public static SyncRunPayloadChange parseSyncRunPayloadChange(String json) {
        JsonNode root = readTree(json);
        if (root == null) {
            return null;
        }

        String op = text(root, "op");
        JsonNode payloadNode = deleteOperation(op) ? root.path("before") : root.path("after");
        if (payloadNode.isMissingNode() || payloadNode.isNull()) {
            return null;
        }

        SyncRunPayloadRecord payload = new SyncRunPayloadRecord(
                text(payloadNode, "sync_run_payload_id"),
                text(payloadNode, "sync_run_id"),
                text(payloadNode, "transaction_id"),
                text(payloadNode, "order_number"),
                doubleValue(payloadNode, "order_amount"),
                text(payloadNode, "created_at"),
                text(payloadNode, "updated_at"),
                text(payloadNode, "venue_id"),
                text(payloadNode, "customer_id"),
                text(payloadNode, "line_items"),
                text(payloadNode, "order_date"),
                intValue(payloadNode, "total_items"),
                text(payloadNode, "payments"),
                text(payloadNode, "note"),
                text(payloadNode, "currency_code"),
                text(payloadNode, "currency_symbol"),
                text(payloadNode, "visit_alias")
        );
        return new SyncRunPayloadChange(payload.syncRunId(), payload.syncRunPayloadId(), payload, deleteOperation(op));
    }

    private static JsonNode readTree(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            return OBJECT_MAPPER.readTree(json);
        } catch (Exception ex) {
            return null;
        }
    }

    private static boolean deleteOperation(String op) {
        return "d".equals(op);
    }

    private static String text(JsonNode node, String fieldName) {
        JsonNode field = node.get(fieldName);
        return field == null || field.isNull() ? null : field.asText();
    }

    private static Double doubleValue(JsonNode node, String fieldName) {
        JsonNode field = node.get(fieldName);
        return field == null || field.isNull() ? null : field.asDouble();
    }

    private static Integer intValue(JsonNode node, String fieldName) {
        JsonNode field = node.get(fieldName);
        return field == null || field.isNull() ? null : field.asInt();
    }

    public record DebeziumChange<T>(String key, boolean deleted, T value) {
    }
}

