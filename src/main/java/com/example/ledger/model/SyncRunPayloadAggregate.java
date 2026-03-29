package com.example.ledger.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SyncRunPayloadAggregate {

    private Map<String, SyncRunPayloadRecord> payloadsById = new LinkedHashMap<>();

    public SyncRunPayloadAggregate() {
    }

    public Map<String, SyncRunPayloadRecord> getPayloadsById() {
        return payloadsById;
    }

    public void setPayloadsById(Map<String, SyncRunPayloadRecord> payloadsById) {
        this.payloadsById = payloadsById == null ? new LinkedHashMap<>() : new LinkedHashMap<>(payloadsById);
    }

    public SyncRunPayloadAggregate apply(SyncRunPayloadChange change) {
        if (change == null || change.syncRunPayloadId() == null) {
            return this;
        }
        if (change.deleted()) {
            payloadsById.remove(change.syncRunPayloadId());
        } else if (change.payload() != null) {
            payloadsById.put(change.syncRunPayloadId(), change.payload());
        }
        return this;
    }

    public List<SyncRunPayloadRecord> payloads() {
        return new ArrayList<>(payloadsById.values());
    }
}

