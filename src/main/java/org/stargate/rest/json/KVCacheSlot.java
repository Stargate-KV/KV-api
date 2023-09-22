package org.stargate.rest.json;

import com.fasterxml.jackson.databind.JsonNode;

public class KVCacheSlot {
    private String key;
    public JsonNode value;
    private String keyspace;
    private String table;
    private KVDataType valueType;
    private boolean dirty;
    private boolean tombstone;

    public KVCacheSlot(String key, JsonNode value, String keyspace, String table, KVDataType kvDataType, boolean dirty, boolean tombstone) {
        this.key = key;
        this.value = value;
        this.keyspace = keyspace;
        this.table = table;
        this.valueType = kvDataType;
        this.dirty = false;
        this.tombstone = false;
    }
    
    public JsonNode getValue() {
        return this.value;
    }

    public void setValue(JsonNode value) {
        this.value = value;
    }

    public KVDataType getValueType() {
        return this.valueType;
    }

    public void setValueType(KVDataType kvDataType) {
        this.valueType = kvDataType;
    }
        
    public boolean getDirty() {
        return this.dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }
    
    public boolean getTombstone() {
        return this.tombstone;
    }

    public void setTombstone(boolean tombstone) {
        this.tombstone = tombstone;
    }
}
