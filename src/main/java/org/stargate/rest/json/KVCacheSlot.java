package org.stargate.rest.json;

import com.fasterxml.jackson.databind.JsonNode;

public class KVCacheSlot {
    private String key;
    public JsonNode value;
    private String keyspace;
    private String table;
    private KVDataType valueType;
    private boolean used;

    public KVCacheSlot(String key, JsonNode value, String keyspace, String table, KVDataType kvDataType, boolean used) {
        this.key = key;
        this.value = value;
        this.keyspace = keyspace;
        this.table = table;
        this.valueType = kvDataType;
        this.used = used;
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
        
    public boolean isUsed() {
        return this.used;
    }

    public void setUsed(boolean used) {
        this.used = used;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public void setTable(String table) {
        this.table = table;
    }

    @Override
    public String toString() {
        return "KVCacheSlot{" +
            "key='" + key + '\'' +
            ", value=" + value +
            ", keyspace='" + keyspace + '\'' +
            ", table='" + table + '\'' +
            ", valueType=" + valueType +
            ", used=" + used +
            '}';
    }

}
