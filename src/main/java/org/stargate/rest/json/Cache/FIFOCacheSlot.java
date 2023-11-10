package org.stargate.rest.json.Cache;

import org.stargate.rest.json.KVDataType;

import com.fasterxml.jackson.databind.JsonNode;

public class FIFOCacheSlot {
  private String key;
  public JsonNode value;
  private String keyspace;
  private String table;
  private KVDataType valueType;
  private boolean used;
  private int hashvalue;

  public FIFOCacheSlot(
      String key,
      JsonNode value,
      String keyspace,
      String table,
      KVDataType kvDataType,
      boolean used,
      int hashvalue) {
    this.key = key;
    this.value = value;
    this.keyspace = keyspace;
    this.table = table;
    this.valueType = kvDataType;
    this.used = used;
    this.hashvalue = hashvalue;
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

  public void setHashvalue(int hashvalue) {
    this.hashvalue = hashvalue;
  }

  public int getHashvalue() {
    return this.hashvalue;
  }

  @Override
  public String toString() {
    return "KVCacheSlot{"
        + "key='"
        + key
        + '\''
        + ", value="
        + value
        + ", keyspace='"
        + keyspace
        + '\''
        + ", table='"
        + table
        + '\''
        + ", valueType="
        + valueType
        + ", used="
        + used
        + ", hashvalue="
        + hashvalue
        + '}';
  }
}
