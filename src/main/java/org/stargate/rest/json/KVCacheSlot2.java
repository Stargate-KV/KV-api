package org.stargate.rest.json;

import com.fasterxml.jackson.databind.JsonNode;

public class KVCacheSlot2 {
  private String hashKey;
  private JsonNode value;
  private KVDataType valueType;

  public KVCacheSlot2(String hashKey, JsonNode value) {
    this.hashKey = hashKey;
    this.value = value;
  }

  public JsonNode getValue() {
    return this.value;
  }

  public String getHashKey() {
    return this.hashKey;
  }

  @Override
  public String toString() {
    return "KVCacheSlot{"
        + "hashKey='"
        + hashKey
        + '\''
        + ", value="
        + value
        + ", valueType="
        + valueType
        + '}';
  }
}
