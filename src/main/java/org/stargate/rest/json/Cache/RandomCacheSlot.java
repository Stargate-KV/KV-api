package org.stargate.rest.json.Cache;

import org.stargate.rest.json.KVDataType;

import com.fasterxml.jackson.databind.JsonNode;

public class RandomCacheSlot {
  private String hashKey;
  private JsonNode value;
  private KVDataType valueType;

  public RandomCacheSlot(String hashKey, JsonNode value) {
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
