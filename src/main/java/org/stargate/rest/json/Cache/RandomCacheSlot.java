package org.stargate.rest.json.Cache;

import org.stargate.rest.json.KVDataType;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Class RandomCacheSlot - Represents a single slot in the RandomCache.
 * It encapsulates a key-value pair along with the hash key used for cache storage.
 */
public class RandomCacheSlot {
  // The hash key used for identifying the cache slot
  private String hashKey;

  // The value stored in the cache slot
  private JsonNode value;

  // The data type of the value (currently unused but can be utilized for type-specific operations)
  private KVDataType valueType;

  /**
   * Constructor for RandomCacheSlot.
   *
   * @param hashKey The hash key associated with this cache slot.
   * @param value The value to be stored in this cache slot.
   */
  public RandomCacheSlot(String hashKey, JsonNode value) {
    this.hashKey = hashKey;
    this.value = value;
  }

  /**
   * Retrieves the value stored in this cache slot.
   *
   * @return JsonNode The value stored in the cache slot.
   */
  public JsonNode getValue() {
    return this.value;
  }

  /**
   * Retrieves the hash key associated with this cache slot.
   *
   * @return String The hash key.
   */
  public String getHashKey() {
    return this.hashKey;
  }

  /**
   * Provides a string representation of the cache slot, including its hash key and value.
   *
   * @return String A string representation of the cache slot.
   */
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
