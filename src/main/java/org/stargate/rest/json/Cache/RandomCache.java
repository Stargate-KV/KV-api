package org.stargate.rest.json.Cache;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * RandomCache - A cache implementation with a simple random eviction policy.
 * 
 * Design:
 * 1. Uses a combination of keyspace, table, and key as a unique hash value for storing in the cache.
 * 2. Utilizes an array of cacheSlots to store key-value pairs.
 * 3. Maintains a map for hash value to index mapping.
 * 4. Implements simple random eviction when the cache is full.
 */

public class RandomCache {

  private final int maxSize; // Maximum size of the cache
  private final Map<String, Integer> hashToIndexMap; // Map to store hash to index mapping
  private final List<RandomCacheSlot> cacheSlots; // Array of cache slots
  private final List<Lock> locks; // Lock for each slot
  private final Lock sizeLock; // Lock for managing the size of the cache
  private int size; // Current size of the cache
  private Random rand; // Random number generator for eviction policy

  // Cache hit and read statistics
  long hitCount = 0;
  long totalRead = 0;
  
  /**
   * Constructor for RandomCache.
   *
   * @param maxSize The maximum size of the cache.
   */
  public RandomCache(int maxSize) {
    this.maxSize = maxSize;
    this.hashToIndexMap = new ConcurrentHashMap<>();
    // create an array of cacheSlots with size maxSize
    this.cacheSlots =
        IntStream.range(0, maxSize)
            .mapToObj(i -> new RandomCacheSlot(null, null))
            .collect(Collectors.toList());

    this.locks =
        IntStream.range(0, maxSize)
            .mapToObj(i -> new ReentrantLock())
            .collect(Collectors.toList());
    this.sizeLock = new ReentrantLock(); // lock for size
    this.size = 0;
    this.rand = new Random();
  }

  /**
   * Retrieves a value from the cache.
   *
   * @param key The key whose associated value is to be returned.
   * @param keyspace The keyspace of the key.
   * @param table The table of the key.
   * @return JsonNode The value associated with the specified key, or null if no value is found.
   */
  public JsonNode get(String key, String keyspace, String table) { // get function
    totalRead++;
    String hashkey = _computeHash(key, keyspace, table);
    int index = hashToIndexMap.getOrDefault(hashkey, -1);
    if (index == -1) {
      return null;
    }

    locks.get(index).lock();
    JsonNode result = cacheSlots.get(index).getValue();
    locks.get(index).unlock();
    hitCount++;
    return result;
  }

  /**
   * Deletes a key from the cache.
   *
   * @param key The key to be deleted.
   * @param keyspace The keyspace of the key.
   * @param table The table of the key.
   * @return boolean True if the key was deleted, false otherwise.
   */  
  public boolean delete(String key, String keyspace, String table) {
    String hashkey = _computeHash(key, keyspace, table);
    int index = hashToIndexMap.getOrDefault(hashkey, -1);
    
    if (index == -1) {
      return false;
    }
        
    sizeLock.lock();
    _delete(index);
    sizeLock.unlock();
    return true;
  }

  /**
   * Puts a key-value pair into the cache.
   *
   * @param key The key with which the specified value is to be associated.
   * @param value The value to be associated with the specified key.
   * @param keyspace The keyspace of the key.
   * @param table The table of the key.
   */
  public void put(String key, JsonNode value, String keyspace, String table) {
    String hashkey = _computeHash(key, keyspace, table);
    // if cache is full, randomly evict one key
    sizeLock.lock();
    if (_isFull()) {
      // randomly select a key to evict
        int index = rand.nextInt(size);
        _delete(index);
    }
    // add new key value pair in size
    cacheSlots.set(size, new RandomCacheSlot(hashkey, value));
    hashToIndexMap.put(hashkey, size++);
    sizeLock.unlock();
  }

  /**
   * Retrieves information about the current cache, such as its size, eviction policy, and hit ratio.
   *
   * @return String Information about the cache.
   */
  public String getCacheInfo() {
    return "Random Cache: eviction policy: Random, maxSlots: "
            + maxSize
            + ", current size: "
            + size
            + ", hit ratio:"
            + ", hit ratio: "
            + String.format("%.2f", (double) hitCount / totalRead * 100) + "%";
  }

  // ==================== Helper Functions ====================
  /**
   * Computes a unique hash value using keyspace, table, and key.
   *
   * @param key The key for the cache entry.
   * @param keyspace The keyspace for the cache entry.
   * @param table The table for the cache entry.
   * @return A string representing the hash value.
   */
  private String _computeHash(String key, String keyspace, String table) {
    // use keyspaces%table%key as hash value to store in cache
    return keyspace + "%" + table + "%" + key;
  }

  /**
   * Internal method to delete a cache entry at a given index.
   *
   * @param index The index of the cache entry to be deleted.
   */
  private void _delete(int index) {
    locks.get(index).lock();

    String hashkey = cacheSlots.get(index).getHashKey();
    hashToIndexMap.remove(hashkey);
    size--;
    // if index is not the last element, swap the last element to index
    if (index != size) {
      locks.get(size).lock();
      RandomCacheSlot lastElement = cacheSlots.get(size);
      cacheSlots.set(index, lastElement);
      String lastHashkey = lastElement.getHashKey();
      hashToIndexMap.put(lastHashkey, index);
      locks.get(size).unlock();
    }
    
    locks.get(index).unlock();
  }

  /**
   * Checks if the cache is full.
   *
   * @return boolean True if the cache is full, false otherwise.
   */
  private boolean _isFull() {
    return size == maxSize;
  }
}
