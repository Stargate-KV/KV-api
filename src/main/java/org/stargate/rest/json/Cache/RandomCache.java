package org.stargate.rest.json.Cache;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
// import javax.enterprise.context.ApplicationScoped;

// import org.checkerframework.checker.units.qual.s;

/*
   ADVANED VERSION OF KVCache.java
   DESIGN:
   1. use keyspaces%table%key as hash value to store in cache
   2. use an array of cacheSlots to store the key value pairs
   3. use a map to store the hash value to index mapping
   4. simple random eviction
*/

// @ApplicationScoped
public class RandomCache {

  private final int maxSize;
  private final Map<String, Integer> hashToIndexMap;
  private final List<RandomCacheSlot> cacheSlots;
  // lock for each slot
  private final List<Lock> locks;
  private final Lock sizeLock;
  private int size;
  Random rand;

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

  // ==================== Helper Functions ====================
  private String computeHash(String key, String keyspace, String table) {
    // use keyspaces%table%key as hash value to store in cache
    return keyspace + "%" + table + "%" + key;
  }

  public JsonNode get(String key, String keyspace, String table) { // get function
    String hashkey = computeHash(key, keyspace, table);
    int index = hashToIndexMap.getOrDefault(hashkey, -1);
    if (index == -1) {
      return null;
    }

    locks.get(index).lock();
    JsonNode result = cacheSlots.get(index).getValue();
    locks.get(index).unlock();
    
    return result;
  }

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

  // return whether the key exists in cache
  public boolean delete(String key, String keyspace, String table) {
    String hashkey = computeHash(key, keyspace, table);
    int index = hashToIndexMap.getOrDefault(hashkey, -1);
    
    if (index == -1) {
      return false;
    }
        
    sizeLock.lock();
    _delete(index);
    sizeLock.unlock();
    return true;
  }

  private boolean isFull() {
    return size == maxSize;
  }

  public void put(String key, JsonNode value, String keyspace, String table) {
    String hashkey = computeHash(key, keyspace, table);
    // if cache is full, randomly evict one key
    sizeLock.lock();
    if (isFull()) {
      // randomly select a key to evict
        int index = rand.nextInt(size);
        _delete(index);
    }
    // add new key value pair in size
    cacheSlots.set(size, new RandomCacheSlot(hashkey, value));
    hashToIndexMap.put(hashkey, size++);
    sizeLock.unlock();
  }

  public String getCacheInfo() {
    return "Random Cache: eviction policy: Random, maxSlots: "
            + maxSize
            + ", current size: "
            + size;
  }
}
