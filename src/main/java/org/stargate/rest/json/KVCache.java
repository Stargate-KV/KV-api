package org.stargate.rest.json;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.enterprise.context.ApplicationScoped;

import org.stargate.rest.json.Cache.FIFOCache;
import org.stargate.rest.json.Cache.RandomCache;
import org.stargate.rest.json.Cache.LRUCache;

// define enum of EvcitionPolicy, FIFO, RANDOM and NONE
enum EvictionPolicy {
  NONE,
  FIFO,
  RANDOM,
  LRU
}

/**
 * Class KVCache - Manages caching for key-value pairs with support for different eviction policies.
 */
@ApplicationScoped
public class KVCache {
  // Maximum size for the cache
  private int maxSize = 1000;

  // Cache implementations based on different eviction policies
  private FIFOCache fifoCache;
  private RandomCache randomCache;
  private LRUCache lruCache;

  // Current eviction policy, default is FIFO
  private EvictionPolicy evictionPolicy = EvictionPolicy.FIFO;

  // ReadWriteLock to ensure thread-safe operations on the cache
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * Constructor for KVCache.
   * Initializes the cache with default size and eviction policy.
   */
  public KVCache() {
    resetCache(this.maxSize, this.evictionPolicy);
  }

  /**
   * Retrieves a value from the cache based on the key, keyspace, and table.
   *
   * @param key The key whose associated value is to be returned.
   * @param keyspace The keyspace of the key.
   * @param table The table of the key.
   * @return JsonNode The value associated with the specified key, or null if no value is found.
   */
  public JsonNode get(String key, String keyspace, String table) { // get function
    lock.readLock().lock();
    try {
      switch (this.evictionPolicy) {
        case FIFO:
          return fifoCache.get(key, keyspace, table);
        case RANDOM:
          return randomCache.get(key, keyspace, table);
        case LRU:
          return lruCache.get(key, keyspace, table);
        case NONE:
          return null;
        default:
          break;
      }
      return null;

    } finally {
      lock.readLock().unlock();
    }
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
    lock.readLock().lock();
    try {
      switch (this.evictionPolicy) {
        case FIFO:
          return fifoCache.delete(key, keyspace, table);
        case RANDOM:
          return randomCache.delete(key, keyspace, table);
        case LRU:
          return lruCache.delete(key, keyspace, table);
        case NONE:
          return true;  
        default:
          break;
      }
      return false;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Puts a key-value pair into the cache.
   *
   * @param key The key with which the specified value is to be associated.
   * @param value The value to be associated with the specified key.
   * @param keyspace The keyspace of the key.
   * @param table The table of the key.
   * @param valueType The data type of the value.
   */
  public void put(String key, JsonNode value, String keyspace, String table, KVDataType valueType) {
    lock.readLock().lock();
    try {
      switch (this.evictionPolicy) {
        case FIFO:
          fifoCache.put(key, value, keyspace, table, valueType);
          break;
        case RANDOM:
          randomCache.put(key, value, keyspace, table);
          break;
        case LRU:
          lruCache.put(key, value, keyspace, table, valueType);
          break;
        case NONE:
          break;
        default:
          break;
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Resets the cache with a new size and eviction policy.
   * If maxSize is -1, the cache size remains unchanged. If evictionPolicy is null, the policy remains unchanged.
   *
   * @param maxSize The new maximum size of the cache.
   * @param evictionPolicy The new eviction policy for the cache.
   */
  public void resetCache(int maxSize, EvictionPolicy evictionPolicy) {
    if(maxSize == -1) { // clear the cache, remain the same maxSize
      maxSize = this.maxSize;
    }
    if(evictionPolicy == null) { // clear the cache, remain the same policy
      evictionPolicy = this.evictionPolicy;
    }
    lock.writeLock().lock();
    try {
      // clear the cache, reset the maxSize and eviction policy
      this.evictionPolicy = evictionPolicy;
      this.maxSize = maxSize;
      switch (evictionPolicy) {
        case FIFO:
          this.fifoCache = new FIFOCache(maxSize);
          this.randomCache = null;
          this.lruCache = null;
          break;
        case RANDOM:
          this.randomCache = new RandomCache(maxSize);
          this.fifoCache = null;
          this.lruCache = null;
          break;
        case LRU:
          this.lruCache = new LRUCache(maxSize);
          this.fifoCache = null;
          this.randomCache = null;
          break;
        case NONE:
          this.fifoCache = null;
          this.randomCache = null;
          this.lruCache = null;
          break;
        default:
          break;
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Retrieves information about the current cache, such as its size and eviction policy.
   *
   * @return String Information about the cache.
   */
  public String getCacheInfo() {
    lock.readLock().lock();
    try {
      switch (this.evictionPolicy) {
        case FIFO:
          return fifoCache.getCacheInfo();
        case RANDOM:
          return randomCache.getCacheInfo();
        case LRU:
          return lruCache.getCacheInfo();
        case NONE:
          return "No cache";
        default:
          break;
      }
      return null;
    } finally {
      lock.readLock().unlock();
    }
  }
}
