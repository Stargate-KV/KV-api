package org.stargate.rest.json;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.enterprise.context.ApplicationScoped;

import org.stargate.rest.json.Cache.FIFOCache;
import org.stargate.rest.json.Cache.RandomCache;

// define enum of EvcitionPolicy, FIFO, RANDOM and NONE
enum EvictionPolicy {
  NONE,
  FIFO,
  RANDOM
}

@ApplicationScoped
public class KVCache {
  private final int maxSize = 1000;

  // define FIFO Cache and Random Cache inside KVCache
  private FIFOCache fifoCache;
  private RandomCache randomCache;
  // set default eviction policy to FIFO
  private EvictionPolicy evictionPolicy = EvictionPolicy.FIFO;

  // make sure no concurrent resetCache and get/put/delete
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  public KVCache() {
    resetCache(this.maxSize, this.evictionPolicy);
  }

  public JsonNode get(String key, String keyspace, String table) { // get function
    lock.readLock().lock();
    try {
      switch (this.evictionPolicy) {
        case FIFO:
          return fifoCache.get(key, keyspace, table);
        case RANDOM:
          return randomCache.get(key, keyspace, table);
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

  // return whether the key exists in cache
  public boolean delete(String key, String keyspace, String table) {
    lock.readLock().lock();
    try {
      switch (this.evictionPolicy) {
        case FIFO:
          return fifoCache.delete(key, keyspace, table);
        case RANDOM:
          return randomCache.delete(key, keyspace, table);
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
        case NONE:
          break;
        default:
          break;
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  public void resetCache(int maxSize, EvictionPolicy evictionPolicy) {
    lock.writeLock().lock();
    try {
      // clear the cache, reset the maxSize and eviction policy
      this.evictionPolicy = evictionPolicy;
      switch (evictionPolicy) {
        case FIFO:
          this.fifoCache = new FIFOCache(maxSize);
          this.randomCache = null;
          break;
        case RANDOM:
          this.randomCache = new RandomCache(maxSize);
          this.fifoCache = null;
          break;
        case NONE:
          this.fifoCache = null;
          this.randomCache = null;
          break;
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public String getCacheInfo() {
    lock.readLock().lock();
    try {
      switch (this.evictionPolicy) {
        case FIFO:
          return fifoCache.getCacheInfo();
        case RANDOM:
          return randomCache.getCacheInfo();
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
