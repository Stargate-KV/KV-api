package org.stargate.rest.json.Cache;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.stargate.rest.json.KVDataType;

/* QUESTIONS:
 * TODO: 1. Set the maxSlots based on 80% * avaliable memory
 * 2. Implement the LRU cache logic
 * 3. Ask about the lock issues
 * 4. Implement the concurrency map
 * Have to lock hashToIndexMap for the index, otherwise, we have the index i, but when we are processing,
 * the element in this index might be deleted (evicted) already! Or replaced with another value!
 * One way is to store the hash value inside, check first whenever we need to process.
 * But the last update wins this case.
 * How to update the value in cache if the Cassandra got updated by other nodes? **\
 * 1. broadcast to all other cache service before return confirmation to client
 * 2. periadically check cloud (trade-off consistency + avability)
 * 3. each read check cloud (optimized network io)
 */
// @ApplicationScoped
public class FIFOCache {

  private final int maxSlots;
  private final List<FIFOCacheSlot> cacheSlots; // List of pre-allocated slots
  private final List<Lock> locks; // List of pre-allocated Locks
  private final Queue<Integer> freeList; // Queue of available slot indices
  private final Map<Integer, Integer> hashToIndexMap; // Map of hash to index
  private final Queue<Integer> fifoOrder; // To implement FIFO eviction, elements are hash values

  private long hitCount = 0;
  private long totalRead = 0;

  public FIFOCache(int maxSlots) {
    // long freeMemory = Runtime.getRuntime().freeMemory();
    this.maxSlots = maxSlots;
    // (int)(freeMemory * 0.8 / (128 * 1024)); // Allocate 80% of the memory, max size of
    // slots is 128KB
    this.fifoOrder = new ConcurrentLinkedQueue<>();
    this.cacheSlots =
        IntStream.range(0, maxSlots)
            .mapToObj(i -> new FIFOCacheSlot(null, null, null, null, null, false, -1))
            .collect(Collectors.toList()); // Initialize all slots with 'used' set to false
    this.freeList =
        new LinkedList<>(
            IntStream.range(0, maxSlots)
                .boxed()
                .collect(Collectors.toList())); // All indices are initially free
    this.locks =
        IntStream.range(0, maxSlots)
            .mapToObj(i -> new ReentrantLock())
            .collect(Collectors.toList()); // Initialize lock objects
    this.hashToIndexMap = new ConcurrentHashMap<>(); // Initialize hash to index mapping
  }

  public JsonNode get(String key, String keyspace, String table) {
    totalRead++;
    int hash = computeHash(key, keyspace, table);
    int index = hashToIndexMap.getOrDefault(hash, -1);
    if (index != -1) {
      // Might need to improve to read lock afterwards
      FIFOCacheSlot slot = cacheSlots.get(index);
      // Check whether the hashvalue matches, if not, the value already got evicted.
      int slotHashvalue = slot.getHashvalue();
      if (slotHashvalue != hash) {
        return null;
      }
      if (slot.isUsed()) {
        hitCount++;
        return slot.getValue();
      }
      return null;
    }
    return null;
  }

  public boolean delete(String key, String keyspace, String table) {
    int hash = computeHash(key, keyspace, table);
    int index = hashToIndexMap.getOrDefault(hash, -1);

    if (index != -1) {
      Lock lock = locks.get(index);
      lock.lock();
      try {
        FIFOCacheSlot slot = cacheSlots.get(index);
        int slotHashvalue = slot.getHashvalue();
        // Only proceed if hash matches
        if (slotHashvalue != hash) {
          lock.unlock();
          return false;
        }
        slot.setUsed(false);
        putInFreeQueue(index);
        removehashToIndexMap(hash); // Remove hash to index mapping
      } finally {
        lock.unlock();
      }
      return true;
    } else {
      return false;
    }
  }

  private void _create(
      String key, JsonNode value, String keyspace, String table, KVDataType valueType) {
    int hash = computeHash(key, keyspace, table);
    int index;
    Lock lock;

    index = hashToIndexMap.getOrDefault(hash, -1);
    // if (index != -1) { // Go to update in this case
    //   this.put(key, value, keyspace, table, valueType);
    // }

    assert index == -1;
    synchronized (freeList) {
      if (!freeList.isEmpty()) {
        index = freeList.poll(); // Get a free slot index
      } else {
        // Handle cache full - eviction logic
        Integer oldestHash = null;
        // Lazy Cleanup for issue in delete method (The element in queue might already got deleted)
        while (oldestHash == null || !hashToIndexMap.containsKey(oldestHash)) {
          oldestHash = fifoOrder.poll(); // Get the oldest hash, might already be deleted
          if (oldestHash == null) {
            // return new KVResponse(
            //     500, "ERROR: The fifoOrder does not have any value inside for eviction!");
            throw new RuntimeException(
                "ERROR: The fifoOrder does not have any value inside for eviction!");
          }
        }

        index = hashToIndexMap.remove(oldestHash); // Remove the oldest hash and get its index
      }
    }
    hashToIndexMap.put(hash, index);

    lock = locks.get(index);
    lock.lock();

    try {
      FIFOCacheSlot cacheslot = cacheSlots.get(index);
      // Now, you can reuse the evicted slot for the new key-value pair
      cacheslot.setUsed(true);
      cacheslot.setValue(value);
      cacheslot.setKey(key);
      cacheslot.setKeyspace(keyspace);
      cacheslot.setTable(table);
      cacheslot.setValueType(valueType);
      cacheslot.setHashvalue(hash);

      fifoOrder.offer(hash); // Add new hash to fifo order
    } finally {
      lock.unlock();
    }

    // return new KVResponse(
    //     201,
    //     "The key value pair '"
    //         + key
    //         + ":"
    //         + value
    //         + "' has been inserted into cache, evicting an old entry.");
  }

  public void put(
      String key, JsonNode value, String keyspace, String table, KVDataType valueType) {
    int hash = computeHash(key, keyspace, table);
    int index;
    Lock lock;

    index = hashToIndexMap.getOrDefault(hash, -1);
    if (index == -1) { // Go to create in this case
      _create(key, value, keyspace, table, valueType);
      return;
    }

    assert index != -1;

    lock = locks.get(index);
    lock.lock();
    try {
      FIFOCacheSlot slot = cacheSlots.get(index);
      assert slot.isUsed();
      int slotHashvalue = slot.getHashvalue();
      // If hash does not match, this one got evicted, go to create logic
      if (slotHashvalue != hash) {
        lock.unlock();
        _create(key, value, keyspace, table, valueType);
      }
      slot.setValue(value);
      slot.setValueType(valueType);

    } finally {
      lock.unlock();
    }
    // return new KVResponse(
    //     201,
    //     "The key value pair '" + key + ":" + value + "' has been updated in cache successfully.");
  }

  private int computeHash(String key, String keyspace, String table) {
    return Objects.hash(key, keyspace, table);
  }

  private void removehashToIndexMap(int hash) {
    hashToIndexMap.remove(hash);
  }

  private void putInFreeQueue(int index) {
    synchronized (freeList) {
      freeList.offer(index);
    }
  }

  public String getCacheInfo() {
    // Print: eviction policy, maxSlots, hashToIndexMap size
    return  "FIFO Cache: eviction policy: FIFO, maxSlots: "
            + maxSlots
            + ", current size: "
            + hashToIndexMap.size()
            + ", hit ratio: "
            + String.format("%.2f", (double) hitCount / totalRead * 100) + "%";
    // Print cacheSlots List
    // System.out.println("cacheSlots: ");
    // for (KVCacheSlot slot : cacheSlots) {
    //   System.out.println(slot);
    // }

    // // Print freeList Queue
    // System.out.println("freeList: " + freeList);

    // // Print hashToIndexMap Map
    // System.out.println("hashToIndexMap: ");
    // for (Map.Entry<Integer, Integer> entry : hashToIndexMap.entrySet()) {
    //   System.out.println("Hash: " + entry.getKey() + ", Index: " + entry.getValue());
    // }

    // // Print fifoOrder Queue
    // System.out.println("fifoOrder: " + fifoOrder);
  }
}
