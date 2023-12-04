package org.stargate.rest.json.Cache;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.stargate.rest.json.KVDataType;

/**
 * LRUCache - A cache implementation with a Least Recently Used (LRU) eviction policy.
 * 
 * Design:
 * 1. Uses a combination of keyspace, table, and key as a unique hash value for storing in the cache.
 * 2. Utilizes a list of pre-allocated cache slots for storing key-value pairs.
 * 3. Maintains a map for hash to index mapping and a LinkedHashMap for tracking the LRU order.
 * 4. Implements LRU eviction when the cache is full.
 */

public class LRUCache {

    private final int maxSlots; // Maximum number of slots in the cache
    private final List<FIFOCacheSlot> cacheSlots; // List of pre-allocated cache slots
    private final List<Lock> locks; // List of locks for each cache slot
    private final Queue<Integer> freeList; // Queue of indices of available slots
    private final Map<Integer, Integer> hashToIndexMap; // Map of hash values to slot indices
    private final LinkedHashMap<Integer, Boolean> lruOrder; // LinkedHashMap to maintain LRU order
    
    private long hitCount = 0; // Number of cache hits
    private long totalRead = 0; // Total number of cache read attempts

    /**
     * Constructor for LRUCache.
     *
     * @param maxSlots The maximum number of slots in the cache.
     */
    public LRUCache(int maxSlots) {
        // this.maxSlots = (int)(freeMemory * 0.8 / (128 * 1024)); // Allocate 80% of the memory, max size of slots is 128KB
        this.maxSlots = maxSlots;
        this.lruOrder = new LinkedHashMap<Integer, Boolean>(maxSlots, 1.0f, true) {
            protected boolean removeEldestEntry(Map.Entry<Integer, Boolean> eldest) {
                return false; // Avoid automatic removal, since managing size externally
            }
        };
        this.cacheSlots = IntStream.range(0, maxSlots)
            .mapToObj(i -> new FIFOCacheSlot(null, null, null, null, null, false, -1))
            .collect(Collectors.toList()); // Initialize all slots with 'used' set to false
        this.freeList = new LinkedList<>(IntStream.range(0, maxSlots).boxed().collect(Collectors.toList())); // All indices are initially free
        this.locks = IntStream.range(0, maxSlots).mapToObj(i -> new ReentrantLock()).collect(Collectors.toList()); // Initialize lock objects
        this.hashToIndexMap = new ConcurrentHashMap<>(); // Initialize hash to index mapping
    }

    /**
     * Retrieves a value from the cache.
     *
     * @param key The key whose associated value is to be returned.
     * @param keyspace The keyspace of the key.
     * @param table The table of the key.
     * @return JsonNode The value associated with the specified key, or null if no value is found.
     */
    public JsonNode get(String key, String keyspace, String table) {
        totalRead++;
        int hash = _computeHash(key, keyspace, table);
        int index = hashToIndexMap.getOrDefault(hash, -1);
        if (index != -1) {
            // Might need to improve to read lock afterwards
            FIFOCacheSlot slot = cacheSlots.get(index);
            // Check whether the hashvalue matches, if not, the value already got evicted.
            int slotHashvalue = slot.getHashvalue();
            if (slotHashvalue != hash) {
                return null;
            }
            synchronized (lruOrder) {
                lruOrder.put(hash, true); // Update LRU order
            }
            // printCache();
            if (slot.isUsed()) {
                hitCount++;
                return slot.getValue();
            }
            return null;
        }
        
        return null;
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
        int hash = _computeHash(key, keyspace, table);
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
                _putInFreeQueue(index);
                _removehashToIndexMap(hash); // Remove hash to index mapping
                synchronized (lruOrder) {
                    lruOrder.remove(hash); // Remove the hash from the LRU order
                }
            } finally {
                lock.unlock();
            }
            // printCache();
            return true;
        } else {
            return false;
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
        int hash = _computeHash(key, keyspace, table);
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

        synchronized (lruOrder) {
            lruOrder.put(hash, true); // Update LRU order
        }
        // printCache();
    }

    /**
     * Retrieves information about the current cache, such as its size, eviction policy, and hit ratio.
     *
     * @return String Information about the cache.
     */
    public String getCacheInfo() {
        return "LRU Cache: eviction policy: LRU, maxSlots: "
            + maxSlots
            + ", current size: "
            + hashToIndexMap.size()
            + ", hit ratio: "
            + String.format("%.2f", (double) hitCount / totalRead * 100) + "%";
    }

    // ==================== Helper Functions ====================

    /**
     * Computes a hash value based on the key, keyspace, and table.
     *
     * @param key The key for the cache entry.
     * @param keyspace The keyspace for the cache entry.
     * @param table The table for the cache entry.
     * @return int A hash value.
     */
    private int _computeHash(String key, String keyspace, String table) {
        return Objects.hash(key, keyspace, table);
    }

    /**
     * Removes a hash value from the hash to index map.
     *
     * @param hash The hash value to be removed.
     */
    private void _removehashToIndexMap(int hash) {
        hashToIndexMap.remove(hash);
    }

    /**
     * Adds an index to the free queue.
     *
     * @param index The index to be added to the free queue.
     */
    private void _putInFreeQueue(int index) {
        synchronized (freeList) {
            freeList.offer(index);
        }
    }

    /**
     * Internal method to create a new cache entry.
     *
     * @param key The key with which the specified value is to be associated.
     * @param value The value to be associated with the specified key.
     * @param keyspace The keyspace of the key.
     * @param table The table of the key.
     * @param valueType The data type of the value.
     */
    public void _create(String key, JsonNode value, String keyspace, String table, KVDataType valueType) {
        int hash = _computeHash(key, keyspace, table);
        int index;
        Lock lock;

        index = hashToIndexMap.getOrDefault(hash, -1);

        assert index == -1;
        // if index == -1, then call put
        if(index != -1) {
            put(key, value, keyspace, table, valueType);
            return;
        }

        synchronized (freeList) {
            if (!freeList.isEmpty()) {
                index = freeList.poll(); // Get a free slot index
            }
            else {
                // Handle cache full - eviction logic
                synchronized (lruOrder) {
                    Iterator<Integer> it = lruOrder.keySet().iterator();
                    if (it.hasNext()) {
                        int oldestHash = it.next(); // Get the oldest hash
                        it.remove();
                        index = hashToIndexMap.remove(oldestHash); // Remove the oldest hash and get its index
                    } else {
                        throw new RuntimeException("ERROR: The lruOrder does not have any value inside for eviction!");
                    }
                }
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
            
            synchronized (lruOrder) {
                lruOrder.put(hash, true); // Add new hash to LRU order
            }

        } finally {
            lock.unlock();
        }
    }
    
}