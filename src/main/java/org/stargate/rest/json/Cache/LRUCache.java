package org.stargate.rest.json.Cache;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.enterprise.context.ApplicationScoped;

import org.stargate.rest.json.KVDataType;
import org.stargate.rest.json.KVResponse;


public class LRUCache {

    // This is the LRU version of implementation
    private final int maxSlots;
    private final List<FIFOCacheSlot> cacheSlots; // List of pre-allocated slots
    private final List<Lock> locks; // List of pre-allocated Locks
    private final Queue<Integer> freeList; // Queue of available slot indices
    private final Map<Integer, Integer> hashToIndexMap; // Map of hash to index
    private final LinkedHashMap<Integer, Boolean> lruOrder; // To implement FIFO eviction, elements are hash values
    
    private long hitCount = 0;
    private long totalRead = 0;

    public LRUCache(int maxSlots) {
        // long freeMemory = Runtime.getRuntime().freeMemory();
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

    public void _create(String key, JsonNode value, String keyspace, String table, KVDataType valueType) {
        int hash = computeHash(key, keyspace, table);
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

        // printCache();
    }

    public void put(String key, JsonNode value, String keyspace, String table, KVDataType valueType) {
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

        synchronized (lruOrder) {
            lruOrder.put(hash, true); // Update LRU order
        }
        // printCache();
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
    return "LRU Cache: eviction policy: LRU, maxSlots: "
            + maxSlots
            + ", current size: "
            + hashToIndexMap.size()
            + ", hit ratio: "
            + String.format("%.2f", (double) hitCount / totalRead * 100) + "%";
  }
    
}